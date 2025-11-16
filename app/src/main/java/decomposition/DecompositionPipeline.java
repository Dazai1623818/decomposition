package decomposition;

import static decomposition.util.DecompositionPipelineUtils.addComponentDiagnostics;
import static decomposition.util.DecompositionPipelineUtils.buildResult;
import static decomposition.util.DecompositionPipelineUtils.earlyExitAfterPartitioning;
import static decomposition.util.DecompositionPipelineUtils.enumerateTuples;
import static decomposition.util.DecompositionPipelineUtils.overBudget;
import static decomposition.util.DecompositionPipelineUtils.resolveOptions;
import static decomposition.util.DecompositionPipelineUtils.selectPreferredFinalComponent;

import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.PartitionExpressionAssembler.ComponentCacheKey;
import decomposition.cpq.model.CacheStats;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.FilteredPartition;
import decomposition.partitions.PartitionFilter;
import decomposition.partitions.PartitionFilter.FilterResult;
import decomposition.partitions.PartitionGenerator;
import decomposition.pipeline.DecompositionPipelineCache;
import decomposition.pipeline.DecompositionPipelineCacheProvider;
import decomposition.pipeline.DecompositionPipelineState.GlobalResult;
import decomposition.pipeline.DecompositionPipelineState.PartitionSets;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import decomposition.pipeline.DecompositionPipelineState.SynthesisState;
import decomposition.util.GraphUtils;
import decomposition.util.JoinAnalysisBuilder;
import decomposition.util.Timing;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Orchestrates the CQ to CPQ decomposition pipeline (flat + early-return style). */
public final class DecompositionPipeline implements DecompositionPipelineCacheProvider {
  private static final int MAX_JOIN_NODES = 2;

  private final CQExtractor extractor = new CQExtractor();
  private final DecompositionPipelineCache pipelineCache = new DecompositionPipelineCache();

  public DecompositionResult execute(
      CQ cq, Set<String> explicitFreeVariables, DecompositionOptions options) {
    pipelineCache.reset();
    final Timing timing = Timing.start();
    final DecompositionOptions opts = resolveOptions(options);

    PipelineContext ctx = extractContext(cq, explicitFreeVariables);
    PartitionSets parts = enumerateAndFilterPartitions(ctx, opts);

    if (overBudget(opts, timing)) {
      return earlyExitAfterPartitioning(ctx, parts, timing);
    }

    SynthesisState state = initSynthesisState(ctx, opts);

    DecompositionResult validationExit = processFilteredPartitions(ctx, parts, state, opts, timing);
    if (validationExit != null) {
      pipelineCache.update(state.cacheStats);
      return validationExit;
    }

    GlobalResult globalResult = computeGlobalCandidates(ctx, state, opts, timing);

    pipelineCache.update(state.cacheStats);
    return buildFinalResult(ctx, parts, state, opts, timing, globalResult);
  }

  private PipelineContext extractContext(CQ cq, Set<String> explicitFreeVariables) {
    ExtractionResult extraction = extractor.extract(cq, explicitFreeVariables);
    List<Edge> edges = extraction.edges();
    int edgeCount = edges.size();
    BitSet fullBits = new BitSet(edgeCount);
    fullBits.set(0, edgeCount);
    Set<String> vertices = GraphUtils.vertices(fullBits, edges);
    return new PipelineContext(
        extraction, edges, extraction.variableNodeMap(), vertices, fullBits, edgeCount);
  }

  private PartitionSets enumerateAndFilterPartitions(
      PipelineContext ctx, DecompositionOptions opts) {
    PartitionGenerator generator = new PartitionGenerator(opts.maxPartitions());
    List<Component> components = generator.enumerateConnectedComponents(ctx.edges());
    List<Partition> partitions =
        generator.enumeratePartitions(
            ctx.edges(), components, ctx.extraction().freeVariables(), MAX_JOIN_NODES);

    FilterResult filterResult =
        new PartitionFilter(MAX_JOIN_NODES).filter(partitions, ctx.extraction().freeVariables());
    List<FilteredPartition> filteredWithJoins = filterResult.partitions();
    List<Partition> filtered =
        filteredWithJoins.stream().map(FilteredPartition::partition).toList();

    List<String> diagnostics = new ArrayList<>(filterResult.diagnostics());
    return new PartitionSets(partitions, filteredWithJoins, filtered, diagnostics);
  }

  private SynthesisState initSynthesisState(PipelineContext ctx, DecompositionOptions opts) {
    PartitionExpressionAssembler synthesizer = new PartitionExpressionAssembler(ctx.edges());
    CacheStats cacheStats = new CacheStats();
    PartitionDiagnostics partitionDiagnostics = new PartitionDiagnostics();
    Map<ComponentCacheKey, CachedComponentExpressions> componentCache = new ConcurrentHashMap<>();
    boolean wantTuples = opts.mode().enumerateTuples();
    boolean singleTuplePerPartition = opts.singleTuplePerPartition();
    int tupleLimit =
        singleTuplePerPartition ? 1 : (opts.enumerationLimit() <= 0 ? 1 : opts.enumerationLimit());
    return new SynthesisState(
        synthesizer, cacheStats, partitionDiagnostics, componentCache, wantTuples, tupleLimit);
  }

  private DecompositionResult processFilteredPartitions(
      PipelineContext ctx,
      PartitionSets parts,
      SynthesisState state,
      DecompositionOptions opts,
      Timing timing) {

    List<FilteredPartition> filteredWithJoins = parts.filteredWithJoins();
    List<Partition> filtered = parts.filtered();
    List<String> diagnostics = parts.diagnostics();
    int idx = 0;

    for (FilteredPartition fp : filteredWithJoins) {
      if (overBudget(opts, timing)) {
        return buildResult(
            ctx.extraction(),
            ctx.vertices(),
            parts.partitions(),
            filtered,
            state.cpqPartitions,
            dedupeCatalogue(state.recognizedCatalogue),
            null,
            List.of(),
            state.partitionEvaluations,
            diagnostics,
            timing.elapsedMillis(),
            "time_budget_exceeded_during_validation");
      }

      idx++;
      Partition partition = fp.partition();
      List<List<CPQExpression>> componentExpressions =
          state.synthesizer.synthesize(
              fp,
              ctx.extraction().freeVariables(),
              ctx.varToNodeMap(),
              state.componentCache,
              state.cacheStats,
              state.partitionDiagnostics);

      if (componentExpressions == null) {
        addComponentDiagnostics(diagnostics, state.partitionDiagnostics);
        continue;
      }

      state.cpqPartitions.add(partition);

      for (List<CPQExpression> compExpressions : componentExpressions) {
        state.recognizedCatalogue.addAll(compExpressions);
      }

      List<List<CPQExpression>> tuples =
          state.wantTuples ? enumerateTuples(componentExpressions, state.tupleLimit) : List.of();

      state.partitionEvaluations.add(
          new PartitionEvaluation(
              partition, idx, componentExpressions.stream().map(List::size).toList(), tuples));
    }

    return null;
  }

  private GlobalResult computeGlobalCandidates(
      PipelineContext ctx, SynthesisState state, DecompositionOptions opts, Timing timing) {
    if (overBudget(opts, timing)) {
      return new GlobalResult(List.of(), null);
    }

    Set<String> globalJoinNodes =
        JoinAnalysisBuilder.analyzePartition(
                new Partition(List.of(new Component(ctx.fullBits(), ctx.vertices()))),
                ctx.extraction().freeVariables())
            .globalJoinNodes();
    List<CPQExpression> globalCandidates =
        state.synthesizer.synthesizeGlobal(ctx.fullBits(), globalJoinNodes, ctx.varToNodeMap());
    return new GlobalResult(globalCandidates, selectPreferredFinalComponent(globalCandidates));
  }

  private DecompositionResult buildFinalResult(
      PipelineContext ctx,
      PartitionSets parts,
      SynthesisState state,
      DecompositionOptions opts,
      Timing timing,
      GlobalResult globalResult) {

    long elapsed = timing.elapsedMillis();
    String termination = overBudget(opts, elapsed) ? "time_budget_exceeded" : null;
    return buildResult(
        ctx.extraction(),
        ctx.vertices(),
        parts.partitions(),
        parts.filtered(),
        state.cpqPartitions,
        dedupeCatalogue(state.recognizedCatalogue),
        globalResult.finalExpression(),
        globalResult.globalCatalogue(),
        state.partitionEvaluations,
        parts.diagnostics(),
        elapsed,
        termination);
  }

  private static List<CPQExpression> dedupeCatalogue(List<CPQExpression> catalogue) {
    return ComponentExpressionBuilder.dedupeExpressions(catalogue);
  }

  @Override
  public DecompositionPipelineCache pipelineCache() {
    return pipelineCache;
  }
}
