package decomposition;

import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.FilteredPartition;
import decomposition.pipeline.CpqSynthesizer;
import decomposition.pipeline.DecompositionPipelineCache;
import decomposition.pipeline.DecompositionPipelineCacheProvider;
import decomposition.pipeline.DecompositionPipelineState.GlobalResult;
import decomposition.pipeline.DecompositionPipelineState.PartitionSets;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import decomposition.pipeline.DecompositionPipelineState.SynthesisState;
import decomposition.pipeline.DefaultCpqSynthesizer;
import decomposition.pipeline.DefaultPartitionPruner;
import decomposition.pipeline.DefaultPartitioner;
import decomposition.pipeline.DefaultTupleEnumerator;
import decomposition.pipeline.PartitionPruner;
import decomposition.pipeline.Partitioner;
import decomposition.pipeline.TupleEnumerator;
import decomposition.util.DecompositionPipelineUtils;
import decomposition.util.GraphUtils;
import decomposition.util.Timing;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Orchestrates the CQ to CPQ decomposition pipeline (flat + early-return style). */
public final class DecompositionPipeline implements DecompositionPipelineCacheProvider {
  private final CQExtractor extractor = new CQExtractor();
  private final DecompositionPipelineCache pipelineCache = new DecompositionPipelineCache();
  private final Partitioner partitioner;
  private final PartitionPruner partitionPruner;
  private final CpqSynthesizer cpqSynthesizer;
  private final TupleEnumerator tupleEnumerator;

  public DecompositionPipeline() {
    this(
        new DefaultPartitioner(),
        new DefaultPartitionPruner(),
        new DefaultCpqSynthesizer(),
        new DefaultTupleEnumerator());
  }

  public DecompositionPipeline(
      Partitioner partitioner,
      PartitionPruner partitionPruner,
      CpqSynthesizer cpqSynthesizer,
      TupleEnumerator tupleEnumerator) {
    this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
    this.partitionPruner = Objects.requireNonNull(partitionPruner, "partitionPruner");
    this.cpqSynthesizer = Objects.requireNonNull(cpqSynthesizer, "cpqSynthesizer");
    this.tupleEnumerator = Objects.requireNonNull(tupleEnumerator, "tupleEnumerator");
  }

  public DecompositionResult execute(
      CQ cq, Set<String> explicitFreeVariables, DecompositionOptions options) {
    pipelineCache.reset();
    final Timing timing = Timing.start();
    final DecompositionOptions opts = DecompositionPipelineUtils.resolveOptions(options);

    PipelineContext ctx = extractContext(cq, explicitFreeVariables);
    List<Partition> partitions = partitioner.enumerate(ctx, opts);
    PartitionSets parts = partitionPruner.prune(partitions, ctx, opts);

    if (cpqSynthesizer.overBudget(opts, timing)) {
      return buildPartitioningExit(ctx, parts, timing);
    }

    SynthesisState state = cpqSynthesizer.createState(ctx, opts);

    DecompositionResult validationExit = processFilteredPartitions(ctx, parts, state, opts, timing);
    if (validationExit != null) {
      pipelineCache.update(state.cacheStats);
      return validationExit;
    }

    GlobalResult globalResult = cpqSynthesizer.computeGlobalResult(ctx, opts, state);

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

  private DecompositionResult processFilteredPartitions(
      PipelineContext ctx,
      PartitionSets parts,
      SynthesisState state,
      DecompositionOptions opts,
      Timing timing) {

    List<FilteredPartition> filteredWithJoins = parts.filteredWithJoins();
    List<String> diagnostics = parts.diagnostics();
    int idx = 0;

    for (FilteredPartition fp : filteredWithJoins) {
      if (cpqSynthesizer.overBudget(opts, timing)) {
        return buildBudgetExceededDuringValidation(ctx, parts, state, timing);
      }

      idx++;
      cpqSynthesizer.processPartition(fp, ctx, opts, state, tupleEnumerator, diagnostics, idx);
    }

    return null;
  }

  private DecompositionResult buildFinalResult(
      PipelineContext ctx,
      PartitionSets parts,
      SynthesisState state,
      DecompositionOptions opts,
      Timing timing,
      GlobalResult globalResult) {

    long elapsed = timing.elapsedMillis();
    String termination = cpqSynthesizer.overBudget(opts, elapsed) ? "time_budget_exceeded" : null;
    return DecompositionPipelineUtils.buildResult(
        ctx.extraction(),
        ctx.vertices(),
        parts.partitions(),
        parts.filtered(),
        state.cpqPartitions,
        cpqSynthesizer.dedupeCatalogue(state.recognizedCatalogue),
        globalResult.finalExpression(),
        globalResult.globalCatalogue(),
        state.partitionEvaluations,
        parts.diagnostics(),
        elapsed,
        termination);
  }

  private DecompositionResult buildPartitioningExit(
      PipelineContext ctx, PartitionSets parts, Timing timing) {
    return DecompositionPipelineUtils.buildResult(
        ctx.extraction(),
        ctx.vertices(),
        parts.partitions(),
        parts.filtered(),
        List.of(),
        List.of(),
        null,
        List.of(),
        List.of(),
        parts.diagnostics(),
        timing.elapsedMillis(),
        "time_budget_exceeded_after_partitioning");
  }

  private DecompositionResult buildBudgetExceededDuringValidation(
      PipelineContext ctx, PartitionSets parts, SynthesisState state, Timing timing) {
    return DecompositionPipelineUtils.buildResult(
        ctx.extraction(),
        ctx.vertices(),
        parts.partitions(),
        parts.filtered(),
        state.cpqPartitions,
        cpqSynthesizer.dedupeCatalogue(state.recognizedCatalogue),
        null,
        List.of(),
        state.partitionEvaluations,
        parts.diagnostics(),
        timing.elapsedMillis(),
        "time_budget_exceeded_during_validation");
  }

  @Override
  public DecompositionPipelineCache pipelineCache() {
    return pipelineCache;
  }
}
