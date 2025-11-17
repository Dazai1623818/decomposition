package decomposition.pipeline;

import decomposition.DecompositionOptions;
import decomposition.PartitionEvaluation;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.PartitionExpressionAssembler.ComponentCacheKey;
import decomposition.cpq.model.CacheStats;
import decomposition.diagnostics.PartitionDiagnostic;
import decomposition.model.Component;
import decomposition.model.Partition;
import decomposition.partitions.FilteredPartition;
import decomposition.pipeline.DecompositionPipelineState.GlobalResult;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import decomposition.pipeline.DecompositionPipelineState.SynthesisState;
import decomposition.util.JoinAnalysisBuilder;
import decomposition.util.Timing;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Default CPQ synthesizer used by the pipeline orchestrator. */
public final class DefaultCpqSynthesizer implements CpqSynthesizer {
  @Override
  public SynthesisState createState(PipelineContext context, DecompositionOptions options) {
    PartitionExpressionAssembler synthesizer = new PartitionExpressionAssembler(context.edges());
    CacheStats cacheStats = new CacheStats();
    PartitionDiagnostics partitionDiagnostics = new PartitionDiagnostics();
    Map<ComponentCacheKey, CachedComponentExpressions> componentCache = new ConcurrentHashMap<>();
    boolean wantTuples = options.mode().enumerateTuples();
    boolean singleTuplePerPartition = options.singleTuplePerPartition();
    int tupleLimit =
        singleTuplePerPartition
            ? 1
            : (options.enumerationLimit() <= 0 ? 1 : options.enumerationLimit());
    return new SynthesisState(
        synthesizer, cacheStats, partitionDiagnostics, componentCache, wantTuples, tupleLimit);
  }

  @Override
  public void processPartition(
      FilteredPartition partition,
      PipelineContext context,
      DecompositionOptions options,
      SynthesisState state,
      TupleEnumerator tupleEnumerator,
      List<PartitionDiagnostic> diagnostics,
      int partitionIndex) {

    List<List<CPQExpression>> componentExpressions =
        state.synthesizer.synthesize(
            partition,
            context.extraction().freeVariables(),
            context.varToNodeMap(),
            state.componentCache,
            state.cacheStats,
            state.partitionDiagnostics,
            partitionIndex);

    if (componentExpressions == null) {
      addComponentDiagnostics(diagnostics, state.partitionDiagnostics, partitionIndex);
      return;
    }

    Partition matchedPartition = partition.partition();
    state.cpqPartitions.add(matchedPartition);
    for (List<CPQExpression> compExpressions : componentExpressions) {
      state.recognizedCatalogue.addAll(compExpressions);
    }

    List<List<CPQExpression>> tuples = tupleEnumerator.enumerate(componentExpressions, state);
    state.partitionEvaluations.add(
        new PartitionEvaluation(
            matchedPartition,
            partitionIndex,
            componentExpressions.stream().map(List::size).toList(),
            tuples));
  }

  @Override
  public GlobalResult computeGlobalResult(
      PipelineContext context, DecompositionOptions options, SynthesisState state) {
    Set<String> globalJoinNodes =
        JoinAnalysisBuilder.analyzePartition(
                new Partition(List.of(new Component(context.fullBits(), context.vertices()))),
                context.extraction().freeVariables())
            .globalJoinNodes();
    List<CPQExpression> globalCandidates =
        state.synthesizer.synthesizeGlobal(
            context.fullBits(), globalJoinNodes, context.varToNodeMap());
    return new GlobalResult(globalCandidates, selectPreferredFinalComponent(globalCandidates));
  }

  @Override
  public boolean overBudget(DecompositionOptions options, Timing timing) {
    return options.timeBudgetMs() > 0 && timing.elapsedMillis() > options.timeBudgetMs();
  }

  @Override
  public boolean overBudget(DecompositionOptions options, long elapsedMillis) {
    return options.timeBudgetMs() > 0 && elapsedMillis > options.timeBudgetMs();
  }

  @Override
  public List<CPQExpression> dedupeCatalogue(List<CPQExpression> expressions) {
    return ComponentExpressionBuilder.dedupeExpressions(expressions);
  }

  private void addComponentDiagnostics(
      List<PartitionDiagnostic> diagnostics,
      PartitionDiagnostics partitionDiagnostics,
      int partitionIndex) {
    List<PartitionDiagnostic> cached = partitionDiagnostics.lastComponentDiagnostics();
    if (cached != null && !cached.isEmpty()) {
      diagnostics.addAll(cached);
    } else {
      diagnostics.add(PartitionDiagnostic.diagnosticsUnavailable(partitionIndex));
    }
  }

  private CPQExpression selectPreferredFinalComponent(List<CPQExpression> rules) {
    return (rules == null || rules.isEmpty()) ? null : rules.get(0);
  }
}
