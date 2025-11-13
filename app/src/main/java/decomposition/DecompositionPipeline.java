package decomposition;

import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentCacheKey;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.model.CacheStats;
import decomposition.cpq.model.ComponentKey;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilter;
import decomposition.partitions.PartitionFilter.FilterResult;
import decomposition.partitions.PartitionFilter.FilteredPartition;
import decomposition.partitions.PartitionGenerator;
import decomposition.util.GraphUtils;
import decomposition.util.JoinNodeUtils;
import decomposition.util.Timing;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Orchestrates the CQ to CPQ decomposition pipeline (flat + early-return style). */
public final class DecompositionPipeline {
  private static final int MAX_JOIN_NODES = 2;

  private final CQExtractor extractor = new CQExtractor();
  private CacheStats lastCacheSnapshot;

  public DecompositionResult execute(
      CQ cq, Set<String> explicitFreeVariables, DecompositionOptions options) {
    lastCacheSnapshot = null;
    final DecompositionOptions opts = (options != null) ? options : DecompositionOptions.defaults();
    final Timing timing = Timing.start();

    // Extract graph + context
    final ExtractionResult extraction = extractor.extract(cq, explicitFreeVariables);
    final List<Edge> edges = extraction.edges();
    final Map<String, String> varToNodeMap = extraction.variableNodeMap();
    final int edgeCount = edges.size();

    final BitSet fullBits = new BitSet(edgeCount);
    fullBits.set(0, edgeCount);
    final Set<String> vertices = GraphUtils.vertices(fullBits, edges);

    // Enumerate + filter partitions
    final PartitionGenerator generator = new PartitionGenerator(opts.maxPartitions());
    final List<Component> components = generator.enumerateConnectedComponents(edges);
    final List<Partition> partitions =
        generator.enumeratePartitions(
            edges, components, extraction.freeVariables(), MAX_JOIN_NODES);

    final FilterResult filterResult =
        new PartitionFilter(MAX_JOIN_NODES).filter(partitions, extraction.freeVariables());
    final List<FilteredPartition> filteredWithJoins = filterResult.partitions();
    final List<Partition> filtered =
        filteredWithJoins.stream().map(FilteredPartition::partition).toList();

    final List<String> diagnostics = new ArrayList<>(filterResult.diagnostics());
    final Map<ComponentKey, CPQExpression> recognizedCatalogueMap = new LinkedHashMap<>();

    // Early exit on time
    if (isOverBudget(opts, timing)) {
      return buildResult(
          extraction,
          vertices,
          partitions,
          filtered,
          List.of(),
          List.of(),
          null,
          List.of(),
          List.of(),
          diagnostics,
          timing.elapsedMillis(),
          "time_budget_exceeded_after_partitioning");
    }

    final PartitionExpressionAssembler synthesizer = new PartitionExpressionAssembler(edges);
    final CacheStats cacheStats = new CacheStats();
    final PartitionDiagnostics partitionDiagnosticsHelper = new PartitionDiagnostics();
    final Map<ComponentCacheKey, CachedComponentExpressions> componentCache =
        new ConcurrentHashMap<>();

    // Working accumulators
    CPQExpression finalExpression = null;
    final List<Partition> cpqPartitions = new ArrayList<>();
    final List<PartitionEvaluation> partitionEvaluations = new ArrayList<>();
    List<CPQExpression> globalCatalogue = List.of();

    // Precompute tuple limit
    final boolean wantTuples = opts.mode().enumerateTuples();
    final boolean singleTuplePerPartition = opts.singleTuplePerPartition();
    final int tupleLimit =
        singleTuplePerPartition ? 1 : (opts.enumerationLimit() <= 0 ? 1 : opts.enumerationLimit());

    int idx = 0;
    for (FilteredPartition fp : filteredWithJoins) {
      if (isOverBudget(opts, timing)) {
        lastCacheSnapshot = cacheStats.snapshot();
        return buildResult(
            extraction,
            vertices,
            partitions,
            filtered,
            cpqPartitions,
            new ArrayList<>(recognizedCatalogueMap.values()),
            finalExpression,
            globalCatalogue,
            partitionEvaluations,
            diagnostics,
            timing.elapsedMillis(),
            "time_budget_exceeded_during_validation");
      }

      idx++;
      final Partition partition = fp.partition();
      final Set<String> joinNodes = fp.joinNodes();

      final List<List<CPQExpression>> componentExpressions =
          synthesizer.synthesize(
              partition,
              joinNodes,
              extraction.freeVariables(),
              varToNodeMap,
              componentCache,
              cacheStats,
              partitionDiagnosticsHelper);

      if (componentExpressions == null) {
        addComponentDiagnostics(diagnostics, partitionDiagnosticsHelper);
        continue;
      }

      cpqPartitions.add(partition);

      // Recognize all finals into catalogue
      for (List<CPQExpression> compExpressions : componentExpressions) {
        for (CPQExpression k : compExpressions) {
          recognizedCatalogueMap.putIfAbsent(k.toKey(edgeCount), k);
        }
      }

      final List<List<CPQExpression>> tuples =
          wantTuples
              ? PartitionExpressionAssembler.enumerateTuples(componentExpressions, tupleLimit)
              : List.of();

      partitionEvaluations.add(
          new PartitionEvaluation(
              partition, idx, componentExpressions.stream().map(List::size).toList(), tuples));
    }

    // Global candidate at the end if still within budget
    if (!isOverBudget(opts, timing)) {
      final Set<String> globalJoinNodes =
          JoinNodeUtils.computeJoinNodes(
              List.of(new Component(fullBits, vertices)), extraction.freeVariables());
      final List<CPQExpression> globalCandidates =
          synthesizer.synthesizeGlobal(fullBits, globalJoinNodes, varToNodeMap);
      globalCatalogue = globalCandidates;
      finalExpression = selectPreferredFinalComponent(globalCandidates);
    }

    // Final time check
    final long elapsed = timing.elapsedMillis();
    final String termination = isOverBudget(opts, elapsed) ? "time_budget_exceeded" : null;

    lastCacheSnapshot = cacheStats.snapshot();
    return buildResult(
        extraction,
        vertices,
        partitions,
        filtered,
        cpqPartitions,
        new ArrayList<>(recognizedCatalogueMap.values()),
        finalExpression,
        globalCatalogue,
        partitionEvaluations,
        diagnostics,
        elapsed,
        termination);
  }

  // ——— helpers (small, focused) ————————————————————————————————————————————————

  private void addComponentDiagnostics(
      List<String> diagnostics, PartitionDiagnostics partitionDiagnostics) {
    List<String> cached = partitionDiagnostics.lastComponentDiagnostics();
    if (cached != null && !cached.isEmpty()) {
      diagnostics.addAll(cached);
    } else {
      diagnostics.add("Partition rejected but component diagnostics were unavailable.");
    }
  }

  private CPQExpression selectPreferredFinalComponent(List<CPQExpression> rules) {
    return (rules == null || rules.isEmpty()) ? null : rules.get(0);
  }

  private boolean isOverBudget(DecompositionOptions opts, Timing t) {
    return opts.timeBudgetMs() > 0 && t.elapsedMillis() > opts.timeBudgetMs();
  }

  private boolean isOverBudget(DecompositionOptions opts, long elapsedMillis) {
    return opts.timeBudgetMs() > 0 && elapsedMillis > opts.timeBudgetMs();
  }

  private DecompositionResult buildResult(
      ExtractionResult extraction,
      Set<String> vertices,
      List<Partition> partitions,
      List<Partition> filteredPartitions,
      List<Partition> cpqPartitions,
      List<CPQExpression> recognizedCatalogue,
      CPQExpression finalExpression,
      List<CPQExpression> globalCatalogue,
      List<PartitionEvaluation> partitionEvaluations,
      List<String> diagnostics,
      long elapsedMillis,
      String terminationReason) {

    return new DecompositionResult(
        extraction.edges(),
        extraction.freeVariables(),
        vertices.size(),
        partitions.size(),
        filteredPartitions.size(),
        partitions,
        filteredPartitions,
        (cpqPartitions != null) ? cpqPartitions : List.of(),
        (recognizedCatalogue != null) ? recognizedCatalogue : List.of(),
        finalExpression,
        (globalCatalogue != null) ? globalCatalogue : List.of(),
        (partitionEvaluations != null) ? partitionEvaluations : List.of(),
        diagnostics,
        elapsedMillis,
        terminationReason);
  }

  public CacheStats lastCacheSnapshot() {
    return lastCacheSnapshot;
  }
}
