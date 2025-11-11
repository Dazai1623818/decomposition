package decomposition;

import decomposition.cpq.CPQEnumerator;
import decomposition.cpq.KnownComponent;
import decomposition.cpq.model.CacheStats;
import decomposition.cpq.model.ComponentKey;
import decomposition.cpq.model.PartitionAnalysis;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilter;
import decomposition.partitions.PartitionFilter.FilterResult;
import decomposition.partitions.PartitionFilter.FilteredPartition;
import decomposition.partitions.PartitionGenerator;
import decomposition.util.BitsetUtils;
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
    final Map<ComponentKey, KnownComponent> recognizedCatalogueMap = new LinkedHashMap<>();

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

    // Enumerator
    final CPQEnumerator enumerator = new CPQEnumerator(edges);

    // Working accumulators
    KnownComponent finalComponent = null;
    final List<Partition> cpqPartitions = new ArrayList<>();
    final List<PartitionEvaluation> partitionEvaluations = new ArrayList<>();
    List<KnownComponent> globalCatalogue = List.of();

    // Precompute tuple limit
    final boolean wantTuples = opts.mode().enumerateTuples();
    final boolean singleTuplePerPartition = opts.singleTuplePerPartition();
    final int tupleLimit =
        singleTuplePerPartition ? 1 : (opts.enumerationLimit() <= 0 ? 1 : opts.enumerationLimit());

    int idx = 0;
    for (FilteredPartition fp : filteredWithJoins) {
      if (isOverBudget(opts, timing)) {
        lastCacheSnapshot = enumerator.cacheStats().snapshot();
        return buildResult(
            extraction,
            vertices,
            partitions,
            filtered,
            cpqPartitions,
            new ArrayList<>(recognizedCatalogueMap.values()),
            finalComponent,
            globalCatalogue,
            partitionEvaluations,
            diagnostics,
            timing.elapsedMillis(),
            "time_budget_exceeded_during_validation");
      }

      idx++;
      final Partition partition = fp.partition();
      final Set<String> joinNodes = fp.joinNodes();

      final PartitionAnalysis analysis =
          enumerator.analyzePartition(partition, joinNodes, extraction.freeVariables(), varToNodeMap);

      if (analysis == null) {
        // Collect simple per-component reasons
        addComponentDiagnostics(
            diagnostics, partition, edges, extraction.freeVariables(), varToNodeMap, enumerator);
        continue;
      }

      cpqPartitions.add(partition);

      // Recognize all finals into catalogue
      for (var compRules : analysis.components()) {
        for (KnownComponent k : compRules.finalRules()) {
          recognizedCatalogueMap.putIfAbsent(k.toKey(edgeCount), k);
        }
      }

      final List<List<KnownComponent>> tuples =
          wantTuples ? enumerator.enumerateTuples(analysis, tupleLimit) : List.of();

      partitionEvaluations.add(
          new PartitionEvaluation(partition, idx, analysis.ruleCounts(), tuples));
    }

    // Global candidate at the end if still within budget
    if (!isOverBudget(opts, timing)) {
      final Set<String> globalJoinNodes =
          JoinNodeUtils.computeJoinNodes(
              List.of(new Component(fullBits, vertices)), extraction.freeVariables());
      final List<KnownComponent> globalCandidates =
          enumerator.constructionRules(fullBits, globalJoinNodes, varToNodeMap);
      globalCatalogue = globalCandidates;
      finalComponent = selectPreferredFinalComponent(globalCandidates);
    }

    // Final time check
    final long elapsed = timing.elapsedMillis();
    final String termination = isOverBudget(opts, elapsed) ? "time_budget_exceeded" : null;

    lastCacheSnapshot = enumerator.cacheStats().snapshot();
    return buildResult(
        extraction,
        vertices,
        partitions,
        filtered,
        cpqPartitions,
        new ArrayList<>(recognizedCatalogueMap.values()),
        finalComponent,
        globalCatalogue,
        partitionEvaluations,
        diagnostics,
        elapsed,
        termination);
  }

  // ——— helpers (small, focused) ————————————————————————————————————————————————

  private void addComponentDiagnostics(
      List<String> diagnostics,
      Partition partition,
      List<Edge> allEdges,
      Set<String> freeVars,
      Map<String, String> varToNodeMap,
      CPQEnumerator enumerator) {
    List<String> cached = enumerator.lastComponentDiagnostics();
    if (cached != null && !cached.isEmpty()) {
      diagnostics.addAll(cached);
      return;
    }
    final int edgeCount = allEdges.size();
    int i = 0;
    for (Component c : partition.components()) {
      i++;
      final var ruleSet =
          enumerator.componentRules(
              c, Set.of(), freeVars, partition.components().size(), varToNodeMap);
      final String sig = BitsetUtils.signature(c.edgeBits(), edgeCount);
      if (ruleSet.rawRules().isEmpty()) {
        diagnostics.add(
            "Partition component#" + i + " rejected: no CPQ construction rules for bits " + sig);
      } else if (ruleSet.joinFilteredRules().isEmpty()) {
        diagnostics.add(
            "Partition component#" + i + " rejected: endpoints not on join nodes for bits " + sig);
      }
    }
  }

  private KnownComponent selectPreferredFinalComponent(List<KnownComponent> rules) {
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
      List<KnownComponent> recognizedCatalogue,
      KnownComponent finalComponent,
      List<KnownComponent> globalCatalogue,
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
        finalComponent,
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
