package decomposition;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.ComponentKey;
import decomposition.cpq.KnownComponent;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilter;
import decomposition.partitions.PartitionFilter.FilterResult;
import decomposition.partitions.PartitionFilter.FilteredPartition;
import decomposition.partitions.PartitionGenerator;
import decomposition.partitions.PartitionValidator;
import decomposition.partitions.PartitionValidator.ComponentConstructionRules;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/** Orchestrates the CQ to CPQ decomposition pipeline. */
public final class DecompositionPipeline {
  private static final int MAX_JOIN_NODES = 2;
  private final CQExtractor extractor = new CQExtractor();
  private final Supplier<PartitionValidator> partitionValidatorSupplier;

  public DecompositionPipeline() {
    this(PartitionValidator::new);
  }

  public DecompositionPipeline(Supplier<PartitionValidator> partitionValidatorSupplier) {
    this.partitionValidatorSupplier =
        Objects.requireNonNull(partitionValidatorSupplier, "partitionValidatorSupplier");
  }

  public DecompositionResult execute(
      CQ cq, Set<String> explicitFreeVariables, DecompositionOptions options) {
    DecompositionOptions effectiveOptions =
        options != null ? options : DecompositionOptions.defaults();
    Timing timing = Timing.start();

    ExtractionResult extraction = extractor.extract(cq, explicitFreeVariables);
    List<Edge> edges = extraction.edges();
    int edgeCount = edges.size();

    BitSet fullBits = new BitSet(edgeCount);
    fullBits.set(0, edgeCount);
    Set<String> vertices = GraphUtils.vertices(fullBits, edges);

    PartitionGenerator generator = new PartitionGenerator(effectiveOptions.maxPartitions());
    List<Component> components = generator.enumerateConnectedComponents(edges);
    List<Partition> partitions = generator.enumeratePartitions(edges, components);

    FilterResult filterResult =
        new PartitionFilter(MAX_JOIN_NODES).filter(partitions, extraction.freeVariables());
    List<FilteredPartition> filteredPartitionsWithJoins = filterResult.partitions();
    List<Partition> filteredPartitions =
        filteredPartitionsWithJoins.stream().map(FilteredPartition::partition).toList();

    List<String> diagnostics = new ArrayList<>(filterResult.diagnostics());
    Map<ComponentKey, KnownComponent> recognizedCatalogueMap = new LinkedHashMap<>();
    String terminationReason = null;

    if (timeExceeded(effectiveOptions, timing)) {
      terminationReason = "time_budget_exceeded_after_partitioning";
      return buildResult(
          extraction,
          vertices,
          partitions,
          filteredPartitions,
          List.of(),
          List.of(),
          null,
          List.of(),
          List.of(),
          diagnostics,
          timing.elapsedMillis(),
          terminationReason);
    }

    ComponentCPQBuilder builder = new ComponentCPQBuilder(edges);
    PartitionValidator validator = partitionValidatorSupplier.get();
    if (validator == null) {
      throw new IllegalStateException("partitionValidatorSupplier returned null");
    }

    KnownComponent finalComponent = null;
    List<Partition> cpqPartitions = new ArrayList<>();
    List<PartitionEvaluation> partitionEvaluations = new ArrayList<>();
    List<KnownComponent> globalCatalogue = List.of();

    int partitionIndex = 0;
    for (FilteredPartition filteredPartition : filteredPartitionsWithJoins) {
      Partition partition = filteredPartition.partition();
      Set<String> joinNodes = filteredPartition.joinNodes();
      partitionIndex++;

      List<ComponentConstructionRules> componentRules =
          validator.componentConstructionRules(
              partition, joinNodes, builder, extraction.freeVariables(), edges);

      if (!componentRules.stream().allMatch(ComponentConstructionRules::hasRules)) {
        int componentIndex = 0;
        for (ComponentConstructionRules componentRuleSet : componentRules) {
          componentIndex++;
          BitSet componentBits = componentRuleSet.component().edgeBits();
          String signature = BitsetUtils.signature(componentBits, edgeCount);

          if (componentRuleSet.rawRules().isEmpty()) {
            diagnostics.add(
                "Partition#"
                    + partitionIndex
                    + " component#"
                    + componentIndex
                    + " rejected: no CPQ construction rules for bits "
                    + signature);
          } else if (componentRuleSet.joinFilteredRules().isEmpty()) {
            diagnostics.add(
                "Partition#"
                    + partitionIndex
                    + " component#"
                    + componentIndex
                    + " rejected: endpoints not on join nodes for bits "
                    + signature);
          }
        }
        continue;
      }

      cpqPartitions.add(partition);

      List<Integer> ruleCounts =
          componentRules.stream().map(ComponentConstructionRules::ruleCount).toList();
      List<List<KnownComponent>> tuples =
          effectiveOptions.mode().enumerateTuples()
              ? validator.enumerateDecompositions(
                  componentRules,
                  effectiveOptions.enumerationLimit() == 0
                      ? 1
                      : Math.min(1, effectiveOptions.enumerationLimit()))
              : List.of();

      List<KnownComponent> preferred =
          !tuples.isEmpty()
              ? tuples.get(0)
              : componentRules.stream()
                  .map(ComponentConstructionRules::finalRules)
                  .filter(finalRules -> !finalRules.isEmpty())
                  .map(finalRules -> finalRules.get(0))
                  .toList();

      preferred.forEach(kc -> registerRecognized(recognizedCatalogueMap, kc, edgeCount));
      componentRules.stream()
          .map(ComponentConstructionRules::finalRules)
          .forEach(
              finalRules ->
                  registerConstructionRuleVariants(finalRules, recognizedCatalogueMap, edgeCount));

      partitionEvaluations.add(
          new PartitionEvaluation(partition, partitionIndex, ruleCounts, tuples));

      if (timeExceeded(effectiveOptions, timing)) {
        terminationReason = "time_budget_exceeded_during_validation";
        break;
      }
    }

    if (terminationReason == null) {
      Set<String> globalJoinNodes =
          JoinNodeUtils.computeJoinNodes(
              List.of(new Component(fullBits, vertices)), extraction.freeVariables());
      List<KnownComponent> globalCandidates = builder.constructionRules(fullBits, globalJoinNodes);
      globalCatalogue = globalCandidates;
      finalComponent = selectPreferredFinalComponent(globalCandidates);
    }

    long elapsed = timing.elapsedMillis();
    if (terminationReason == null && timeExceeded(effectiveOptions, elapsed)) {
      terminationReason = "time_budget_exceeded";
    }

    List<KnownComponent> recognizedCatalogue = new ArrayList<>(recognizedCatalogueMap.values());

    return buildResult(
        extraction,
        vertices,
        partitions,
        filteredPartitions,
        cpqPartitions,
        recognizedCatalogue,
        finalComponent,
        globalCatalogue,
        partitionEvaluations,
        diagnostics,
        elapsed,
        terminationReason);
  }

  private void registerConstructionRuleVariants(
      List<KnownComponent> constructionRules,
      Map<ComponentKey, KnownComponent> catalogue,
      int edgeCount) {
    if (constructionRules == null) {
      return;
    }

    for (KnownComponent rule : constructionRules) {
      registerRecognized(catalogue, rule, edgeCount);
    }
  }

  private void registerRecognized(
      Map<ComponentKey, KnownComponent> catalogue, KnownComponent rule, int edgeCount) {
    ComponentKey key = rule.toKey(edgeCount);
    catalogue.putIfAbsent(key, rule);
  }

  private KnownComponent selectPreferredFinalComponent(List<KnownComponent> rules) {
    if (rules == null || rules.isEmpty()) {
      return null;
    }
    return rules.get(0);
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
        cpqPartitions != null ? cpqPartitions : List.of(),
        recognizedCatalogue != null ? recognizedCatalogue : List.of(),
        finalComponent,
        globalCatalogue != null ? globalCatalogue : List.of(),
        partitionEvaluations != null ? partitionEvaluations : List.of(),
        diagnostics,
        elapsedMillis,
        terminationReason);
  }

  private boolean timeExceeded(DecompositionOptions options, Timing timing) {
    return options.timeBudgetMs() > 0 && timing.elapsedMillis() > options.timeBudgetMs();
  }

  private boolean timeExceeded(DecompositionOptions options, long elapsedMillis) {
    return options.timeBudgetMs() > 0 && elapsedMillis > options.timeBudgetMs();
  }
}
