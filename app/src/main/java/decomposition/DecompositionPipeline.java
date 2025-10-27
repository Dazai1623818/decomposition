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
import decomposition.partitions.PartitionValidator.ComponentOptions;
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
import java.util.stream.Collectors;

/** Orchestrates the CQ to CPQ decomposition pipeline. */
public final class DecompositionPipeline {
  private static final int MAX_JOIN_NODES = 2;
  private final CQExtractor extractor = new CQExtractor();

  public DecompositionResult execute(
      CQ cq, Set<String> explicitFreeVariables, DecompositionOptions options) {
    DecompositionOptions effectiveOptions =
        options != null ? options : DecompositionOptions.defaults();
    Timing timing = Timing.start();

    ExtractionResult extraction = extractor.extract(cq, explicitFreeVariables);
    List<Edge> edges = extraction.edges();
    int edgeCount = edges.size();
    List<String> freeVariableOrder = extraction.freeVariableOrder();

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
        List.copyOf(
            filteredPartitionsWithJoins.stream()
                .map(FilteredPartition::partition)
                .collect(Collectors.toList()));

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
    PartitionValidator validator = new PartitionValidator();

    KnownComponent finalComponent = null;
    List<Partition> cpqPartitions = new ArrayList<>();
    List<PartitionEvaluation> partitionEvaluations = new ArrayList<>();
    List<KnownComponent> globalCatalogue = List.of();

    int partitionIndex = 0;
    for (FilteredPartition filteredPartition : filteredPartitionsWithJoins) {
      Partition partition = filteredPartition.partition();
      Set<String> joinNodes = filteredPartition.joinNodes();
      partitionIndex++;

      List<ComponentOptions> componentOptions =
          validator.componentOptions(
              partition, joinNodes, builder, extraction.freeVariables(), freeVariableOrder, edges);

      boolean valid = componentOptions.stream().allMatch(ComponentOptions::hasCandidates);
      if (valid) {
        cpqPartitions.add(partition);

        List<Integer> optionCounts =
            componentOptions.stream()
                .map(ComponentOptions::candidateCount)
                .collect(Collectors.toList());

        List<List<KnownComponent>> filteredOptionsPerComponent =
            componentOptions.stream()
                .map(ComponentOptions::finalOptions)
                .collect(Collectors.toList());

        List<List<KnownComponent>> tuples =
            effectiveOptions.mode().enumerateTuples()
                ? validator.enumerateDecompositions(
                    componentOptions,
                    effectiveOptions.enumerationLimit() == 0
                        ? 1
                        : Math.min(1, effectiveOptions.enumerationLimit()))
                : List.of();

        if (!tuples.isEmpty()) {
          for (KnownComponent kc : tuples.get(0)) {
            registerRecognized(recognizedCatalogueMap, kc, edgeCount);
          }
        } else {
          for (List<KnownComponent> filteredOptions : filteredOptionsPerComponent) {
            if (!filteredOptions.isEmpty()) {
              KnownComponent kc = filteredOptions.get(0);
              registerRecognized(recognizedCatalogueMap, kc, edgeCount);
            }
          }
        }

        for (List<KnownComponent> filteredOptions : filteredOptionsPerComponent) {
          registerOptionVariants(filteredOptions, recognizedCatalogueMap, edgeCount);
        }

        partitionEvaluations.add(
            new PartitionEvaluation(partition, partitionIndex, optionCounts, tuples));
      } else {
        int componentIndex = 0;
        for (ComponentOptions componentOption : componentOptions) {
          componentIndex++;
          BitSet componentBits = componentOption.component().edgeBits();
          String signature = BitsetUtils.signature(componentBits, edgeCount);

          if (componentOption.rawOptions().isEmpty()) {
            diagnostics.add(
                "Partition#"
                    + partitionIndex
                    + " component#"
                    + componentIndex
                    + " rejected: no CPQ candidates for bits "
                    + signature);
          } else if (componentOption.joinFilteredOptions().isEmpty()) {
            diagnostics.add(
                "Partition#"
                    + partitionIndex
                    + " component#"
                    + componentIndex
                    + " rejected: endpoints not on join nodes for bits "
                    + signature);
          } else if (componentOption.finalOptions().isEmpty()) {
            diagnostics.add(
                "Partition#"
                    + partitionIndex
                    + " component#"
                    + componentIndex
                    + " rejected: endpoints violate free-variable ordering for bits "
                    + signature);
          }
        }
      }

      if (timeExceeded(effectiveOptions, timing)) {
        terminationReason = "time_budget_exceeded_during_validation";
        break;
      }
    }

    if (terminationReason == null) {
      Set<String> globalJoinNodes =
          JoinNodeUtils.computeJoinNodes(
              List.of(new Component(fullBits, vertices)), extraction.freeVariables());
      List<KnownComponent> globalCandidates = builder.options(fullBits, globalJoinNodes);
      globalCatalogue = globalCandidates;
      List<KnownComponent> orderedGlobalCandidates =
          filterGlobalCandidates(globalCandidates, freeVariableOrder);
      finalComponent = selectPreferredFinalComponent(orderedGlobalCandidates);
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

  private List<KnownComponent> filterGlobalCandidates(
      List<KnownComponent> candidates, List<String> freeVariableOrder) {
    if (candidates == null || candidates.isEmpty()) {
      return List.of();
    }
    if (freeVariableOrder == null || freeVariableOrder.isEmpty()) {
      return candidates;
    }
    String expectedSource = freeVariableOrder.get(0);
    String expectedTarget = freeVariableOrder.size() >= 2 ? freeVariableOrder.get(1) : null;
    if (expectedSource == null) {
      return candidates;
    }
    return candidates.stream()
        .filter(
            option ->
                JoinNodeUtils.matchesFreeVariableOrdering(option, expectedSource, expectedTarget))
        .collect(Collectors.toList());
  }

  private void registerOptionVariants(
      List<KnownComponent> options, Map<ComponentKey, KnownComponent> catalogue, int edgeCount) {
    if (options == null) {
      return;
    }

    for (KnownComponent option : options) {
      registerRecognized(catalogue, option, edgeCount);
    }
  }

  private void registerRecognized(
      Map<ComponentKey, KnownComponent> catalogue, KnownComponent candidate, int edgeCount) {
    ComponentKey key = candidate.toKey(edgeCount);
    catalogue.putIfAbsent(key, candidate);
  }

  private KnownComponent selectPreferredFinalComponent(List<KnownComponent> candidates) {
    if (candidates == null || candidates.isEmpty()) {
      return null;
    }
    return candidates.get(0);
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
