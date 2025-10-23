package decomposition;
import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.ComponentKey;
import decomposition.cpq.KnownComponent;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.partitions.PartitionValidator;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilter;
import decomposition.partitions.PartitionFilter.FilteredPartition;
import decomposition.partitions.PartitionFilter.FilterResult;
import decomposition.partitions.PartitionGenerator;
import decomposition.util.BitsetUtils;
import decomposition.util.GraphUtils;
import decomposition.util.JoinNodeUtils;
import decomposition.util.JoinNodeUtils.JoinNodeRole;
import decomposition.util.Timing;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import dev.roanh.gmark.lang.cq.CQ;

/**
 * Orchestrates the CQ to CPQ decomposition pipeline.
 */
public final class DecompositionPipeline {
    private static final int MAX_JOIN_NODES = 2;
    private final CQExtractor extractor = new CQExtractor();

    public DecompositionResult execute(CQ cq,
                                       Set<String> explicitFreeVariables,
                                       DecompositionOptions options) {
        DecompositionOptions effectiveOptions = options != null ? options : DecompositionOptions.defaults();
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

        FilterResult filterResult = new PartitionFilter(MAX_JOIN_NODES)
                .filter(partitions, extraction.freeVariables());
        List<FilteredPartition> filteredPartitionsWithJoins = filterResult.partitions();
        List<Partition> filteredPartitions = List.copyOf(
                filteredPartitionsWithJoins.stream()
                        .map(FilteredPartition::partition)
                        .collect(Collectors.toList()));

        List<String> diagnostics = new ArrayList<>(filterResult.diagnostics());
        Map<ComponentKey, KnownComponent> recognizedCatalogueMap = new LinkedHashMap<>();
        String terminationReason = null;

        if (timeExceeded(effectiveOptions, timing)) {
            terminationReason = "time_budget_exceeded_after_partitioning";
            return buildResult(extraction, vertices, partitions, filteredPartitions,
                    List.of(), List.of(), null, List.of(), List.of(),
                    diagnostics, timing.elapsedMillis(), terminationReason);
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
            List<Component> componentsInPartition = partition.components();
            boolean valid = validator.isValidCPQDecomposition(
                    partition,
                    joinNodes,
                    builder,
                    extraction.freeVariables(),
                    freeVariableOrder,
                    edges);
            if (valid) {
                cpqPartitions.add(partition);
                List<Integer> optionCounts = new ArrayList<>();
                List<List<KnownComponent>> filteredOptionsPerComponent = new ArrayList<>();
                Map<Component, Set<String>> localJoinNodeCache = new HashMap<>();
                Map<Component, Map<String, JoinNodeRole>> joinNodeRoleCache = new HashMap<>();
                int componentIndex = 0;
                for (Component component : componentsInPartition) {
                    componentIndex++;
                    BitSet componentBits = component.edgeBits();
                    List<KnownComponent> rawOptions = builder.options(componentBits, joinNodes);
                    Set<String> componentJoinNodes = localJoinNodeCache.computeIfAbsent(component,
                            c -> localJoinNodes(c, joinNodes));
                    Map<String, JoinNodeRole> componentJoinRoles = joinNodeRoleCache.computeIfAbsent(component,
                            c -> JoinNodeUtils.computeJoinNodeRoles(c, joinNodes, edges));
                    List<KnownComponent> joinFilteredOptions = shouldEnforceJoinNodes(joinNodes, componentsInPartition.size(), component)
                            ? rawOptions.stream()
                                    .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(
                                            kc, component, componentJoinNodes, componentJoinRoles))
                                    .collect(Collectors.toList())
                            : rawOptions;
                    List<KnownComponent> filteredOptions = enforceFreeVariableOrdering(
                            joinFilteredOptions,
                            component,
                            componentsInPartition,
                            freeVariableOrder);

                    optionCounts.add(filteredOptions.size());
                    filteredOptionsPerComponent.add(filteredOptions);
                }
                List<List<KnownComponent>> tuples = effectiveOptions.mode().enumerateTuples()
                        ? validator.enumerateDecompositions(
                                partition,
                                joinNodes,
                                builder,
                                effectiveOptions.enumerationLimit() == 0 ? 1 : Math.min(1, effectiveOptions.enumerationLimit()),
                                extraction.freeVariables(),
                                freeVariableOrder,
                                edges)
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

                partitionEvaluations.add(new PartitionEvaluation(partition, partitionIndex, optionCounts, tuples));
            } else {
                int componentIndex = 0;
                Map<Component, Set<String>> localJoinNodeCache = new HashMap<>();
                Map<Component, Map<String, JoinNodeRole>> joinNodeRoleCache = new HashMap<>();
                for (Component component : componentsInPartition) {
                    componentIndex++;
                    BitSet componentBits = component.edgeBits();
                    List<KnownComponent> rawOptions = builder.options(componentBits, joinNodes);
                    Set<String> componentJoinNodes = localJoinNodeCache.computeIfAbsent(component,
                            c -> localJoinNodes(c, joinNodes));
                    Map<String, JoinNodeRole> componentJoinRoles = joinNodeRoleCache.computeIfAbsent(component,
                            c -> JoinNodeUtils.computeJoinNodeRoles(c, joinNodes, edges));
                    List<KnownComponent> joinFilteredOptions = shouldEnforceJoinNodes(joinNodes, componentsInPartition.size(), component)
                            ? rawOptions.stream()
                                    .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(
                                            kc, component, componentJoinNodes, componentJoinRoles))
                                    .collect(Collectors.toList())
                            : rawOptions;
                    List<KnownComponent> orientationFilteredOptions = enforceFreeVariableOrdering(
                            joinFilteredOptions,
                            component,
                            componentsInPartition,
                            freeVariableOrder);

                    if (rawOptions.isEmpty()) {
                        String signature = BitsetUtils.signature(componentBits, edgeCount);
                        diagnostics.add("Partition#" + partitionIndex + " component#" + componentIndex
                                + " rejected: no CPQ candidates for bits " + signature);
                    } else if (joinFilteredOptions.isEmpty()) {
                        String signature = BitsetUtils.signature(componentBits, edgeCount);
                        diagnostics.add("Partition#" + partitionIndex + " component#" + componentIndex
                                + " rejected: endpoints not on join nodes for bits " + signature);
                    } else if (orientationFilteredOptions.isEmpty()) {
                        String signature = BitsetUtils.signature(componentBits, edgeCount);
                        diagnostics.add("Partition#" + partitionIndex + " component#" + componentIndex
                                + " rejected: endpoints violate free-variable ordering for bits " + signature);
                    }
                }
            }

            if (timeExceeded(effectiveOptions, timing)) {
                terminationReason = "time_budget_exceeded_during_validation";
                break;
            }
        }

        if (terminationReason == null) {
            Set<String> globalJoinNodes = JoinNodeUtils.computeJoinNodes(List.of(new Component(fullBits, vertices)), extraction.freeVariables());
            List<KnownComponent> globalCandidates = builder.options(fullBits, globalJoinNodes);
            globalCatalogue = globalCandidates;
            List<KnownComponent> orderedGlobalCandidates = filterGlobalCandidates(globalCandidates, freeVariableOrder);
            finalComponent = selectPreferredFinalComponent(orderedGlobalCandidates);
        }

        long elapsed = timing.elapsedMillis();
        if (terminationReason == null && timeExceeded(effectiveOptions, elapsed)) {
            terminationReason = "time_budget_exceeded";
        }

        List<KnownComponent> recognizedCatalogue = new ArrayList<>(recognizedCatalogueMap.values());

        return buildResult(extraction, vertices, partitions, filteredPartitions,
                cpqPartitions, recognizedCatalogue, finalComponent, globalCatalogue,
                partitionEvaluations, diagnostics, elapsed, terminationReason);
    }

    private List<KnownComponent> enforceFreeVariableOrdering(List<KnownComponent> options,
                                                             Component component,
                                                             List<Component> componentsInPartition,
                                                             List<String> freeVariableOrder) {
        if (options == null || options.isEmpty()) {
            return options;
        }
        if (componentsInPartition.size() != 1) {
            return options;
        }
        if (freeVariableOrder == null || freeVariableOrder.isEmpty()) {
            return options;
        }
        String expectedSource = freeVariableOrder.get(0);
        String expectedTarget = freeVariableOrder.size() >= 2 ? freeVariableOrder.get(1) : null;
        if (expectedSource == null) {
            return options;
        }
        Set<String> componentVertices = component.vertices();
        if (!componentVertices.contains(expectedSource)) {
            return options;
        }
        return options.stream()
                .filter(option -> matchesFreeVariableOrdering(option, expectedSource, expectedTarget))
                .collect(Collectors.toList());
    }

    private List<KnownComponent> filterGlobalCandidates(List<KnownComponent> candidates,
                                                        List<String> freeVariableOrder) {
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
                .filter(option -> matchesFreeVariableOrdering(option, expectedSource, expectedTarget))
                .collect(Collectors.toList());
    }

    private boolean matchesFreeVariableOrdering(KnownComponent option,
                                                String expectedSource,
                                                String expectedTarget) {
        // Check forward orientation: option matches (expectedSource → expectedTarget)
        boolean forwardMatch = expectedSource.equals(option.source())
                && (expectedTarget == null || expectedTarget.equals(option.target()));

        // Check reverse orientation: option matches (expectedTarget → expectedSource)
        // This is valid because the query graph homomorphism can work in either direction
        boolean reverseMatch = (expectedTarget != null)
                && expectedTarget.equals(option.source())
                && expectedSource.equals(option.target());

        return forwardMatch || reverseMatch;
    }

    private void registerOptionVariants(List<KnownComponent> options,
                                        Map<ComponentKey, KnownComponent> catalogue,
                                        int edgeCount) {
        if (options == null) {
            return;
        }

        for (KnownComponent option : options) {
            registerRecognized(catalogue, option, edgeCount);
        }
    }

    private void registerRecognized(Map<ComponentKey, KnownComponent> catalogue,
                                    KnownComponent candidate,
                                    int edgeCount) {
        ComponentKey key = candidate.toKey(edgeCount);
        catalogue.putIfAbsent(key, candidate);
    }

    private KnownComponent selectPreferredFinalComponent(List<KnownComponent> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        return candidates.get(0);
    }

    private DecompositionResult buildResult(ExtractionResult extraction,
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

    private boolean shouldEnforceJoinNodes(Set<String> joinNodes, int totalComponents, Component component) {
        if (joinNodes.isEmpty()) {
            return false;
        }
        if (totalComponents > 1) {
            return true;
        }
        return component.edgeCount() > 1;
    }

    private Set<String> localJoinNodes(Component component, Set<String> joinNodes) {
        if (joinNodes == null || joinNodes.isEmpty()) {
            return Set.of();
        }
        Set<String> local = new HashSet<>();
        for (String vertex : component.vertices()) {
            if (joinNodes.contains(vertex)) {
                local.add(vertex);
            }
        }
        return local;
    }

}
