package decomposition;

import decomposition.PartitionEvaluation;
import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.ComponentKey;
import decomposition.cpq.KnownComponent;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.partitions.PartitionValidator;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilterSorter;
import decomposition.partitions.PartitionFilterSorter.FilterResult;
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

        BitSet fullBits = new BitSet(edgeCount);
        fullBits.set(0, edgeCount);
        Set<String> vertices = GraphUtils.vertices(fullBits, edges);

        PartitionGenerator generator = new PartitionGenerator(effectiveOptions.maxPartitions());
        List<Component> components = generator.enumerateConnectedComponents(edges);
        List<Partition> partitions = generator.enumeratePartitions(edges, components);

        FilterResult filterResult = new PartitionFilterSorter(MAX_JOIN_NODES)
                .filterAndSort(partitions, extraction.freeVariables());
        List<Partition> filteredPartitions = filterResult.partitions();

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
        for (Partition partition : filteredPartitions) {
            partitionIndex++;
            List<Component> componentsInPartition = partition.components();
            Set<String> joinNodes = computeJoinNodes(componentsInPartition, extraction.freeVariables());
            boolean valid = validator.isValidCPQDecomposition(partition, builder, extraction.freeVariables(), edges);
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
                    List<KnownComponent> rawOptions = builder.options(componentBits);
                    Set<String> componentJoinNodes = localJoinNodeCache.computeIfAbsent(component,
                            c -> localJoinNodes(c, joinNodes));
                    Map<String, JoinNodeRole> componentJoinRoles = joinNodeRoleCache.computeIfAbsent(component,
                            c -> JoinNodeUtils.computeJoinNodeRoles(c, joinNodes, edges));
                    List<KnownComponent> filteredOptions = shouldEnforceJoinNodes(joinNodes, componentsInPartition.size(), component)
                            ? rawOptions.stream()
                                    .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(
                                            kc, component, componentJoinNodes, componentJoinRoles))
                                    .collect(Collectors.toList())
                            : rawOptions;

                    optionCounts.add(filteredOptions.size());
                    filteredOptionsPerComponent.add(filteredOptions);
                }
                List<List<KnownComponent>> tuples = effectiveOptions.mode().enumerateTuples()
                        ? validator.enumerateDecompositions(
                                partition,
                                builder,
                                effectiveOptions.enumerationLimit() == 0 ? 1 : Math.min(1, effectiveOptions.enumerationLimit()),
                                extraction.freeVariables(),
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

                for (int i = 0; i < componentsInPartition.size(); i++) {
                    Component component = componentsInPartition.get(i);
                    Set<String> componentJoinNodes = localJoinNodeCache.computeIfAbsent(component,
                            c -> localJoinNodes(c, joinNodes));
                    Map<String, JoinNodeRole> componentJoinRoles = joinNodeRoleCache.computeIfAbsent(component,
                            c -> JoinNodeUtils.computeJoinNodeRoles(c, joinNodes, edges));
                    registerOptionVariants(filteredOptionsPerComponent.get(i), component,
                            componentJoinNodes, componentJoinRoles, recognizedCatalogueMap, edgeCount);
                }

                partitionEvaluations.add(new PartitionEvaluation(partition, partitionIndex, optionCounts, tuples));
            } else {
                int componentIndex = 0;
                Map<Component, Set<String>> localJoinNodeCache = new HashMap<>();
                Map<Component, Map<String, JoinNodeRole>> joinNodeRoleCache = new HashMap<>();
                for (Component component : componentsInPartition) {
                    componentIndex++;
                    BitSet componentBits = component.edgeBits();
                    List<KnownComponent> rawOptions = builder.options(componentBits);
                    Set<String> componentJoinNodes = localJoinNodeCache.computeIfAbsent(component,
                            c -> localJoinNodes(c, joinNodes));
                    Map<String, JoinNodeRole> componentJoinRoles = joinNodeRoleCache.computeIfAbsent(component,
                            c -> JoinNodeUtils.computeJoinNodeRoles(c, joinNodes, edges));
                    List<KnownComponent> filteredOptions = shouldEnforceJoinNodes(joinNodes, componentsInPartition.size(), component)
                            ? rawOptions.stream()
                                    .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(
                                            kc, component, componentJoinNodes, componentJoinRoles))
                                    .collect(Collectors.toList())
                            : rawOptions;

                    if (rawOptions.isEmpty()) {
                        String signature = BitsetUtils.signature(componentBits, edgeCount);
                        diagnostics.add("Partition#" + partitionIndex + " component#" + componentIndex
                                + " rejected: no CPQ candidates for bits " + signature);
                    } else if (filteredOptions.isEmpty()) {
                        String signature = BitsetUtils.signature(componentBits, edgeCount);
                        diagnostics.add("Partition#" + partitionIndex + " component#" + componentIndex
                                + " rejected: endpoints not on join nodes for bits " + signature);
                    }
                }
            }

            if (timeExceeded(effectiveOptions, timing)) {
                terminationReason = "time_budget_exceeded_during_validation";
                break;
            }
        }

        if (terminationReason == null) {
            List<KnownComponent> globalCandidates = builder.options(fullBits);
            globalCatalogue = globalCandidates;
            finalComponent = selectPreferredFinalComponent(globalCandidates);
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

    private void registerOptionVariants(List<KnownComponent> options,
                                        Component component,
                                        Set<String> componentJoinNodes,
                                        Map<String, JoinNodeRole> joinNodeRoles,
                                        Map<ComponentKey, KnownComponent> catalogue,
                                        int edgeCount) {
        if (options == null || options.isEmpty()) {
            return;
        }

        KnownComponent first = options.get(0);
        registerRecognized(catalogue, first, edgeCount);

        options.stream()
                .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(
                        kc, component, componentJoinNodes, joinNodeRoles) && isAnchored(kc))
                .findFirst()
                .ifPresent(kc -> registerRecognized(catalogue, kc, edgeCount));

        options.stream()
                .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(
                        kc, component, componentJoinNodes, joinNodeRoles) && !isAnchored(kc))
                .findFirst()
                .ifPresent(kc -> registerRecognized(catalogue, kc, edgeCount));
    }

    private void registerRecognized(Map<ComponentKey, KnownComponent> catalogue,
                                    KnownComponent candidate,
                                    int edgeCount) {
        ComponentKey key = candidate.toKey(edgeCount);
        catalogue.merge(key, candidate, this::preferRecognizedCandidate);
    }

    private KnownComponent preferRecognizedCandidate(KnownComponent existing,
                                                     KnownComponent candidate) {
        boolean existingAnchored = isAnchored(existing);
        boolean candidateAnchored = isAnchored(candidate);
        if (candidateAnchored && !existingAnchored) {
            return candidate;
        }
        if (candidateAnchored == existingAnchored) {
            int existingLength = existing.cpq().toString().length();
            int candidateLength = candidate.cpq().toString().length();
            if (candidateLength > existingLength) {
                return candidate;
            }
        }
        return existing;
    }

    private boolean isAnchored(KnownComponent component) {
        String normalized = component.cpq().toString().replace(" ", "");
        return normalized.endsWith("∩id") || normalized.contains(")∩id");
    }

    private KnownComponent selectPreferredFinalComponent(List<KnownComponent> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        return candidates.stream()
                .filter(kc -> !isAnchored(kc))
                .findFirst()
                .orElse(candidates.get(0));
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

    private Set<String> computeJoinNodes(List<Component> components, Set<String> freeVariables) {
        Map<String, Integer> counts = new HashMap<>();
        for (Component component : components) {
            for (String vertex : component.vertices()) {
                counts.merge(vertex, 1, Integer::sum);
            }
        }

        Set<String> joinNodes = new HashSet<>();
        if (freeVariables != null) {
            joinNodes.addAll(freeVariables);
        }

        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() >= 2) {
                joinNodes.add(entry.getKey());
            }
        }
        return joinNodes;
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
