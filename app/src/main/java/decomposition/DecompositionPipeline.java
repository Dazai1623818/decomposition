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
import decomposition.util.Timing;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            boolean valid = validator.isValidCPQDecomposition(partition, builder);
            if (valid) {
                cpqPartitions.add(partition);
                List<Integer> optionCounts = new ArrayList<>();
                int componentIndex = 0;
                for (Component component : partition.components()) {
                    componentIndex++;
                    BitSet componentBits = component.edgeBits();
                    List<KnownComponent> optionsForComponent = builder.options(componentBits);
                    optionCounts.add(optionsForComponent.size());
                    for (KnownComponent kc : optionsForComponent) {
                        recognizedCatalogueMap.putIfAbsent(kc.toKey(edgeCount), kc);
                    }
                }
                List<List<KnownComponent>> tuples = effectiveOptions.mode().enumerateTuples()
                        ? validator.enumerateDecompositions(partition, builder, effectiveOptions.enumerationLimit())
                        : List.of();
                partitionEvaluations.add(new PartitionEvaluation(partition, partitionIndex, optionCounts, tuples));
            } else {
                int componentIndex = 0;
                for (Component component : partition.components()) {
                    componentIndex++;
                    BitSet componentBits = component.edgeBits();
                    List<KnownComponent> optionsForComponent = builder.options(componentBits);
                    if (optionsForComponent.isEmpty()) {
                        String signature = BitsetUtils.signature(componentBits, edgeCount);
                        diagnostics.add("Partition#" + partitionIndex + " component#" + componentIndex
                                + " rejected: no CPQ candidates for bits " + signature);
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
            finalComponent = globalCandidates.isEmpty() ? null : globalCandidates.get(0);
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
}
