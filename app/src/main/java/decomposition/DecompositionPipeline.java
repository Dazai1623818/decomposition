package decomposition;

import decomposition.DecompositionOptions.Mode;
import decomposition.cpq.ComponentKey;
import decomposition.cpq.CPQRecognizer;
import decomposition.cpq.KnownComponent;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilterSorter;
import decomposition.partitions.PartitionFilterSorter.FilterResult;
import decomposition.partitions.PartitionGenerator;
import decomposition.util.GraphUtils;
import decomposition.util.Timing;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            return buildResult(extraction, vertices, partitions, filteredPartitions, null,
                    List.of(), List.of(), null, List.of(), List.of(), diagnostics, timing.elapsedMillis(), terminationReason, effectiveOptions.mode());
        }

        Partition winningPartition = null;
        List<KnownComponent> winningComponents = List.of();
        KnownComponent finalComponent = null;
        List<Partition> cpqPartitions = new ArrayList<>();
        List<KnownComponent> globalCatalogue = List.of();

        if (effectiveOptions.mode().decomposeEnabled()) {
            CPQRecognizer recognizer = new CPQRecognizer(edges);

            int partitionIndex = 0;
            for (Partition partition : filteredPartitions) {
                partitionIndex++;
                List<KnownComponent> recognized = new ArrayList<>();
                boolean allRecognized = true;
                int componentIndex = 0;
                for (Component component : partition.components()) {
                    componentIndex++;
                    Optional<KnownComponent> recognizedOpt = recognizer.recognize(component);
                    if (recognizedOpt.isEmpty()) {
                        diagnostics.add("Partition#" + partitionIndex + " skipped: component #" + componentIndex + " not recognized");
                        allRecognized = false;
                        break;
                    }
                    recognized.add(recognizedOpt.get());
                }

                if (allRecognized) {
                    cpqPartitions.add(partition);
                    for (KnownComponent kc : recognized) {
                        recognizedCatalogueMap.putIfAbsent(kc.toKey(edgeCount), kc);
                    }
                    if (winningPartition == null) {
                        winningPartition = partition;
                        winningComponents = List.copyOf(recognized);
                    }
                }

                if (timeExceeded(effectiveOptions, timing)) {
                    terminationReason = "time_budget_exceeded_during_recognition";
                    break;
                }
            }

            if (winningPartition != null) {
                BitSet globalBits = new BitSet(edgeCount);
                globalBits.set(0, edgeCount);
                Component whole = new Component(globalBits, GraphUtils.vertices(globalBits, edges));
                finalComponent = recognizer.recognize(whole).orElse(null);
                globalCatalogue = recognizer.enumerateAll(whole);
            } else {
                BitSet globalBits = new BitSet(edgeCount);
                globalBits.set(0, edgeCount);
                Component whole = new Component(globalBits, GraphUtils.vertices(globalBits, edges));
                globalCatalogue = recognizer.enumerateAll(whole);
            }
        } else {
            globalCatalogue = List.of();
        }

        long elapsed = timing.elapsedMillis();
        if (terminationReason == null && timeExceeded(effectiveOptions, elapsed)) {
            terminationReason = "time_budget_exceeded";
        }

        return buildResult(extraction, vertices, partitions, filteredPartitions,
                winningPartition, winningComponents, new ArrayList<>(recognizedCatalogueMap.values()),
                finalComponent, cpqPartitions, globalCatalogue,
                diagnostics, elapsed, terminationReason, effectiveOptions.mode());
    }

    private DecompositionResult buildResult(ExtractionResult extraction,
                                            Set<String> vertices,
                                            List<Partition> partitions,
                                            List<Partition> filteredPartitions,
                                            Partition winningPartition,
                                            List<KnownComponent> winningComponents,
                                            List<KnownComponent> recognizedCatalogue,
                                            KnownComponent finalComponent,
                                            List<Partition> cpqPartitions,
                                            List<KnownComponent> globalCatalogue,
                                            List<String> diagnostics,
                                            long elapsedMillis,
                                            String terminationReason,
                                            Mode mode) {
        List<Partition> partitionsForResult = mode.partitionsEnabled()
                ? filteredPartitions
                : List.of();
        return new DecompositionResult(
                extraction.edges(),
                extraction.freeVariables(),
                vertices.size(),
                partitions.size(),
                filteredPartitions.size(),
                partitions,
                partitionsForResult,
                winningPartition,
                cpqPartitions,
                winningComponents != null ? winningComponents : List.of(),
                recognizedCatalogue != null ? recognizedCatalogue : List.of(),
                finalComponent,
                globalCatalogue != null ? globalCatalogue : List.of(),
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
