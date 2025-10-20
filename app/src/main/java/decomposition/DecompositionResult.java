package decomposition;

import decomposition.cpq.KnownComponent;
import decomposition.model.Edge;
import decomposition.model.Partition;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Aggregated outcome of running the decomposition pipeline.
 */
public record DecompositionResult(
        List<Edge> edges,
        Set<String> freeVariables,
        int vertexCount,
        int totalPartitions,
        int filteredPartitions,
        List<Partition> allPartitions,
        List<Partition> filteredPartitionList,
        Partition winningPartition,
        List<Partition> cpqPartitions,
        List<KnownComponent> recognizedComponents,
        List<KnownComponent> recognizedCatalogue,
        KnownComponent finalComponent,
        List<KnownComponent> globalCatalogue,
        List<String> diagnostics,
        long elapsedMillis,
        String terminationReason) {

    public DecompositionResult {
        Objects.requireNonNull(edges, "edges");
        Objects.requireNonNull(freeVariables, "freeVariables");
        Objects.requireNonNull(allPartitions, "allPartitions");
        Objects.requireNonNull(filteredPartitionList, "filteredPartitionList");
        Objects.requireNonNull(cpqPartitions, "cpqPartitions");
        Objects.requireNonNull(recognizedComponents, "recognizedComponents");
        Objects.requireNonNull(recognizedCatalogue, "recognizedCatalogue");
        Objects.requireNonNull(globalCatalogue, "globalCatalogue");
        Objects.requireNonNull(diagnostics, "diagnostics");
        edges = List.copyOf(edges);
        freeVariables = Set.copyOf(freeVariables);
        allPartitions = Collections.unmodifiableList(allPartitions);
        filteredPartitionList = Collections.unmodifiableList(filteredPartitionList);
        cpqPartitions = Collections.unmodifiableList(cpqPartitions);
        recognizedComponents = List.copyOf(recognizedComponents);
        recognizedCatalogue = List.copyOf(recognizedCatalogue);
        globalCatalogue = List.copyOf(globalCatalogue);
        diagnostics = List.copyOf(diagnostics);
    }

    public boolean hasWinningPartition() {
        return winningPartition != null;
    }

    public boolean hasFinalComponent() {
        return finalComponent != null;
    }
}
