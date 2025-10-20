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
        List<Partition> cpqPartitions,
        List<KnownComponent> recognizedCatalogue,
        KnownComponent finalComponent,
        List<KnownComponent> globalCatalogue,
        List<PartitionEvaluation> partitionEvaluations,
        List<String> diagnostics,
        long elapsedMillis,
        String terminationReason) {

    public DecompositionResult {
        Objects.requireNonNull(edges, "edges");
        Objects.requireNonNull(freeVariables, "freeVariables");
        Objects.requireNonNull(allPartitions, "allPartitions");
        Objects.requireNonNull(filteredPartitionList, "filteredPartitionList");
        Objects.requireNonNull(cpqPartitions, "cpqPartitions");
        Objects.requireNonNull(recognizedCatalogue, "recognizedCatalogue");
        Objects.requireNonNull(globalCatalogue, "globalCatalogue");
        Objects.requireNonNull(diagnostics, "diagnostics");
        Objects.requireNonNull(partitionEvaluations, "partitionEvaluations");
        edges = List.copyOf(edges);
        freeVariables = Set.copyOf(freeVariables);
        allPartitions = Collections.unmodifiableList(allPartitions);
        filteredPartitionList = Collections.unmodifiableList(filteredPartitionList);
        cpqPartitions = Collections.unmodifiableList(cpqPartitions);
        recognizedCatalogue = List.copyOf(recognizedCatalogue);
        globalCatalogue = List.copyOf(globalCatalogue);
        partitionEvaluations = List.copyOf(partitionEvaluations);
        diagnostics = List.copyOf(diagnostics);
    }

    public boolean hasFinalComponent() {
        return finalComponent != null;
    }
}
