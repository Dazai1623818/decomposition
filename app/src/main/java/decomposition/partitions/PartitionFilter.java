package decomposition.partitions;

import decomposition.model.Component;
import decomposition.model.Partition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Applies structural filters on partitions.
 */
public final class PartitionFilter {
    private final int maxJoinNodesPerComponent;

    public PartitionFilter(int maxJoinNodesPerComponent) {
        this.maxJoinNodesPerComponent = maxJoinNodesPerComponent;
    }

    public record FilterResult(List<Partition> partitions,
                               List<String> diagnostics,
                               int consideredCount) {
    }

    public FilterResult filter(List<Partition> partitions, Set<String> freeVariables) {
        Objects.requireNonNull(partitions, "partitions");
        Objects.requireNonNull(freeVariables, "freeVariables");

        List<Partition> accepted = new ArrayList<>();
        List<String> diagnostics = new ArrayList<>();

        int index = 0;
        for (Partition partition : partitions) {
            index++;
            String failure = violatesConstraints(partition, freeVariables);
            if (failure == null) {
                accepted.add(partition);
            } else {
                diagnostics.add("Partition#" + index + " rejected: " + failure);
            }
        }

        return new FilterResult(List.copyOf(accepted), diagnostics, partitions.size());
    }

    private String violatesConstraints(Partition partition, Set<String> freeVariables) {
        Map<String, Integer> multiplicity = vertexMultiplicity(partition);
        if (partition.components().size() > 1) {
            int requiredMultiplicity = freeVariables.size() <= 1 ? 2 : 1;
            for (String freeVar : freeVariables) {
                int count = multiplicity.getOrDefault(freeVar, 0);
                if (count < requiredMultiplicity) {
                    if (count == 0) {
                        return "free var " + freeVar + " absent from partition";
                    }
                    return "free var " + freeVar + " not a join node";
                }
            }
        }

        for (Component component : partition.components()) {
            long joinNodes = component.vertices().stream()
                    .filter(v -> multiplicity.getOrDefault(v, 0) >= 2)
                    .count();
            if (joinNodes > maxJoinNodesPerComponent) {
                return "component had >" + maxJoinNodesPerComponent + " join nodes";
            }
        }
        return null;
    }

    private Map<String, Integer> vertexMultiplicity(Partition partition) {
        Map<String, Integer> counts = new HashMap<>();
        for (Component component : partition.components()) {
            for (String vertex : component.vertices()) {
                counts.merge(vertex, 1, Integer::sum);
            }
        }
        return counts;
    }
}
