package decomposition.partitions;

import decomposition.model.Component;
import decomposition.model.Partition;
import decomposition.util.JoinAnalysis;
import decomposition.util.JoinAnalysisBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Applies structural filters on partitions. */
public final class PartitionFilter {
  private final int maxJoinNodesPerComponent;

  public PartitionFilter(int maxJoinNodesPerComponent) {
    this.maxJoinNodesPerComponent = maxJoinNodesPerComponent;
  }

  public record FilterResult(
      List<FilteredPartition> partitions, List<String> diagnostics, int consideredCount) {}

  public FilterResult filter(List<Partition> partitions, Set<String> freeVariables) {
    Objects.requireNonNull(partitions, "partitions");
    Objects.requireNonNull(freeVariables, "freeVariables");

    List<FilteredPartition> accepted = new ArrayList<>();
    List<String> diagnostics = new ArrayList<>();

    int index = 0;
    for (Partition partition : partitions) {
      index++;
      JoinAnalysis analysis = JoinAnalysisBuilder.analyzePartition(partition, freeVariables);
      String failure = violatesConstraints(partition, freeVariables, analysis.multiplicity());
      if (failure == null) {
        accepted.add(new FilteredPartition(partition, analysis));
      } else {
        diagnostics.add("Partition#" + index + " rejected: " + failure);
      }
    }

    return new FilterResult(List.copyOf(accepted), diagnostics, partitions.size());
  }

  private String violatesConstraints(
      Partition partition, Set<String> freeVariables, Map<String, Integer> multiplicity) {
    if (partition.components().size() > 1) {
      // Free variables must remain visible as join nodes. Presence (multiplicity > 0) suffices,
      // because join-node computation later always includes free vars while also allowing
      // additional shared vertices to act as joins.
      for (String freeVar : freeVariables) {
        int count = multiplicity.getOrDefault(freeVar, 0);
        if (count == 0) {
          return "free var " + freeVar + " absent from partition";
        }
      }
    }

    for (Component component : partition.components()) {
      long joinNodes =
          component.vertices().stream()
              .filter(
                  v ->
                      multiplicity.getOrDefault(v, 0) >= 2
                          || (freeVariables != null && freeVariables.contains(v)))
              .count();
      if (joinNodes > maxJoinNodesPerComponent) {
        return "component had >" + maxJoinNodesPerComponent + " join nodes";
      }
    }
    return null;
  }
}
