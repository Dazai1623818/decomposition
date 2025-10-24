package decomposition.partitions;

import decomposition.model.Component;
import decomposition.model.Partition;
import decomposition.util.JoinNodeUtils;
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

  public record FilteredPartition(Partition partition, Set<String> joinNodes) {}

  public FilterResult filter(List<Partition> partitions, Set<String> freeVariables) {
    Objects.requireNonNull(partitions, "partitions");
    Objects.requireNonNull(freeVariables, "freeVariables");

    List<FilteredPartition> accepted = new ArrayList<>();
    List<String> diagnostics = new ArrayList<>();

    int index = 0;
    for (Partition partition : partitions) {
      index++;
      Map<String, Integer> multiplicity = JoinNodeUtils.computeVertexMultiplicity(partition);
      String failure = violatesConstraints(partition, freeVariables, multiplicity);
      if (failure == null) {
        Set<String> joinNodes =
            JoinNodeUtils.computeJoinNodesFromMultiplicity(multiplicity, freeVariables);
        accepted.add(new FilteredPartition(partition, joinNodes));
      } else {
        diagnostics.add("Partition#" + index + " rejected: " + failure);
      }
    }

    return new FilterResult(List.copyOf(accepted), diagnostics, partitions.size());
  }

  private String violatesConstraints(
      Partition partition, Set<String> freeVariables, Map<String, Integer> multiplicity) {
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
      long joinNodes =
          component.vertices().stream().filter(v -> multiplicity.getOrDefault(v, 0) >= 2).count();
      if (joinNodes > maxJoinNodesPerComponent) {
        return "component had >" + maxJoinNodesPerComponent + " join nodes";
      }
    }
    return null;
  }
}
