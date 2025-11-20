package decomposition.pipeline.partitioning;

import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Component;
import decomposition.core.model.Partition;
import decomposition.util.JoinAnalysis;
import decomposition.util.JoinAnalysisBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Applies structural filters on partitions. */
public final class PartitionFilter {
  private final int maxJoinNodesPerComponent;

  public PartitionFilter(int maxJoinNodesPerComponent) {
    this.maxJoinNodesPerComponent = maxJoinNodesPerComponent;
  }

  public record FilterResult(
      List<FilteredPartition> partitions,
      List<PartitionDiagnostic> diagnostics,
      int consideredCount) {}

  public FilterResult filter(List<Partition> partitions, Set<String> freeVariables) {
    Objects.requireNonNull(partitions, "partitions");
    Objects.requireNonNull(freeVariables, "freeVariables");

    List<FilteredPartition> accepted = new ArrayList<>();
    List<PartitionDiagnostic> diagnostics = new ArrayList<>();

    int index = 0;
    for (Partition partition : partitions) {
      index++;
      JoinAnalysis analysis = JoinAnalysisBuilder.analyzePartition(partition, freeVariables);
      PartitionDiagnostic failure = violatesConstraints(partition, freeVariables, analysis, index);
      if (failure == null) {
        accepted.add(new FilteredPartition(partition, analysis));
      } else {
        diagnostics.add(failure);
      }
    }

    return new FilterResult(List.copyOf(accepted), diagnostics, partitions.size());
  }

  private PartitionDiagnostic violatesConstraints(
      Partition partition, Set<String> freeVariables, JoinAnalysis analysis, int partitionIndex) {
    if (partition.components().size() > 1) {
      // Free variables must remain visible as join nodes. Presence (multiplicity > 0) suffices,
      // because join-node computation later always includes free vars while also allowing
      // additional shared vertices to act as joins.
      for (String freeVar : freeVariables) {
        int count = analysis.multiplicity().getOrDefault(freeVar, 0);
        if (count == 0) {
          return PartitionDiagnostic.freeVariableAbsent(partitionIndex, freeVar);
        }
      }
    }

    int componentIndex = 1;
    for (Component component : partition.components()) {
      Set<String> localJoinNodes = analysis.joinNodesForComponent(component);
      if (localJoinNodes.size() > maxJoinNodesPerComponent) {
        return PartitionDiagnostic.excessJoinNodes(
            partitionIndex, componentIndex, maxJoinNodesPerComponent, localJoinNodes.size());
      }
      componentIndex++;
    }
    return null;
  }
}
