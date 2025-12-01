package decomposition.core;

import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Captures per-partition evaluation details such as expression counts and enumerated tuples. */
public record PartitionEvaluation(
    Partition partition,
    int partitionIndex,
    List<Integer> componentExpressionCounts,
    List<List<CPQExpression>> decompositionTuples,
    int maxDiameter) {

  public PartitionEvaluation(
      Partition partition,
      int partitionIndex,
      List<Integer> componentExpressionCounts,
      List<List<CPQExpression>> decompositionTuples,
      int maxDiameter) {
    this.partition = Objects.requireNonNull(partition, "partition");
    if (partitionIndex < 1) {
      throw new IllegalArgumentException("partitionIndex must be >= 1");
    }
    this.partitionIndex = partitionIndex;
    this.componentExpressionCounts =
        List.copyOf(Objects.requireNonNull(componentExpressionCounts, "componentExpressionCounts"));
    this.decompositionTuples =
        deepCopy(Objects.requireNonNull(decompositionTuples, "decompositionTuples"));
    this.maxDiameter = maxDiameter;
  }

  public int componentCount() {
    return componentExpressionCounts.size();
  }

  private static List<List<CPQExpression>> deepCopy(List<List<CPQExpression>> tuples) {
    List<List<CPQExpression>> copy = new ArrayList<>(tuples.size());
    for (List<CPQExpression> tuple : tuples) {
      copy.add(List.copyOf(tuple));
    }
    return List.copyOf(copy);
  }
}
