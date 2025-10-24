package decomposition;

import decomposition.cpq.KnownComponent;
import decomposition.model.Partition;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Captures per-partition evaluation details such as option counts and enumerated tuples. */
public record PartitionEvaluation(
    Partition partition,
    int partitionIndex,
    List<Integer> componentOptionCounts,
    List<List<KnownComponent>> decompositionTuples) {

  public PartitionEvaluation(
      Partition partition,
      int partitionIndex,
      List<Integer> componentOptionCounts,
      List<List<KnownComponent>> decompositionTuples) {
    this.partition = Objects.requireNonNull(partition, "partition");
    if (partitionIndex < 1) {
      throw new IllegalArgumentException("partitionIndex must be >= 1");
    }
    this.partitionIndex = partitionIndex;
    this.componentOptionCounts =
        List.copyOf(Objects.requireNonNull(componentOptionCounts, "componentOptionCounts"));
    this.decompositionTuples =
        deepCopy(Objects.requireNonNull(decompositionTuples, "decompositionTuples"));
  }

  public int componentCount() {
    return componentOptionCounts.size();
  }

  private static List<List<KnownComponent>> deepCopy(List<List<KnownComponent>> tuples) {
    List<List<KnownComponent>> copy = new ArrayList<>(tuples.size());
    for (List<KnownComponent> tuple : tuples) {
      copy.add(List.copyOf(tuple));
    }
    return List.copyOf(copy);
  }
}
