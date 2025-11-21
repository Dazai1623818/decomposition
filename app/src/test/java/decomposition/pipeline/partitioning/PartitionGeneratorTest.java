package decomposition.pipeline.partitioning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.core.model.Component;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.pipeline.generation.PartitionGenerator;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class PartitionGeneratorTest {

  @Test
  void enumeratesConnectedComponentsAndPartitions() {
    List<Edge> edges =
        List.of(
            new Edge("a", "b", new Predicate(1, "r1"), 0),
            new Edge("b", "c", new Predicate(2, "r2"), 1),
            new Edge("c", "d", new Predicate(3, "r3"), 2));

    PartitionGenerator generator = new PartitionGenerator(0);
    List<Component> components = generator.enumerateConnectedComponents(edges);

    List<BitSet> componentBits =
        components.stream().map(Component::edgeBits).collect(Collectors.toList());
    assertTrue(
        componentBits.stream().anyMatch(bs -> bs.cardinality() == 3),
        "Should contain full component covering all edges");

    List<Partition> partitions = generator.enumeratePartitions(edges, components);
    assertEquals(4, partitions.size(), "Expected four connected partitions for a simple path");
  }
}
