package decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.examples.Example;
import decomposition.examples.RandomExampleConfig;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class RandomExampleTest {

  @Test
  void randomExampleRespectsConfigCounts() {
    RandomExampleConfig config = new RandomExampleConfig(2, 3, 7, 123L);
    CQ cq = Example.random(config);

    assertEquals(
        config.freeVariableCount(), cq.getFreeVariables().size(), "free variable count mismatch");
    assertEquals(config.edgeCount(), cq.toQueryGraph().getEdgeCount(), "edge count mismatch");
  }

  @Test
  void randomExampleDeterministicForSeed() {
    RandomExampleConfig config = new RandomExampleConfig(1, 5, 3, 42L);
    CQ first = Example.random(config);
    CQ second = Example.random(config);
    RandomExampleConfig otherSeed = new RandomExampleConfig(1, 5, 3, 99L);
    CQ different = Example.random(otherSeed);

    assertEquals(
        first.toFormalSyntax(), second.toFormalSyntax(), "generated CQ should repeat with seed");
    assertNotEquals(
        first.toFormalSyntax(),
        different.toFormalSyntax(),
        "different seeds should yield different CQ");
  }

  @Test
  void randomExampleProducesConnectedGraph() {
    RandomExampleConfig config = new RandomExampleConfig(2, 7, 5, null);
    for (int i = 0; i < 10; i++) {
      RandomExampleConfig seeded =
          new RandomExampleConfig(
              config.freeVariableCount(),
              config.edgeCount(),
              config.predicateLabelCount(),
              (long) i);
      assertFreeVariablesConnected(Example.random(seeded));
    }
    assertFreeVariablesConnected(Example.random(config));
  }

  @Test
  void randomExampleRejectsTooFewEdgesForConnectivity() {
    RandomExampleConfig config = new RandomExampleConfig(3, 1, 3, 7L);
    assertThrows(IllegalArgumentException.class, () -> Example.random(config));
  }

  private static void assertFreeVariablesConnected(CQ cq) {
    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
    List<UniqueGraph.GraphNode<VarCQ, AtomCQ>> nodes = graph.getNodes();
    UniqueGraph.GraphNode<VarCQ, AtomCQ> start =
        nodes.stream().filter(node -> node.getData().isFree()).findFirst().orElse(null);
    if (start == null) {
      return;
    }
    Set<UniqueGraph.GraphNode<VarCQ, AtomCQ>> visited = new HashSet<>();
    Deque<UniqueGraph.GraphNode<VarCQ, AtomCQ>> stack = new ArrayDeque<>();
    stack.push(start);
    while (!stack.isEmpty()) {
      UniqueGraph.GraphNode<VarCQ, AtomCQ> node = stack.pop();
      if (!visited.add(node)) {
        continue;
      }
      for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : node.getOutEdges()) {
        stack.push(edge.getTargetNode());
      }
      for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : node.getInEdges()) {
        stack.push(edge.getSourceNode());
      }
    }
    for (UniqueGraph.GraphNode<VarCQ, AtomCQ> node : nodes) {
      if (node.getData().isFree()) {
        assertTrue(visited.contains(node), "free variables must be connected");
      }
    }
  }
}
