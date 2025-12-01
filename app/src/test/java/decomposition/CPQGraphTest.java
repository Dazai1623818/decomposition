package decomposition;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import org.junit.jupiter.api.Test;

final class CPQGraphTest {

  @Test
  void testCPQGraphStructure() {
    CPQ cpq = CPQ.parse("(r5 ∩ (r1◦r2))");

    QueryGraphCPQ graph = cpq.toQueryGraph();
    assertNotNull(graph, "CPQ should produce a query graph");
    assertFalse(graph.getVertices().isEmpty(), "Graph should contain vertices");
    assertFalse(graph.getEdges().isEmpty(), "Graph should contain edges");
    assertNotNull(graph.getSourceVertex(), "Graph should expose a source vertex");
    assertNotNull(graph.getTargetVertex(), "Graph should expose a target vertex");
  }
}
