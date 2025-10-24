package decomposition;

import dev.roanh.gmark.lang.cpq.CPQ;
import org.junit.jupiter.api.Test;

public class CPQGraphTest {

  @Test
  void testCPQGraphStructure() {
    CPQ cpq = CPQ.parse("(r5 ∩ (r1◦r2))");

    // Test what methods are available
    System.out.println("CPQ: " + cpq);
    System.out.println("CPQ class: " + cpq.getClass().getName());

    // Try the method from user's example
    try {
      Object graph = cpq.getClass().getMethod("toQueryGraphCPQ").invoke(cpq);
      System.out.println("QueryGraph: " + graph);
      System.out.println("QueryGraph class: " + graph.getClass().getName());

      // Get vertices and edges
      Object vertices = graph.getClass().getMethod("getVertices").invoke(graph);
      Object edges = graph.getClass().getMethod("getEdges").invoke(graph);

      System.out.println("Vertices: " + vertices);
      System.out.println("Edges: " + edges);
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
