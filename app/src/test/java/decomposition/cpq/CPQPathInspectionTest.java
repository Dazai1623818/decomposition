package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;

final class CPQPathInspectionTest {

  @Test
  void inspectCycleCandidate() {
    CPQ cpq = CPQ.parse("r1⁻◦(((r4⁻◦r3⁻) ∩ r5)◦r2⁻)");
    QueryGraphCPQ graph = cpq.toQueryGraph();

    System.out.println("Vertices:");
    graph
        .getVertices()
        .forEach(v -> System.out.println("  " + v.getID() + " -> " + graph.getVertexLabel(v)));

    System.out.println("Edges:");
    graph
        .getEdges()
        .forEach(
            e ->
                System.out.println(
                    "  "
                        + e.getSource().getID()
                        + " -["
                        + e.getLabel().getAlias()
                        + (e.getLabel().isInverse() ? "⁻" : "")
                        + "]-> "
                        + e.getTarget().getID()));
    System.out.println("Is loop: " + graph.isLoop());

    var cq = cpq.toCQ();
    var cqGraph = cq.toQueryGraph();
    var unique = cqGraph.toUniqueGraph();
    System.out.println("CQ edges:");
    unique
        .getEdges()
        .forEach(
            edge ->
                System.out.println(
                    "  "
                        + edge.getSourceNode().getData().getName()
                        + " -["
                        + edge.getData().getLabel().getAlias()
                        + (edge.getData().getLabel().isInverse() ? "⁻" : "")
                        + "]-> "
                        + edge.getTargetNode().getData().getName()));

    String source = graph.getVertexLabel(graph.getSourceVertex());
    String target = graph.getVertexLabel(graph.getTargetVertex());
    assertEquals("src", source, "Parser should expose correct source variable name");
    assertEquals("trg", target, "Parser should expose correct target variable name");
  }

  @Test
  void inspectBacktrackVariants() {
    String[] expressions = {"r4◦r4⁻", "(r4◦r4⁻) ∩ id", "(r4◦r4⁻◦r4) ∩ id"};
    int[] expectedEdges = {2, 1, 3};
    boolean[] expectedLoops = {false, true, true};
    for (int i = 0; i < expressions.length; i++) {
      String expr = expressions[i];
      CPQ cpq = CPQ.parse(expr);
      QueryGraphCPQ graph = cpq.toQueryGraph();
      System.out.println(expr + " -> edges=" + graph.getEdgeCount() + ", isLoop=" + graph.isLoop());
      graph
          .getVertices()
          .forEach(
              v ->
                  System.out.println(
                      "  vertex " + v.getID() + " label=" + graph.getVertexLabel(v)));
      graph
          .getEdges()
          .forEach(
              e ->
                  System.out.println(
                      "  edge "
                          + e.getSource().getID()
                          + "-["
                          + e.getLabel().getAlias()
                          + (e.getLabel().isInverse() ? "⁻" : "")
                          + "]->"
                          + e.getTarget().getID()));
      assertEquals(expectedEdges[i], graph.getEdgeCount(), "Unexpected edge count for " + expr);
      assertEquals(expectedLoops[i], graph.isLoop(), "Unexpected loop flag for " + expr);
    }
  }
}
