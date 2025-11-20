package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.core.model.Component;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.examples.Example;
import decomposition.pipeline.extract.CQExtractor;
import decomposition.pipeline.extract.CQExtractor.ExtractionResult;
import decomposition.util.JoinAnalysisBuilder;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class PartitionTwelveRegressionTest {

  @Test
  void fullComponentRetainsLoopAnchoredAtFreeVariable() {
    ExtractionResult extraction = new CQExtractor().extract(Example.example6(), Set.of("A"));
    List<Edge> edges = extraction.edges();
    Map<String, String> varMap = extraction.variableNodeMap();
    ComponentExpressionBuilder resolver = new ComponentExpressionBuilder(edges);

    BitSet fullBits = new BitSet(edges.size());
    fullBits.set(0, edges.size());

    Set<String> vertices = new HashSet<>();
    for (Edge edge : edges) {
      vertices.add(edge.source());
      vertices.add(edge.target());
    }
    Component component = new Component(fullBits, vertices);
    Set<String> joinNodes =
        JoinAnalysisBuilder.analyzePartition(
                new Partition(List.of(component)), extraction.freeVariables())
            .globalJoinNodes();

    List<CPQExpression> expressions = resolver.build(fullBits, joinNodes, varMap);

    assertFalse(
        expressions.isEmpty(), "Expected at least one CPQ expression for the full component");
    assertTrue(
        expressions.stream()
            .anyMatch(
                kc ->
                    "A".equals(kc.source())
                        && "A".equals(kc.target())
                        && kc.cpq().toString().contains("∩ id")),
        () ->
            "Expected a loop anchored at join node A but saw: "
                + expressions.stream()
                    .map(kc -> kc.cpq().toString() + " [" + kc.source() + "→" + kc.target() + "]")
                    .toList());
  }
}
