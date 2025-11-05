package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.Example;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.util.JoinNodeUtils;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class PartitionTwelveRegressionTest {

  @Test
  void fullComponentRetainsLoopAnchoredAtFreeVariable() {
    ExtractionResult extraction = new CQExtractor().extract(Example.example6(), Set.of("A"));
    List<Edge> edges = extraction.edges();
    ComponentCPQBuilder builder = new ComponentCPQBuilder(edges);

    BitSet fullBits = new BitSet(edges.size());
    fullBits.set(0, edges.size());

    Set<String> vertices = new HashSet<>();
    for (Edge edge : edges) {
      vertices.add(edge.source());
      vertices.add(edge.target());
    }
    Component component = new Component(fullBits, vertices);
    Set<String> joinNodes =
        JoinNodeUtils.computeJoinNodes(List.of(component), extraction.freeVariables());

    List<KnownComponent> rules = builder.constructionRules(fullBits, joinNodes);

    assertFalse(
        rules.isEmpty(), "Expected at least one CPQ construction rule for the full component");
    assertTrue(
        rules.stream()
            .anyMatch(
                kc ->
                    "A".equals(kc.source())
                        && "A".equals(kc.target())
                        && kc.cpq().toString().contains("∩ id")),
        () ->
            "Expected a loop anchored at join node A but saw: "
                + rules.stream()
                    .map(kc -> kc.cpq().toString() + " [" + kc.source() + "→" + kc.target() + "]")
                    .toList());
  }
}
