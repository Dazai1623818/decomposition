package decomposition.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.cpq.CPQExpression;
import decomposition.model.Component;
import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class JoinNodeUtilsTest {

  private static final Predicate P1 = new Predicate(1, "r1");
  private static final Predicate P2 = new Predicate(2, "r2");
  private static final Predicate P3 = new Predicate(3, "r3");

  @Test
  void computeJoinNodesWithNoOverlap() {
    Component c1 = makeComponent(Set.of("a", "b"));
    Component c2 = makeComponent(Set.of("c", "d"));
    Set<String> joinNodes = JoinNodeUtils.computeJoinNodes(List.of(c1, c2), Set.of());
    assertTrue(joinNodes.isEmpty(), "No join nodes when components are disjoint");
  }

  @Test
  void computeJoinNodesWithSingleOverlap() {
    Component c1 = makeComponent(Set.of("a", "b"));
    Component c2 = makeComponent(Set.of("b", "c"));
    Set<String> joinNodes = JoinNodeUtils.computeJoinNodes(List.of(c1, c2), Set.of());
    assertEquals(Set.of("b"), joinNodes, "Vertex 'b' appears in both components");
  }

  @Test
  void computeJoinNodesWithMultipleOverlaps() {
    Component c1 = makeComponent(Set.of("a", "b", "c"));
    Component c2 = makeComponent(Set.of("b", "c", "d"));
    Set<String> joinNodes = JoinNodeUtils.computeJoinNodes(List.of(c1, c2), Set.of());
    assertEquals(Set.of("b", "c"), joinNodes, "Both b and c are join nodes");
  }

  @Test
  void computeJoinNodesIncludesFreeVariables() {
    Component c1 = makeComponent(Set.of("a", "b"));
    Set<String> joinNodes = JoinNodeUtils.computeJoinNodes(List.of(c1), Set.of("a", "x"));
    assertEquals(Set.of("a", "x"), joinNodes, "Free variables are always join nodes");
  }

  @Test
  void computeJoinNodesFromVariablesMatchesFromComponents() {
    Component c1 = makeComponent(Set.of("a", "b"));
    Component c2 = makeComponent(Set.of("b", "c"));
    List<Component> components = List.of(c1, c2);
    Set<String> freeVars = Set.of("a");

    Set<String> fromComponents = JoinNodeUtils.computeJoinNodes(components, freeVars);
    Set<String> fromVariables =
        JoinNodeUtils.computeJoinNodesFromVariables(
            List.of(c1.vertices(), c2.vertices()), freeVars);

    assertEquals(fromComponents, fromVariables, "Both methods should produce identical results");
  }

  @Test
  void localJoinNodesWithEmptyJoinNodes() {
    Component c = makeComponent(Set.of("a", "b", "c"));
    Set<String> local = JoinNodeUtils.localJoinNodes(c, Set.of());
    assertTrue(local.isEmpty(), "No local join nodes when global set is empty");
  }

  @Test
  void localJoinNodesWithNoIntersection() {
    Component c = makeComponent(Set.of("a", "b"));
    Set<String> local = JoinNodeUtils.localJoinNodes(c, Set.of("x", "y"));
    assertTrue(local.isEmpty(), "No local join nodes when none overlap with component");
  }

  @Test
  void localJoinNodesWithPartialIntersection() {
    Component c = makeComponent(Set.of("a", "b", "c"));
    Set<String> local = JoinNodeUtils.localJoinNodes(c, Set.of("b", "c", "d"));
    assertEquals(Set.of("b", "c"), local, "Only b and c are both in component and join nodes");
  }

  @Test
  void endpointsRespectJoinNodeRolesWithNoJoinNodes() {
    Component c = makeComponent(Set.of("a", "b"));
    CPQExpression kc = makeCPQExpression("a", "b");
    assertTrue(
        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, c, Set.of()),
        "Always respects when no join nodes");
  }

  @Test
  void endpointsRespectJoinNodeRolesWithSingleJoinNodeInSingleEdge() {
    List<Edge> edges = List.of(new Edge("a", "b", P1, 0));
    BitSet bits = new BitSet(1);
    bits.set(0);
    Component c = new Component(bits, Set.of("a", "b"));
    CPQExpression kc = makeCPQExpression("a", "b");

    assertTrue(
        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, c, Set.of("a")),
        "Single join node 'a' matches source");
    assertTrue(
        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, c, Set.of("b")),
        "Single join node 'b' matches target");
    assertFalse(
        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, c, Set.of("x")),
        "Single join node 'x' doesn't match either endpoint");
  }

  @Test
  void endpointsRespectJoinNodeRolesWithSingleJoinNodeLoop() {
    List<Edge> edges = List.of(new Edge("a", "b", P1, 0), new Edge("b", "c", P2, 1));
    BitSet bits = new BitSet(2);
    bits.set(0);
    bits.set(1);
    Component c = new Component(bits, Set.of("a", "b", "c"));
    CPQExpression loop = makeCPQExpression("b", "b");

    assertTrue(
        JoinNodeUtils.endpointsRespectJoinNodeRoles(loop, c, Set.of("b")),
        "Single join node 'b' matches loop endpoint");
  }

  @Test
  void endpointsRespectJoinNodeRolesWithTwoJoinNodes() {
    List<Edge> edges = List.of(new Edge("a", "b", P1, 0), new Edge("b", "c", P2, 1));
    BitSet bits = new BitSet(2);
    bits.set(0);
    bits.set(1);
    Component c = new Component(bits, Set.of("a", "b", "c"));
    CPQExpression kc = makeCPQExpression("a", "c");

    assertTrue(
        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, c, Set.of("a", "c")),
        "Both join nodes are endpoints");
    assertFalse(
        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, c, Set.of("a", "b")),
        "Join node 'b' is not an endpoint");
  }

  // Helper methods

  private Component makeComponent(Set<String> vertices) {
    BitSet bits = new BitSet();
    bits.set(0);
    return new Component(bits, vertices);
  }

  private CPQExpression makeCPQExpression(String source, String target) {
    CPQ cpq = CPQ.parse("r1");
    BitSet bits = new BitSet();
    bits.set(0);
    Map<String, String> mapping = new LinkedHashMap<>();
    mapping.put(source, source);
    mapping.put(target, target);
    return new CPQExpression(cpq, bits, source, target, "test", mapping);
  }
}
