package decomposition.util;

import decomposition.core.model.Component;
import decomposition.core.model.Edge;
import decomposition.cpq.CPQExpression;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Utilities for reasoning about join nodes within components and CPQ expressions. */
public final class JoinNodeUtils {

  private JoinNodeUtils() {}

  /**
   * Normalizes the provided node set to an immutable empty set when null/empty, otherwise returns a
   * defensive copy.
   */
  public static Set<String> normalizeNodeSet(Set<String> values) {
    return (values == null || values.isEmpty()) ? Set.of() : Set.copyOf(values);
  }

  public static int originalVariableOrder(String node, Map<String, String> originalVarMap) {
    if (node == null || originalVarMap == null || originalVarMap.isEmpty()) {
      return Integer.MAX_VALUE;
    }
    int idx = 0;
    for (Map.Entry<String, String> entry : originalVarMap.entrySet()) {
      if (Objects.equals(node, entry.getValue())) {
        return idx;
      }
      idx++;
    }
    return Integer.MAX_VALUE;
  }

  /** Computes the subset of join nodes that are present within the given component. */
  public static Set<String> localJoinNodes(Component component, Set<String> joinNodes) {
    Objects.requireNonNull(component, "component");
    if (joinNodes == null || joinNodes.isEmpty()) {
      return Set.of();
    }
    Set<String> local = new HashSet<>();
    for (String vertex : component.vertices()) {
      if (joinNodes.contains(vertex)) {
        local.add(vertex);
      }
    }
    return local.isEmpty() ? Set.of() : Collections.unmodifiableSet(local);
  }

  /**
   * Computes the subset of join nodes that are present within the specified edge subset. This is
   * useful when working with BitSet-based edge representations.
   *
   * @param edgeBits the subset of edges to consider
   * @param edges the full edge list
   * @param joinNodes the global set of join nodes
   * @return immutable set of join nodes present in the edge subset
   */
  public static Set<String> localJoinNodes(
      BitSet edgeBits, List<Edge> edges, Set<String> joinNodes) {
    Objects.requireNonNull(edgeBits, "edgeBits");
    Objects.requireNonNull(edges, "edges");
    if (joinNodes == null || joinNodes.isEmpty()) {
      return Set.of();
    }
    Set<String> present = new HashSet<>();
    for (String vertex : GraphUtils.vertices(edgeBits, edges)) {
      if (joinNodes.contains(vertex)) {
        present.add(vertex);
      }
    }
    return present.isEmpty() ? Set.of() : Collections.unmodifiableSet(present);
  }

  /**
   * Checks whether the endpoints of the provided CPQ expression respect the directional roles
   * inferred for the local join nodes of its component.
   *
   * @param rule CPQ expression to validate
   * @param component component whose edges the rule covers
   * @param localJoinNodes join nodes present inside the component
   * @return {@code true} if endpoints comply with join node expectations
   */
  public static boolean endpointsRespectJoinNodeRoles(
      CPQExpression rule, Component component, Set<String> localJoinNodes) {
    Objects.requireNonNull(rule, "rule");
    return endpointsRespectJoinNodeRoles(rule.source(), rule.target(), component, localJoinNodes);
  }

  /**
   * Checks whether the given endpoints respect the directional roles inferred for the local join
   * nodes of a component.
   *
   * <p>Semantics:
   *
   * <ul>
   *   <li>If the component has a single vertex (CQ self-loop), the CPQ must be a self-loop anchored
   *       at that vertex: {@code source == target == v}.
   *   <li>If there are no local join nodes, any endpoints are allowed (subject to the self-loop
   *       rule).
   *   <li>If there is exactly one local join node {@code j}:
   *       <ul>
   *         <li>For single-edge components, at least one endpoint must equal {@code j}.
   *         <li>For multi-edge components, both endpoints must equal {@code j}.
   *       </ul>
   *   <li>If there are exactly two local join nodes, the set of endpoints must equal the set of
   *       join nodes.
   *   <li>Components with more than two local join nodes are considered invalid (these are filtered
   *       earlier).
   * </ul>
   */
  public static boolean endpointsRespectJoinNodeRoles(
      String source, String target, Component component, Set<String> localJoinNodes) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(localJoinNodes, "localJoinNodes");

    Set<String> vertices = component.vertices();
    if (vertices.size() == 1) {
      String v = vertices.iterator().next();
      return v.equals(source) && v.equals(target);
    }

    if (localJoinNodes.isEmpty()) {
      return true;
    }

    if (localJoinNodes.size() == 1) {
      String join = localJoinNodes.iterator().next();
      if (component.edgeCount() == 1) {
        return join.equals(source) || join.equals(target);
      }
      return join.equals(source) && join.equals(target);
    }

    if (localJoinNodes.size() == 2) {
      Set<String> endpoints = new HashSet<>();
      endpoints.add(source);
      endpoints.add(target);
      return endpoints.size() == localJoinNodes.size() && endpoints.containsAll(localJoinNodes);
    }

    // Components with more than two join nodes are filtered earlier; treat as invalid.
    return false;
  }
}
