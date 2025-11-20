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
   * @param localJoinNodes join nodes present inside the component
   * @return {@code true} if endpoints comply with join node expectations
   */
  public static boolean endpointsRespectJoinNodeRoles(
      CPQExpression rule, Component component, Set<String> localJoinNodes) {
    Objects.requireNonNull(rule, "rule");
    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(localJoinNodes, "localJoinNodes");

    if (localJoinNodes.isEmpty()) {
      return true;
    }

    String source = rule.source();
    String target = rule.target();

    if (localJoinNodes.size() == 1) {
      String join = localJoinNodes.iterator().next();
      if (component.edgeCount() == 1) {
        boolean matchesSource = source.equals(join);
        boolean matchesTarget = target.equals(join);
        return matchesSource || matchesTarget;
      }
      if (!source.equals(join) || !target.equals(join)) {
        return false;
      }
      return true;
    }

    if (localJoinNodes.size() == 2) {
      Set<String> endpoints = new HashSet<>();
      endpoints.add(source);
      endpoints.add(target);
      if (endpoints.size() != localJoinNodes.size() || !endpoints.containsAll(localJoinNodes)) {
        return false;
      }
      return true;
    }

    // Components with more than two join nodes are filtered earlier; treat as invalid.
    return false;
  }
}
