package decomposition.util;

import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Utilities for reasoning about join nodes within components and CPQ construction rules. */
public final class JoinNodeUtils {

  private JoinNodeUtils() {}

  /**
   * Computes the set of join nodes given a collection of components and optional free variables. A
   * vertex qualifies as a join node if it either appears in at least two components or is a free
   * variable.
   */
  public static Set<String> computeJoinNodes(
      Collection<Component> components, Set<String> freeVariables) {
    Objects.requireNonNull(components, "components");
    Map<String, Integer> counts = new HashMap<>();
    for (Component component : components) {
      for (String vertex : component.vertices()) {
        counts.merge(vertex, 1, Integer::sum);
      }
    }
    return computeJoinNodesFromCounts(counts, freeVariables);
  }

  /**
   * Computes the set of join nodes from precomputed component variable sets and optional free
   * variables. A vertex qualifies as a join node if it either appears in at least two components or
   * is a free variable.
   */
  public static Set<String> computeJoinNodesFromVariables(
      Collection<? extends Set<String>> componentVariables, Set<String> freeVariables) {
    Objects.requireNonNull(componentVariables, "componentVariables");
    Map<String, Integer> counts = new HashMap<>();
    for (Set<String> vars : componentVariables) {
      for (String var : vars) {
        counts.merge(var, 1, Integer::sum);
      }
    }
    return computeJoinNodesFromCounts(counts, freeVariables);
  }

  /**
   * Computes the set of join nodes from a vertex multiplicity map and optional free variables. A
   * vertex qualifies as a join node if it either appears at least twice or is a free variable.
   *
   * @param vertexMultiplicity map from vertex to its occurrence count across components
   * @param freeVariables optional set of free variables (always treated as join nodes)
   * @return immutable set of join nodes
   */
  public static Set<String> computeJoinNodesFromMultiplicity(
      Map<String, Integer> vertexMultiplicity, Set<String> freeVariables) {
    Objects.requireNonNull(vertexMultiplicity, "vertexMultiplicity");
    Set<String> joinNodes = new HashSet<>();
    if (freeVariables != null && !freeVariables.isEmpty()) {
      joinNodes.addAll(freeVariables);
    }
    for (Map.Entry<String, Integer> entry : vertexMultiplicity.entrySet()) {
      if (entry.getValue() >= 2) {
        joinNodes.add(entry.getKey());
      }
    }
    return joinNodes.isEmpty() ? Set.of() : Collections.unmodifiableSet(joinNodes);
  }

  /**
   * Computes vertex multiplicity for a partition: how many components each vertex appears in.
   *
   * @param partition the partition to analyze
   * @return map from vertex name to occurrence count
   */
  public static Map<String, Integer> computeVertexMultiplicity(Partition partition) {
    Objects.requireNonNull(partition, "partition");
    Map<String, Integer> counts = new HashMap<>();
    for (Component component : partition.components()) {
      for (String vertex : component.vertices()) {
        counts.merge(vertex, 1, Integer::sum);
      }
    }
    return counts;
  }

  private static Set<String> computeJoinNodesFromCounts(
      Map<String, Integer> counts, Set<String> freeVariables) {
    return computeJoinNodesFromMultiplicity(counts, freeVariables);
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
    for (int idx = edgeBits.nextSetBit(0); idx >= 0; idx = edgeBits.nextSetBit(idx + 1)) {
      Edge edge = edges.get(idx);
      if (joinNodes.contains(edge.source())) {
        present.add(edge.source());
      }
      if (joinNodes.contains(edge.target())) {
        present.add(edge.target());
      }
    }
    return present.isEmpty() ? Set.of() : Collections.unmodifiableSet(present);
  }

  /**
   * Checks whether the endpoints of the provided CPQ construction rule respect the directional
   * roles inferred for the local join nodes of its component.
   *
   * @param rule CPQ construction rule to validate
   * @param localJoinNodes join nodes present inside the component
   * @return {@code true} if endpoints comply with join node expectations
   */
  public static boolean endpointsRespectJoinNodeRoles(
      KnownComponent rule, decomposition.model.Component component, Set<String> localJoinNodes) {
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
