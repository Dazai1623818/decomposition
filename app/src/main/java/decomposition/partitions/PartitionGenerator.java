package decomposition.partitions;

import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.BitsetUtils;
import decomposition.util.GraphUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Generates: 1) All non-empty, connected, edge-induced subgraphs ("components") of an input CQ edge
 * list; and 2) All edge-disjoint partitions of the entire CQ into such components.
 *
 * <p>Notes on terminology: - "Component" here means *any* connected subset of edges (not only
 * maximal). - "Partition" is a list of components whose edge sets are disjoint and whose union
 * covers all edges of the CQ.
 *
 * <p>Runtime: - enumerateConnectedComponents explores the lattice of connected edge-sets
 * (exponential in the worst case, pruned by connectivity). - enumeratePartitions is a backtracking
 * set-partition over those components (also exponential in the worst case).
 */
public final class PartitionGenerator {
  /** If > 0, stop after generating this many partitions; 0/negative means "no limit". */
  private final int maxPartitions;

  public PartitionGenerator(int maxPartitions) {
    this.maxPartitions = maxPartitions;
  }

  /**
   * Enumerate all non-empty connected edge-induced subgraphs of the CQ.
   *
   * <p>Strategy: - Seed with each single edge (BitSet with one bit set). - Recursively "expand" by
   * adding any edge touching the current vertex frontier (so we remain connected). - Use a global
   * "seen" set (by BitSet signature) to avoid duplicates that would arise from different seeds /
   * orders.
   */
  public List<Component> enumerateConnectedComponents(List<Edge> edges) {
    Objects.requireNonNull(edges, "edges");
    int edgeCount = edges.size();
    List<Component> components = new ArrayList<>();
    Set<String> seen = new HashSet<>(); // canonical strings of visited BitSets

    // Start a DFS expansion from every single edge;
    // the global 'seen' ensures each connected edge-set is visited once.
    for (int edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
      BitSet seed = new BitSet(edgeCount);
      seed.set(edgeIndex);
      expand(seed, edges, components, seen);
    }

    return components;
  }

  /**
   * Enumerate all partitions of the full CQ edge set into edge-disjoint components (i.e., a cover
   * by components with no overlap).
   *
   * @param edges original edge list
   * @param components precomputed connected components (should include single-edge comps)
   */
  public List<Partition> enumeratePartitions(List<Edge> edges, List<Component> components) {
    return enumeratePartitions(edges, components, null, -1);
  }

  /**
   * Enumerates partitions while enforcing optional join-node constraints per component.
   *
   * @param edges original edge list
   * @param components precomputed connected components
   * @param freeVariables set of free variables to treat as implicit join nodes (may be null)
   * @param maxJoinNodes maximum allowed join nodes per component; &lt;= 0 disables the check
   */
  public List<Partition> enumeratePartitions(
      List<Edge> edges, List<Component> components, Set<String> freeVariables, int maxJoinNodes) {
    Objects.requireNonNull(edges, "edges");
    Objects.requireNonNull(components, "components");

    int edgeCount = edges.size();
    BitSet allEdges = BitsetUtils.allOnes(edgeCount); // target: all edges covered
    Map<Integer, List<Component>> componentsByEdge = indexComponentsByEdge(edgeCount, components);
    Set<String> normalizedFreeVars =
        (freeVariables == null || freeVariables.isEmpty()) ? Set.of() : Set.copyOf(freeVariables);

    List<Partition> partitions = new ArrayList<>();
    // Backtrack by always choosing the smallest-index uncovered edge
    // and trying all components that cover it and don't overlap used edges.
    backtrack(
        allEdges,
        new BitSet(edgeCount),
        new ArrayList<>(),
        partitions,
        componentsByEdge,
        normalizedFreeVars,
        maxJoinNodes,
        new HashMap<>());
    return partitions;
  }

  /**
   * Depth-first enumeration of connected edge-sets.
   *
   * @param current current chosen edge-set (connected by construction)
   * @param edges full edge list
   * @param components output sink for Component objects
   * @param seen global "visited" cache of edge-set signatures
   */
  private void expand(
      BitSet current, List<Edge> edges, List<Component> components, Set<String> seen) {
    String signature = BitsetUtils.signature(current, edges.size());
    if (!seen.add(signature)) {
      // Already processed this edge-set from another seed/order.
      return;
    }

    // IMPORTANT: clone before storing; BitSet is mutable
    BitSet snapshot = (BitSet) current.clone();
    components.add(GraphUtils.buildComponent(snapshot, edges));

    // Candidate edges that keep the set connected if added
    Set<Integer> expandable = expandableEdges(current, edges);
    for (Integer candidate : expandable) {
      current.set(candidate);
      expand(current, edges, components, seen);
      current.clear(candidate); // backtrack
    }
  }

  /**
   * Compute the set of edges that can be added while preserving connectivity: any edge with at
   * least one endpoint in the current vertex frontier.
   */
  private Set<Integer> expandableEdges(BitSet current, List<Edge> edges) {
    Set<Integer> result = new HashSet<>();
    Set<String> frontier = GraphUtils.vertices(current, edges); // all vertices touched by 'current'

    for (int idx = 0; idx < edges.size(); idx++) {
      if (current.get(idx)) {
        continue; // already in the set
      }
      Edge edge = edges.get(idx);
      // If the edge touches the current frontier, adding it maintains connectivity
      if (frontier.contains(edge.source()) || frontier.contains(edge.target())) {
        result.add(idx);
      }
    }
    return result;
  }

  /**
   * Build an inverted index: for each edge index i, list all components that contain i. Used to
   * quickly find candidate components that can cover the next uncovered edge.
   */
  private Map<Integer, List<Component>> indexComponentsByEdge(
      int edgeCount, List<Component> components) {
    Map<Integer, List<Component>> map = new HashMap<>();
    for (int i = 0; i < edgeCount; i++) {
      map.put(i, new ArrayList<>());
    }
    for (Component component : components) {
      BitSet bits = component.edgeBits();
      for (int edgeIndex = bits.nextSetBit(0);
          edgeIndex >= 0;
          edgeIndex = bits.nextSetBit(edgeIndex + 1)) {
        map.get(edgeIndex).add(component);
      }
    }
    return map;
  }

  /**
   * Backtracking search to build all edge-disjoint covers: - 'used' marks currently covered edges,
   * - choose the smallest-index uncovered edge, - try each component that includes it and doesn't
   * overlap 'used', - stop when we've covered 'allEdges'.
   */
  private void backtrack(
      BitSet allEdges,
      BitSet used,
      List<Component> chosen,
      List<Partition> output,
      Map<Integer, List<Component>> componentsByEdge,
      Set<String> freeVariables,
      int maxJoinNodes,
      Map<String, Integer> multiplicity) {
    // Success: all edges are covered exactly once
    if (used.equals(allEdges)) {
      output.add(new Partition(chosen));
      return;
    }

    // Optional cap to avoid combinatorial blow-up
    if (maxPartitions > 0 && output.size() >= maxPartitions) {
      return;
    }

    // Pick the smallest-index edge that is still uncovered
    int nextEdge = used.nextClearBit(0);

    // If no component covers this edge, we dead-end (implicit failure branch)
    List<Component> candidates = componentsByEdge.getOrDefault(nextEdge, List.of());
    for (Component candidate : candidates) {
      BitSet bits = candidate.edgeBits();

      // Skip if this component conflicts with already used edges
      BitSet overlap = (BitSet) bits.clone();
      overlap.and(used);
      if (!overlap.isEmpty()) {
        continue;
      }

      if (violatesJoinLimit(candidate, freeVariables, maxJoinNodes, multiplicity)) {
        continue;
      }

      // Extend the cover
      BitSet nextUsed = (BitSet) used.clone();
      nextUsed.or(bits);

      List<Component> nextChosen = new ArrayList<>(chosen);
      nextChosen.add(candidate);
      incrementMultiplicity(candidate, multiplicity);

      backtrack(
          allEdges,
          nextUsed,
          nextChosen,
          output,
          componentsByEdge,
          freeVariables,
          maxJoinNodes,
          multiplicity);

      decrementMultiplicity(candidate, multiplicity);
    }
  }

  private boolean violatesJoinLimit(
      Component component,
      Set<String> freeVariables,
      int maxJoinNodes,
      Map<String, Integer> multiplicity) {
    if (maxJoinNodes <= 0) {
      return false;
    }
    long joinNodes =
        component.vertices().stream()
            .filter(
                v -> {
                  int nextCount = multiplicity.getOrDefault(v, 0) + 1;
                  return nextCount >= 2 || freeVariables.contains(v);
                })
            .count();
    return joinNodes > maxJoinNodes;
  }

  private void incrementMultiplicity(Component component, Map<String, Integer> multiplicity) {
    for (String vertex : component.vertices()) {
      multiplicity.merge(vertex, 1, Integer::sum);
    }
  }

  private void decrementMultiplicity(Component component, Map<String, Integer> multiplicity) {
    for (String vertex : component.vertices()) {
      multiplicity.compute(
          vertex,
          (key, count) -> {
            if (count == null || count <= 1) {
              return null;
            }
            return count - 1;
          });
    }
  }
}
