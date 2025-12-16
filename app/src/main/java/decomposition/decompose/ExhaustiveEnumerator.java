package decomposition.decompose;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Exhaustive enumeration of all valid partitions of a CQ into connected components with at most two
 * join variables each. The output is the list of partitions; conversion to CPQ expressions is
 * handled elsewhere.
 */
final class ExhaustiveEnumerator {

  private ExhaustiveEnumerator() {}

  static List<Partition> enumeratePartitions(ConjunctiveQuery query) {
    Objects.requireNonNull(query, "query");
    ComponentPool pool = enumerateComponents(query);
    return enumeratePartitions(query.edgeCount(), pool);
  }

  static List<Partition> enumeratePartitions(dev.roanh.gmark.lang.cq.CQ cq) {
    return enumeratePartitions(new ConjunctiveQuery(cq));
  }

  private static ComponentPool enumerateComponents(ConjunctiveQuery query) {
    List<Component> components = new ArrayList<>();
    List<List<Component>> componentsByEdge = new ArrayList<>(query.edgeCount());
    for (int i = 0; i < query.edgeCount(); i++) {
      componentsByEdge.add(new ArrayList<>());
    }

    for (int seed = 0; seed < query.edgeCount(); seed++) {
      BitSet edges = new BitSet(query.edgeCount());
      edges.set(seed);
      BitSet vars = query.edgeVarsBitSet(seed);
      BitSet frontier = adjacentEdges(query, vars, seed);
      dfsGrow(query, seed, edges, vars, frontier, components, componentsByEdge);
    }

    return new ComponentPool(components, componentsByEdge);
  }

  private static void dfsGrow(
      ConjunctiveQuery query,
      int seed,
      BitSet edgeBits,
      BitSet varBits,
      BitSet frontier,
      List<Component> out,
      List<List<Component>> componentsByEdge) {
    BitSet join = query.joinVars(edgeBits, varBits);
    if (join.cardinality() > 2) {
      return; // prune: supersets only increase join size
    }

    Component component =
        new Component((BitSet) edgeBits.clone(), (BitSet) varBits.clone(), (BitSet) join.clone());
    out.add(component);
    for (int edgeIdx = component.edgeBits.nextSetBit(0);
        edgeIdx >= 0;
        edgeIdx = component.edgeBits.nextSetBit(edgeIdx + 1)) {
      componentsByEdge.get(edgeIdx).add(component);
    }

    for (int next = frontier.nextSetBit(0); next >= 0; next = frontier.nextSetBit(next + 1)) {
      BitSet newEdges = (BitSet) edgeBits.clone();
      newEdges.set(next);

      BitSet newVars = (BitSet) varBits.clone();
      newVars.or(query.edgeVarsBitSet(next));

      BitSet newFrontier = (BitSet) frontier.clone();
      newFrontier.or(adjacentEdges(query, query.edgeVarsBitSet(next), seed));
      newFrontier.andNot(newEdges);
      newFrontier.clear(0, seed + 1); // enforce min edge id == seed

      dfsGrow(query, seed, newEdges, newVars, newFrontier, out, componentsByEdge);
    }
  }

  private static BitSet adjacentEdges(ConjunctiveQuery query, BitSet varBits, int seed) {
    BitSet frontier = new BitSet(query.edgeCount());
    for (int var = varBits.nextSetBit(0); var >= 0; var = varBits.nextSetBit(var + 1)) {
      frontier.or(query.incidentEdges(var));
    }
    frontier.clear(0, seed + 1);
    return frontier;
  }

  private static List<Partition> enumeratePartitions(int edgeCount, ComponentPool pool) {
    BitSet remaining = new BitSet(edgeCount);
    remaining.set(0, edgeCount);
    List<Partition> partitions = new ArrayList<>();
    backtrack(remaining, new ArrayList<>(), partitions, pool.componentsByEdge);
    return partitions;
  }

  private static void backtrack(
      BitSet remaining,
      List<Component> chosen,
      List<Partition> partitions,
      List<List<Component>> componentsByEdge) {
    int nextEdge = remaining.nextSetBit(0);
    if (nextEdge < 0) {
      partitions.add(new Partition(new ArrayList<>(chosen)));
      return;
    }

    for (Component candidate : componentsByEdge.get(nextEdge)) {
      if (!isSubset(candidate.edgeBits, remaining)) {
        continue;
      }
      BitSet nextRemaining = (BitSet) remaining.clone();
      nextRemaining.andNot(candidate.edgeBits);
      chosen.add(candidate);

      if (hasCoverage(nextRemaining, componentsByEdge)) {
        backtrack(nextRemaining, chosen, partitions, componentsByEdge);
      }

      chosen.remove(chosen.size() - 1);
    }
  }

  private static boolean hasCoverage(BitSet remaining, List<List<Component>> componentsByEdge) {
    for (int edge = remaining.nextSetBit(0); edge >= 0; edge = remaining.nextSetBit(edge + 1)) {
      boolean covered =
          componentsByEdge.get(edge).stream().anyMatch(c -> isSubset(c.edgeBits, remaining));
      if (!covered) {
        return false;
      }
    }
    return true;
  }

  private static boolean isSubset(BitSet subset, BitSet superset) {
    BitSet tmp = (BitSet) subset.clone();
    tmp.andNot(superset);
    return tmp.isEmpty();
  }

  record Component(BitSet edgeBits, BitSet varBits, BitSet joinVars) {
    Component {
      Objects.requireNonNull(edgeBits, "edgeBits");
      Objects.requireNonNull(varBits, "varBits");
      Objects.requireNonNull(joinVars, "joinVars");
    }
  }

  record Partition(List<Component> components) {
    Partition {
      Objects.requireNonNull(components, "components");
    }
  }

  private record ComponentPool(List<Component> components, List<List<Component>> componentsByEdge) {}
}
