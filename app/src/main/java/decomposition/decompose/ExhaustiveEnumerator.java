package decomposition.decompose;

import decomposition.core.model.Edge;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphNode;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Exhaustive enumeration strategy for decomposing a CQ into CPQ expressions. This class handles
 * both the partition enumeration and the final CPQ conversion.
 */
final class ExhaustiveEnumerator {

  private ExhaustiveEnumerator() {}

  /** Decompose a CQ into all valid CPQ decompositions. */
  static List<List<CPQ>> decompose(ConjunctiveQuery query) {
    QueryPrecompute precompute = QueryPrecompute.from(query);
    List<Partition> partitions = enumeratePartitions(precompute);
    if (partitions.isEmpty()) {
      return List.of();
    }

    ComponentExpressionBuilder builder = new ComponentExpressionBuilder(precompute.edges());
    BitSet fullMask = precompute.fullEdgeMask();

    List<List<CPQ>> decompositions = new ArrayList<>();

    for (Partition partition : partitions) {
      List<List<CPQExpression>> perComponent = new ArrayList<>();
      boolean valid = true;
      for (Component component : partition.components()) {
        Set<String> joinNodes = toJoinNodes(component.joinVars(), precompute.varIdToName());
        List<CPQExpression> rules =
            builder.build(component.edgeBits(), joinNodes, precompute.originalVarMap(), 0, false);
        List<CPQExpression> deduped = ComponentExpressionBuilder.dedupeExpressions(rules);
        if (deduped.isEmpty()) {
          valid = false;
          break;
        }
        perComponent.add(deduped);
      }
      if (valid) {
        expandComponentChoices(
            perComponent,
            0,
            new ArrayList<>(),
            new BitSet(precompute.edgeCount()),
            fullMask,
            decompositions);
      }
    }

    return List.copyOf(decompositions);
  }

  private static void expandComponentChoices(
      List<List<CPQExpression>> components,
      int index,
      List<CPQ> current,
      BitSet covered,
      BitSet fullMask,
      List<List<CPQ>> out) {

    if (index == components.size()) {
      if (covered.equals(fullMask)) {
        out.add(List.copyOf(current));
      }
      return;
    }

    for (CPQExpression expression : components.get(index)) {
      BitSet nextCovered = (BitSet) covered.clone();
      nextCovered.or(expression.edges());
      current.add(expression.cpq());
      expandComponentChoices(components, index + 1, current, nextCovered, fullMask, out);
      current.remove(current.size() - 1);
    }
  }

  private static Set<String> toJoinNodes(BitSet joinVars, Map<Integer, String> varIdToName) {
    Set<String> names = new LinkedHashSet<>();
    for (int var = joinVars.nextSetBit(0); var >= 0; var = joinVars.nextSetBit(var + 1)) {
      String name = varIdToName.get(var);
      if (name != null) {
        names.add(name);
      }
    }
    return names;
  }

  // ----- Partition enumeration -----

  private static List<Partition> enumeratePartitions(QueryPrecompute precompute) {
    ComponentPool pool = enumerateComponents(precompute);
    return enumeratePartitions(precompute.edgeCount(), pool);
  }

  private static ComponentPool enumerateComponents(QueryPrecompute precompute) {
    List<Component> components = new ArrayList<>();
    List<List<Component>> componentsByEdge = new ArrayList<>(precompute.edgeCount());
    for (int i = 0; i < precompute.edgeCount(); i++) {
      componentsByEdge.add(new ArrayList<>());
    }

    for (int seed = 0; seed < precompute.edgeCount(); seed++) {
      BitSet edges = new BitSet(precompute.edgeCount());
      edges.set(seed);
      BitSet vars = precompute.edgeVarsBitSet(seed);
      BitSet frontier = adjacentEdges(precompute, vars, seed);
      dfsGrow(precompute, seed, edges, vars, frontier, components, componentsByEdge);
    }

    return new ComponentPool(components, componentsByEdge);
  }

  private static void dfsGrow(
      QueryPrecompute precompute,
      int seed,
      BitSet edgeBits,
      BitSet varBits,
      BitSet frontier,
      List<Component> out,
      List<List<Component>> componentsByEdge) {
    BitSet join = precompute.joinVars(edgeBits, varBits);

    // Only add components with ≤2 join vars (valid for CPQ decomposition)
    if (join.cardinality() <= 2) {
      Component component =
          new Component((BitSet) edgeBits.clone(), (BitSet) varBits.clone(), (BitSet) join.clone());
      out.add(component);
      for (int edgeIdx = component.edgeBits.nextSetBit(0);
          edgeIdx >= 0;
          edgeIdx = component.edgeBits.nextSetBit(edgeIdx + 1)) {
        componentsByEdge.get(edgeIdx).add(component);
      }
    }
    // Continue DFS even if current component has >2 join vars - adding more edges
    // may reduce join size

    for (int next = frontier.nextSetBit(0); next >= 0; next = frontier.nextSetBit(next + 1)) {
      BitSet newEdges = (BitSet) edgeBits.clone();
      newEdges.set(next);

      BitSet newVars = (BitSet) varBits.clone();
      newVars.or(precompute.edgeVarsBitSet(next));

      BitSet newFrontier = (BitSet) frontier.clone();
      newFrontier.or(adjacentEdges(precompute, precompute.edgeVarsBitSet(next), seed));
      newFrontier.andNot(newEdges);
      newFrontier.clear(0, seed + 1); // enforce min edge id == seed

      dfsGrow(precompute, seed, newEdges, newVars, newFrontier, out, componentsByEdge);
    }
  }

  private static BitSet adjacentEdges(QueryPrecompute precompute, BitSet varBits, int seed) {
    BitSet frontier = new BitSet(precompute.edgeCount());
    for (int var = varBits.nextSetBit(0); var >= 0; var = varBits.nextSetBit(var + 1)) {
      frontier.or(precompute.incidentEdges(var));
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

  // ----- Data structures -----

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

  private record ComponentPool(
      List<Component> components, List<List<Component>> componentsByEdge) {}

  /** Precomputed data for exhaustive enumeration. */
  private record QueryPrecompute(
      UniqueGraph<VarCQ, AtomCQ> graph,
      List<GraphEdge<VarCQ, AtomCQ>> graphEdges,
      int[][] edgeVars,
      BitSet[] incidentEdges,
      BitSet freeVars,
      List<Edge> edges,
      Map<Integer, String> varIdToName,
      Map<String, String> originalVarMap) {

    static QueryPrecompute from(ConjunctiveQuery query) {
      UniqueGraph<VarCQ, AtomCQ> graph = query.graph();
      List<GraphEdge<VarCQ, AtomCQ>> graphEdges = new ArrayList<>(graph.getEdges());
      int edgeCount = graphEdges.size();
      int varCount = graph.getNodeCount();

      int[][] edgeVars = new int[edgeCount][2];
      BitSet[] incidentEdges = new BitSet[varCount];
      for (int i = 0; i < varCount; i++) {
        incidentEdges[i] = new BitSet(edgeCount);
      }

      List<Edge> edgeList = new ArrayList<>(edgeCount);
      long syntheticId = 0L;
      for (int edgeId = 0; edgeId < edgeCount; edgeId++) {
        GraphEdge<VarCQ, AtomCQ> edge = graphEdges.get(edgeId);
        int srcId = edge.getSourceNode().getID();
        int dstId = edge.getTargetNode().getID();
        edgeVars[edgeId][0] = srcId;
        edgeVars[edgeId][1] = dstId;
        incidentEdges[srcId].set(edgeId);
        incidentEdges[dstId].set(edgeId);

        AtomCQ atom = edge.getData();
        String source = atom.getSource().getName();
        String target = atom.getTarget().getName();
        edgeList.add(new Edge(source, target, atom.getLabel(), syntheticId++));
      }

      BitSet freeVars = new BitSet(varCount);
      Map<Integer, String> varIdToName = new LinkedHashMap<>();
      Map<String, String> originalVarMap = new LinkedHashMap<>();
      for (GraphNode<VarCQ, AtomCQ> node : graph.getNodes()) {
        int id = node.getID();
        String name = node.getData().getName();
        varIdToName.put(id, name);
        originalVarMap.put(name, name);
      }
      for (VarCQ free : query.gmarkCQ().getFreeVariables()) {
        GraphNode<VarCQ, AtomCQ> node = graph.getNode(free);
        if (node != null) {
          freeVars.set(node.getID());
        }
      }

      return new QueryPrecompute(
          graph,
          graphEdges,
          edgeVars,
          incidentEdges,
          freeVars,
          List.copyOf(edgeList),
          Map.copyOf(varIdToName),
          Map.copyOf(originalVarMap));
    }

    int edgeCount() {
      return graphEdges.size();
    }

    BitSet edgeVarsBitSet(int edgeId) {
      BitSet bits = new BitSet(graph.getNodeCount());
      bits.set(edgeVars[edgeId][0]);
      bits.set(edgeVars[edgeId][1]);
      return bits;
    }

    BitSet incidentEdges(int varId) {
      return incidentEdges[varId];
    }

    BitSet joinVars(BitSet edgeBits, BitSet varBits) {
      BitSet join = (BitSet) varBits.clone();
      join.and(freeVars);

      for (int var = varBits.nextSetBit(0); var >= 0; var = varBits.nextSetBit(var + 1)) {
        BitSet outside = (BitSet) incidentEdges[var].clone();
        outside.andNot(edgeBits);
        if (!outside.isEmpty()) {
          join.set(var);
        }
      }
      return join;
    }

    BitSet fullEdgeMask() {
      BitSet fullMask = new BitSet(edgeCount());
      fullMask.set(0, edgeCount());
      return fullMask;
    }
  }
}
