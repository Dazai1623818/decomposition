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
    Work work = Work.from(query);
    List<Partition> partitions = enumeratePartitions(work);
    if (partitions.isEmpty()) {
      return List.of();
    }

    ComponentExpressionBuilder builder = new ComponentExpressionBuilder(work.edges());
    BitSet fullMask = work.fullEdgeMask();

    List<List<CPQ>> decompositions = new ArrayList<>();
    addWholeQueryCandidates(work, builder, fullMask, decompositions);

    for (Partition partition : partitions) {
      List<List<CPQExpression>> perComponent = new ArrayList<>();
      boolean valid = true;
      for (Component component : partition.components()) {
        Set<String> joinNodes = toJoinNodes(component.joinVars(), work.varIdToName());
        List<CPQExpression> rules =
            builder.build(component.edgeBits(), joinNodes, work.originalVarMap(), 0, false);
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
            new BitSet(work.edgeCount()),
            fullMask,
            decompositions);
      }
    }

    return List.copyOf(decompositions);
  }

  private static void addWholeQueryCandidates(
      Work work,
      ComponentExpressionBuilder builder,
      BitSet fullMask,
      List<List<CPQ>> decompositions) {
    BitSet fullJoinVars = work.joinVars(fullMask, work.fullVarBits());
    Set<String> joinNodes = toJoinNodes(fullJoinVars, work.varIdToName());
    List<CPQExpression> rules = builder.build(fullMask, joinNodes, work.originalVarMap(), 0, false);
    List<CPQExpression> deduped = ComponentExpressionBuilder.dedupeExpressions(rules);
    for (CPQExpression expression : deduped) {
      decompositions.add(List.of(expression.cpq()));
    }
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

  private static List<Partition> enumeratePartitions(Work work) {
    ComponentPool pool = enumerateComponents(work);
    return enumeratePartitions(work.edgeCount(), pool);
  }

  private static ComponentPool enumerateComponents(Work work) {
    List<Component> components = new ArrayList<>();
    List<List<Component>> componentsByEdge = new ArrayList<>(work.edgeCount());
    for (int i = 0; i < work.edgeCount(); i++) {
      componentsByEdge.add(new ArrayList<>());
    }

    for (int seed = 0; seed < work.edgeCount(); seed++) {
      BitSet edges = new BitSet(work.edgeCount());
      edges.set(seed);
      BitSet vars = work.edgeVarsBitSet(seed);
      BitSet frontier = adjacentEdges(work, vars, seed);
      dfsGrow(work, seed, edges, vars, frontier, components, componentsByEdge);
    }

    return new ComponentPool(components, componentsByEdge);
  }

  private static void dfsGrow(
      Work work,
      int seed,
      BitSet edgeBits,
      BitSet varBits,
      BitSet frontier,
      List<Component> out,
      List<List<Component>> componentsByEdge) {
    BitSet join = work.joinVars(edgeBits, varBits);
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
      newVars.or(work.edgeVarsBitSet(next));

      BitSet newFrontier = (BitSet) frontier.clone();
      newFrontier.or(adjacentEdges(work, work.edgeVarsBitSet(next), seed));
      newFrontier.andNot(newEdges);
      newFrontier.clear(0, seed + 1); // enforce min edge id == seed

      dfsGrow(work, seed, newEdges, newVars, newFrontier, out, componentsByEdge);
    }
  }

  private static BitSet adjacentEdges(Work work, BitSet varBits, int seed) {
    BitSet frontier = new BitSet(work.edgeCount());
    for (int var = varBits.nextSetBit(0); var >= 0; var = varBits.nextSetBit(var + 1)) {
      frontier.or(work.incidentEdges(var));
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
  static final class Work {
    private final UniqueGraph<VarCQ, AtomCQ> graph;
    private final List<GraphEdge<VarCQ, AtomCQ>> graphEdges;
    private final int[][] edgeVars;
    private final BitSet[] incidentEdges;
    private final BitSet freeVars;
    private final List<Edge> edges;
    private final Map<Integer, String> varIdToName;
    private final Map<String, String> originalVarMap;

    private Work(
        UniqueGraph<VarCQ, AtomCQ> graph,
        List<GraphEdge<VarCQ, AtomCQ>> graphEdges,
        int[][] edgeVars,
        BitSet[] incidentEdges,
        BitSet freeVars,
        List<Edge> edges,
        Map<Integer, String> varIdToName,
        Map<String, String> originalVarMap) {
      this.graph = graph;
      this.graphEdges = graphEdges;
      this.edgeVars = edgeVars;
      this.incidentEdges = incidentEdges;
      this.freeVars = freeVars;
      this.edges = edges;
      this.varIdToName = varIdToName;
      this.originalVarMap = originalVarMap;
    }

    static Work from(ConjunctiveQuery query) {
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

      return new Work(
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

    List<Edge> edges() {
      return edges;
    }

    Map<Integer, String> varIdToName() {
      return varIdToName;
    }

    Map<String, String> originalVarMap() {
      return originalVarMap;
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

    BitSet fullVarBits() {
      BitSet vars = new BitSet(graph.getNodeCount());
      for (int edgeId = 0; edgeId < edgeCount(); edgeId++) {
        vars.or(edgeVarsBitSet(edgeId));
      }
      return vars;
    }
  }
}
