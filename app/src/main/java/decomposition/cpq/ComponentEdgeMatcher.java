package decomposition.cpq;

import decomposition.core.model.Edge;
import decomposition.util.BitsetUtils;
import decomposition.util.GraphUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.QueryGraphCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Matches a {@link CPQExpression}'s CQ/CPQ structure to concrete decomposition edges. */
final class ComponentEdgeMatcher {
  private final List<Edge> edges;

  ComponentEdgeMatcher(List<Edge> edges) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
  }

  boolean isValid(CPQExpression rule) {
    List<Edge> componentEdges = new ArrayList<>(rule.edges().cardinality());
    BitsetUtils.stream(rule.edges()).forEach(idx -> componentEdges.add(edges.get(idx)));
    if (componentEdges.isEmpty()) return false;

    Set<String> vertices = GraphUtils.vertices(rule.edges(), edges);
    if (!vertices.contains(rule.source()) || !vertices.contains(rule.target())) return false;

    List<GraphEdge<VarCQ, AtomCQ>> cpqEdges;
    QueryGraphCPQ cpqGraph;
    String sourceVar;
    String targetVar;
    try {
      CPQ cpq = rule.cpq();
      CQ cqPattern = cpq.toCQ();
      QueryGraphCQ cqGraph = cqPattern.toQueryGraph();
      UniqueGraph<VarCQ, AtomCQ> graph = cqGraph.toUniqueGraph();
      cpqEdges = graph.getEdges();
      cpqGraph = cpq.toQueryGraph();
      sourceVar = cpqGraph.getVertexLabel(cpqGraph.getSourceVertex());
      targetVar = cpqGraph.getVertexLabel(cpqGraph.getTargetVertex());
      if (cpqEdges.size() != componentEdges.size()) return false;
    } catch (RuntimeException ex) {
      return false;
    }

    boolean ruleIsLoop = rule.source().equals(rule.target());
    if (cpqGraph.isLoop() != ruleIsLoop) return false;

    return matchEdges(cpqEdges, componentEdges, sourceVar, targetVar, rule);
  }

  private boolean matchEdges(
      List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
      List<Edge> componentEdges,
      String sourceVar,
      String targetVar,
      CPQExpression rule) {

    Map<String, String> initialMapping = new HashMap<>();
    Set<String> initialUsedNodes = new HashSet<>();
    initialMapping.put(sourceVar, rule.source());
    initialMapping.put(targetVar, rule.target());
    initialUsedNodes.add(rule.source());
    initialUsedNodes.add(rule.target());

    BitSet initialUsedEdges = new BitSet(componentEdges.size());
    Deque<MatchState> stack = new ArrayDeque<>();
    stack.push(new MatchState(0, initialUsedEdges, initialMapping, initialUsedNodes));

    while (!stack.isEmpty()) {
      MatchState state = stack.pop();
      if (state.index() == cpqEdges.size()) {
        if (state.usedEdges().cardinality() == componentEdges.size()
            && rule.source().equals(state.mapping().get(sourceVar))
            && rule.target().equals(state.mapping().get(targetVar))) {
          return true;
        }
        continue;
      }

      GraphEdge<VarCQ, AtomCQ> cpqEdge = cpqEdges.get(state.index());
      AtomCQ atom = cpqEdge.getData();
      String label = atom.getLabel().getAlias();
      String cpqSrcName = cpqEdge.getSourceNode().getData().getName();
      String cpqTrgName = cpqEdge.getTargetNode().getData().getName();

      for (int edgeIdx = 0; edgeIdx < componentEdges.size(); edgeIdx++) {
        if (state.usedEdges().get(edgeIdx)) continue;

        Edge componentEdge = componentEdges.get(edgeIdx);
        if (!label.equals(componentEdge.label())) continue;

        String mappedSrc = state.mapping().get(cpqSrcName);
        String mappedTrg = state.mapping().get(cpqTrgName);
        if (mappedSrc != null && !mappedSrc.equals(componentEdge.source())) continue;
        if (mappedTrg != null && !mappedTrg.equals(componentEdge.target())) continue;
        if (mappedSrc == null && state.usedNodes().contains(componentEdge.source())) continue;
        if (mappedTrg == null && state.usedNodes().contains(componentEdge.target())) continue;

        Map<String, String> nextMapping = new HashMap<>(state.mapping());
        Set<String> nextUsedNodes = new HashSet<>(state.usedNodes());
        if (mappedSrc == null) {
          nextMapping.put(cpqSrcName, componentEdge.source());
          nextUsedNodes.add(componentEdge.source());
        }
        if (mappedTrg == null) {
          nextMapping.put(cpqTrgName, componentEdge.target());
          nextUsedNodes.add(componentEdge.target());
        }

        BitSet nextUsedEdges = (BitSet) state.usedEdges().clone();
        nextUsedEdges.set(edgeIdx);

        stack.push(new MatchState(state.index() + 1, nextUsedEdges, nextMapping, nextUsedNodes));
      }
    }

    return false;
  }

  private record MatchState(
      int index, BitSet usedEdges, Map<String, String> mapping, Set<String> usedNodes) {}
}
