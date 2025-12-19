package decomposition.cpq;

import decomposition.core.Component;
import decomposition.core.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class BacktrackingLoops {

  private BacktrackingLoops() {}

  static List<CPQExpression> findAll(
      List<Edge> globalEdges,
      Component component,
      BitSet edgeBits,
      Set<String> allowedAnchors,
      int diameterCap) {

    Map<String, List<AdjacencyEdge>> adj = buildAdjacency(globalEdges, edgeBits);
    if (adj.isEmpty()) return List.of();

    List<CPQExpression> out = new ArrayList<>();
    for (String anchor : adj.keySet()) {
      if (allowedAnchors != null && !allowedAnchors.isEmpty() && !allowedAnchors.contains(anchor)) {
        continue;
      }

      BitSet visited = new BitSet(edgeBits.length());
      CPQ body = loopForVertex(anchor, adj, visited, diameterCap);
      if (visited.cardinality() != edgeBits.cardinality()) continue;

      CPQ loop = CPQ.intersect(List.of(body, CPQ.id()));
      if (diameterCap > 0 && loop.getDiameter() > diameterCap) continue;

      out.add(
          new CPQExpression(
              loop,
              component,
              anchor,
              anchor,
              "Loop via backtracking anchored at '" + anchor + "'"));
    }
    return out;
  }

  private static Map<String, List<AdjacencyEdge>> buildAdjacency(List<Edge> edges, BitSet bits) {
    Map<String, List<AdjacencyEdge>> adj = new LinkedHashMap<>();
    for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx + 1)) {
      Edge e = edges.get(idx);
      adj.computeIfAbsent(e.source(), k -> new ArrayList<>()).add(new AdjacencyEdge(idx, e));
      adj.computeIfAbsent(e.target(), k -> new ArrayList<>()).add(new AdjacencyEdge(idx, e));
    }
    return adj;
  }

  private static CPQ loopForVertex(
      String current, Map<String, List<AdjacencyEdge>> adj, BitSet visited, int diameterCap) {

    List<CPQ> segments = new ArrayList<>();

    for (AdjacencyEdge e : adj.getOrDefault(current, List.of())) {
      if (visited.get(e.index())) continue;
      visited.set(e.index());

      CPQ segment;
      if (e.isSelfLoop()) {
        segment = CPQ.intersect(List.of(e.stepFrom(current), CPQ.id()));
      } else {
        String next = e.other(current);
        CPQ forward = e.stepFrom(current);
        CPQ nested = loopForVertex(next, adj, visited, diameterCap);
        CPQ backward = e.stepFrom(next);

        List<CPQ> path = new ArrayList<>(3);
        path.add(forward);
        if (!nested.equals(CPQ.IDENTITY)) path.add(nested);
        path.add(backward);

        CPQ concat = path.size() == 1 ? path.get(0) : CPQ.concat(path);
        segment = CPQ.intersect(List.of(concat, CPQ.id()));
      }

      if (diameterCap > 0 && segment.getDiameter() > diameterCap) {
        visited.clear(e.index());
        continue;
      }
      segments.add(segment);
    }

    if (segments.isEmpty()) return CPQ.id();
    return segments.size() == 1 ? segments.get(0) : CPQ.concat(segments);
  }

  private record AdjacencyEdge(int index, Edge edge) {
    String other(String vertex) {
      return edge.source().equals(vertex) ? edge.target() : edge.source();
    }

    boolean isSelfLoop() {
      return edge.source().equals(edge.target());
    }

    CPQ stepFrom(String vertex) {
      return edge.source().equals(vertex)
          ? CPQ.label(edge.predicate())
          : CPQ.label(edge.predicate().getInverse());
    }
  }
}
