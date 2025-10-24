package decomposition.cpq;

import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Synthesizes loop-shaped CPQs that cover every edge in a component via backtracking.
 */
final class LoopBacktrackBuilder {

    private LoopBacktrackBuilder() {
    }

    static List<KnownComponent> build(List<Edge> edges, BitSet edgeBits, Set<String> allowedAnchors) {
        Map<String, List<AdjacencyEdge>> adjacency = buildAdjacency(edges, edgeBits);
        if (adjacency.isEmpty()) {
            return List.of();
        }

        List<KnownComponent> results = new ArrayList<>();
        for (String anchor : adjacency.keySet()) {
            if (!isAnchorAllowed(anchor, allowedAnchors)) {
                continue;
            }
            BitSet visited = new BitSet(edgeBits.length());
            CPQ loopCpq = buildLoop(anchor, adjacency, visited);
            if (loopCpq == null) {
                continue;
            }
            results.add(new KnownComponent(
                    loopCpq,
                    edgeBits,
                    anchor,
                    anchor,
                    "Loop via backtracking anchored at '" + anchor + "'"));
        }
        return results;
    }

    private static boolean isAnchorAllowed(String anchor, Set<String> allowedAnchors) {
        return allowedAnchors == null || allowedAnchors.isEmpty() || allowedAnchors.contains(anchor);
    }

    private static Map<String, List<AdjacencyEdge>> buildAdjacency(List<Edge> edges, BitSet bits) {
        Map<String, List<AdjacencyEdge>> adjacency = new LinkedHashMap<>();
        for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx + 1)) {
            Edge edge = edges.get(idx);
            adjacency.computeIfAbsent(edge.source(), k -> new ArrayList<>())
                    .add(new AdjacencyEdge(idx, edge));
            adjacency.computeIfAbsent(edge.target(), k -> new ArrayList<>())
                    .add(new AdjacencyEdge(idx, edge));
        }
        return adjacency;
    }

    private static CPQ buildLoop(String anchor,
                                 Map<String, List<AdjacencyEdge>> adjacency,
                                 BitSet visited) {
        CPQ loopBody = loopForVertex(anchor, adjacency, visited);
        if (loopBody == null) {
            return null;
        }
        return CPQ.intersect(List.of(loopBody, CPQ.id()));
    }

    private static CPQ loopForVertex(String current,
                                     Map<String, List<AdjacencyEdge>> adjacency,
                                     BitSet visited) {
        List<CPQ> segments = new ArrayList<>();
        for (AdjacencyEdge edge : adjacency.getOrDefault(current, List.of())) {
            String neighbor = edge.other(current);
            if (visited.get(edge.index())) {
                continue;
            }
            visited.set(edge.index());

            CPQ segment;
            if (edge.isSelfLoop()) {
                segment = CPQ.intersect(List.of(edge.forwardCpq(current), CPQ.id()));
            } else {
                CPQ forward = edge.forwardCpq(current);
                CPQ nested = loopForVertex(neighbor, adjacency, visited);
                CPQ backward = edge.forwardCpq(neighbor);
                List<CPQ> path = new ArrayList<>();
                path.add(forward);
                if (nested != null && nested != CPQ.id()) {
                    path.add(nested);
                }
                path.add(backward);
                CPQ concat = path.size() == 1 ? path.get(0) : CPQ.concat(path);
                segment = CPQ.intersect(List.of(concat, CPQ.id()));
            }
            segments.add(segment);
        }

        if (segments.isEmpty()) {
            return CPQ.id();
        }
        return segments.size() == 1 ? segments.get(0) : CPQ.concat(segments);
    }

    private record AdjacencyEdge(int index, Edge edge) {
        String other(String vertex) {
            return edge.source().equals(vertex) ? edge.target() : edge.source();
        }

        boolean isSelfLoop() {
            return edge.source().equals(edge.target());
        }

        CPQ forwardCpq(String vertex) {
            if (edge.source().equals(vertex)) {
                return CPQ.label(edge.predicate());
            }
            return CPQ.label(edge.predicate().getInverse());
        }
    }
}
