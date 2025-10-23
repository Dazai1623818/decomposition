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
            String expression = loopExpression(anchor, null, adjacency, visited);
            if (expression.isBlank()) {
                continue;
            }
            if (visited.cardinality() != edgeBits.cardinality()) {
                continue;
            }
            try {
                CPQ cpq = CPQ.parse(expression);
                results.add(KnownComponentFactory.create(
                        cpq,
                        edgeBits,
                        anchor,
                        anchor,
                        "Loop via backtracking anchored at '" + anchor + "'"));
            } catch (RuntimeException ex) {
                // Ignore unparsable synthesized expressions.
            }
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

    private static String loopExpression(String current,
                                         String parent,
                                         Map<String, List<AdjacencyEdge>> adjacency,
                                         BitSet visited) {
        List<String> segments = new ArrayList<>();
        for (AdjacencyEdge edge : adjacency.getOrDefault(current, List.of())) {
            String neighbor = edge.other(current);
            if (parent != null && neighbor.equals(parent)) {
                continue;
            }
            if (visited.get(edge.index())) {
                continue;
            }
            // Edges are marked globally per anchor; we do not backtrack the bit because every edge
            // must be consumed exactly once in the synthetic loop.
            visited.set(edge.index());

            String forward = edge.tokenFrom(current);
            String nested = loopExpression(neighbor, current, adjacency, visited);
            String backward = edge.tokenFrom(neighbor);

            StringBuilder builder = new StringBuilder();
            builder.append(forward);
            if (!nested.isBlank()) {
                builder.append(" ◦ ").append(nested);
            }
            builder.append(" ◦ ").append(backward);
            segments.add("((" + builder + ") ∩ id)");
        }

        if (segments.isEmpty()) {
            return "";
        }
        return composeSegments(segments);
    }

    private static String composeSegments(List<String> segments) {
        if (segments.isEmpty()) {
            return "";
        }
        List<String> normalized = new ArrayList<>(segments.size());
        for (String segment : segments) {
            normalized.add(segment.contains("◦") ? "(" + segment + ")" : segment);
        }
        String combined = String.join(" ◦ ", normalized);
        return segments.size() > 1 ? "(" + combined + ")" : combined;
    }

    private record AdjacencyEdge(int index, Edge edge) {
        String other(String vertex) {
            return edge.source().equals(vertex) ? edge.target() : edge.source();
        }

        String tokenFrom(String vertex) {
            return edge.source().equals(vertex) ? edge.label() : edge.label() + "⁻";
        }
    }
}
