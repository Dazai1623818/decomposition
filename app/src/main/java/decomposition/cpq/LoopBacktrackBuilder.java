package decomposition.cpq;

import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Synthesizes loop-shaped CPQs that cover every edge in a component via backtracking.
 */
final class LoopBacktrackBuilder {

    private LoopBacktrackBuilder() {
    }

    static List<KnownComponent> build(List<Edge> edges, BitSet edgeBits) {
        Map<String, List<AdjacencyEdge>> adjacency = buildAdjacency(edges, edgeBits);
        if (adjacency.isEmpty()) {
            return List.of();
        }

        List<KnownComponent> results = new ArrayList<>();
        for (String anchor : adjacency.keySet()) {
            Set<Integer> visited = new HashSet<>();
            String expression = loopExpression(anchor, null, adjacency, visited);
            if (expression.isBlank()) {
                continue;
            }
            if (visited.size() != edgeBits.cardinality()) {
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
                                         Set<Integer> visited) {
        List<String> segments = new ArrayList<>();
        for (AdjacencyEdge edge : adjacency.getOrDefault(current, List.of())) {
            String neighbor = edge.other(current);
            if (parent != null && neighbor.equals(parent)) {
                continue;
            }
            if (!visited.add(edge.index())) {
                continue;
            }

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
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < segments.size(); i++) {
            if (i > 0) {
                builder.append(" ◦ ");
            }
            String segment = segments.get(i);
            builder.append(segment.contains("◦") ? "(" + segment + ")" : segment);
        }
        String combined = builder.toString();
        if (segments.size() > 1) {
            combined = "(" + combined + ")";
        }
        return combined;
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
