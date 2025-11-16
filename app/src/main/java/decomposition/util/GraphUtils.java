package decomposition.util;

import decomposition.model.Component;
import decomposition.model.Edge;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Graph utilities used across the decomposition pipeline. */
public final class GraphUtils {
  private GraphUtils() {}

  public static Set<String> vertices(BitSet edgeBits, List<Edge> edges) {
    Set<String> vertices = new HashSet<>();
    for (int idx = edgeBits.nextSetBit(0); idx >= 0; idx = edgeBits.nextSetBit(idx + 1)) {
      Edge edge = edges.get(idx);
      vertices.add(edge.source());
      vertices.add(edge.target());
    }
    return vertices;
  }

  public static Set<String> verticesForComponent(Component component, List<Edge> edges) {
    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(edges, "edges");
    return vertices(component.edgeBits(), edges);
  }

  public static boolean isConnected(BitSet edgeBits, List<Edge> edges) {
    if (edgeBits.isEmpty()) {
      return false;
    }

    Map<String, Set<String>> adjacency = buildAdjacency(edgeBits, edges);
    Set<String> visited = new HashSet<>();
    Deque<String> stack = new ArrayDeque<>();
    String start = pickStartVertex(edgeBits, edges);
    if (start == null) {
      return false;
    }
    stack.push(start);

    while (!stack.isEmpty()) {
      String current = stack.pop();
      if (!visited.add(current)) {
        continue;
      }
      for (String neighbor : adjacency.getOrDefault(current, Set.of())) {
        if (!visited.contains(neighbor)) {
          stack.push(neighbor);
        }
      }
    }

    return visited.containsAll(vertices(edgeBits, edges));
  }

  public static Component buildComponent(BitSet edgeBits, List<Edge> edges) {
    return new Component(edgeBits, vertices(edgeBits, edges));
  }

  public static Map<String, Set<String>> buildAdjacency(BitSet edgeBits, List<Edge> edges) {
    Map<String, Set<String>> adjacency = new HashMap<>();
    for (int idx = edgeBits.nextSetBit(0); idx >= 0; idx = edgeBits.nextSetBit(idx + 1)) {
      Edge edge = edges.get(idx);
      adjacency.computeIfAbsent(edge.source(), k -> new HashSet<>()).add(edge.target());
      adjacency.computeIfAbsent(edge.target(), k -> new HashSet<>()).add(edge.source());
    }
    return adjacency;
  }

  private static String pickStartVertex(BitSet edgeBits, List<Edge> edges) {
    int firstIndex = edgeBits.nextSetBit(0);
    if (firstIndex < 0) {
      return null;
    }
    Edge startEdge = edges.get(firstIndex);
    return startEdge.source();
  }
}
