package decomposition.core.model;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Connected subset of edges together with the incident vertices. */
public record Component(
    BitSet edgeBits, Set<String> vertices, Set<String> joinNodes, Map<String, String> varMap) {

  public Component {
    Objects.requireNonNull(edgeBits, "edgeBits");
    Objects.requireNonNull(vertices, "vertices");

    edgeBits = (BitSet) edgeBits.clone();
    vertices = Collections.unmodifiableSet(new HashSet<>(vertices));
    joinNodes =
        joinNodes == null || joinNodes.isEmpty()
            ? Set.of()
            : Collections.unmodifiableSet(new HashSet<>(joinNodes));
    varMap =
        varMap == null || varMap.isEmpty()
            ? Map.of()
            : Collections.unmodifiableMap(new HashMap<>(varMap));
  }

  public int edgeCount() {
    return edgeBits.cardinality();
  }

  @Override
  public BitSet edgeBits() {
    return (BitSet) edgeBits.clone();
  }

  public boolean containsEdge(int index) {
    return edgeBits.get(index);
  }

  public Component restrictTo(BitSet subEdgeBits, List<Edge> edges) {
    Objects.requireNonNull(subEdgeBits, "subEdgeBits");
    Objects.requireNonNull(edges, "edges");

    Set<String> subVertices = new HashSet<>();
    for (int idx = subEdgeBits.nextSetBit(0); idx >= 0; idx = subEdgeBits.nextSetBit(idx + 1)) {
      Edge edge = edges.get(idx);
      subVertices.add(edge.source());
      subVertices.add(edge.target());
    }

    Set<String> subJoinNodes = new HashSet<>();
    for (String joinNode : joinNodes()) {
      if (subVertices.contains(joinNode)) {
        subJoinNodes.add(joinNode);
      }
    }

    return new Component(subEdgeBits, subVertices, subJoinNodes, varMap());
  }

  public boolean endpointsAllowed(String source, String target) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");

    Set<String> vertices = vertices();
    if (vertices.size() == 1) {
      String v = vertices.iterator().next();
      return v.equals(source) && v.equals(target);
    }

    Set<String> joinNodes = joinNodes();
    return switch (joinNodes.size()) {
      case 0 -> true;
      case 1 -> {
        String join = joinNodes.iterator().next();
        yield edgeCount() == 1
            ? (join.equals(source) || join.equals(target))
            : (join.equals(source) && join.equals(target));
      }
      case 2 -> joinNodes.contains(source) && joinNodes.contains(target) && !source.equals(target);
      default -> false;
    };
  }
}
