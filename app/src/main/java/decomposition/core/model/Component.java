package decomposition.core.model;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
}
