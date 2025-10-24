package decomposition.model;

import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Connected subset of edges together with the incident vertices. */
public record Component(BitSet edgeBits, Set<String> vertices) {

  public Component {
    Objects.requireNonNull(edgeBits, "edgeBits");
    Objects.requireNonNull(vertices, "vertices");
    edgeBits = (BitSet) edgeBits.clone();
    vertices = Collections.unmodifiableSet(new HashSet<>(vertices));
  }

  @Override
  public BitSet edgeBits() {
    return (BitSet) edgeBits.clone();
  }

  public int edgeCount() {
    return edgeBits.cardinality();
  }

  public boolean containsEdge(int index) {
    return edgeBits.get(index);
  }
}
