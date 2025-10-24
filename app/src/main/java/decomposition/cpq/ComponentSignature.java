package decomposition.cpq;

import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.Objects;

/**
 * Core identity signature for component equivalence based on the domain specification: two CPQ
 * candidates are component-equivalent iff they share the same covered edge bitset, the same
 * oriented endpoints (source → target), and a label-preserving variable mapping.
 *
 * <p>This signature captures exactly the structural identity requirements without including
 * implementation details like totalEdges or the CPQ syntax tree.
 */
public record ComponentSignature(BitSet edgeBits, String source, String target) {

  public ComponentSignature {
    Objects.requireNonNull(edgeBits, "edgeBits");
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
    edgeBits = BitsetUtils.copy(edgeBits);
  }

  @Override
  public BitSet edgeBits() {
    return BitsetUtils.copy(edgeBits);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ComponentSignature other)) {
      return false;
    }
    return edgeBits.equals(other.edgeBits)
        && source.equals(other.source)
        && target.equals(other.target);
  }

  @Override
  public int hashCode() {
    // Use signature string for stable hashing (same approach as ComponentKey)
    return Objects.hash(BitsetUtils.signature(edgeBits, edgeBits.length()), source, target);
  }

  @Override
  public String toString() {
    return "ComponentSignature["
        + BitsetUtils.signature(edgeBits, edgeBits.length())
        + "|"
        + source
        + "→"
        + target
        + "]";
  }
}
