package decomposition.cpq;

import decomposition.util.BitsetUtils;
import java.util.BitSet;

/**
 * Structural identity for a component based on its covered edges and oriented endpoints. The
 * underlying CPQ syntax is intentionally ignored; only the coverage signature matters.
 *
 * <p>This is a lightweight wrapper around ComponentSignature that maintains backward compatibility
 * while delegating to the core domain concept.
 */
public record ComponentKey(ComponentSignature signature) {

  public ComponentKey(BitSet bits, int totalEdges, String source, String target) {
    this(new ComponentSignature(bits, source, target));
    // totalEdges parameter ignored - not part of component identity per domain specification
  }

  public BitSet bits() {
    return signature.edgeBits();
  }

  public String source() {
    return signature.source();
  }

  public String target() {
    return signature.target();
  }

  @Override
  public String toString() {
    return "ComponentKey["
        + BitsetUtils.signature(signature.edgeBits(), signature.edgeBits().length())
        + "|"
        + signature.source()
        + "→"
        + signature.target()
        + "]";
  }
}
