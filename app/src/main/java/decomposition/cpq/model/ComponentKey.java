package decomposition.cpq.model;

import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.Objects;

public record ComponentKey(BitSet edgeBits, String source, String target) {
  public ComponentKey {
    edgeBits = BitsetUtils.copy(edgeBits);
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
  }

  public BitSet bits() {
    return BitsetUtils.copy(edgeBits);
  }
}
