package decomposition.cpq;

import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.Objects;

/**
 * Structural identity for a component based on its covered edges and oriented endpoints.
 * The underlying CPQ syntax is intentionally ignored; only the coverage signature matters.
 */
public record ComponentKey(BitSet bits, int totalEdges, String source, String target) {

    public ComponentKey {
        Objects.requireNonNull(bits, "bits");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        bits = BitsetUtils.copy(bits);
    }

    @Override
    public BitSet bits() {
        return BitsetUtils.copy(bits);
    }

    @Override
    public String toString() {
        return "ComponentKey[" + bits + "/" + totalEdges + "|" + source + "->" + target + "]";
    }
}
