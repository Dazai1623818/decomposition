package decomposition.cpq;

import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.Objects;

/**
 * A unique key for a component based on its CQ structure: edge coverage + endpoints.
 * Used for memoization and deduplication instead of CPQ string representations.
 */
public final class ComponentKey {
    private final String bitSignature;
    private final String source;
    private final String target;

    public ComponentKey(BitSet bits, int totalEdges, String source, String target) {
        this.bitSignature = BitsetUtils.signature(bits, totalEdges);
        this.source = Objects.requireNonNull(source, "source");
        this.target = Objects.requireNonNull(target, "target");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ComponentKey other)) return false;
        return bitSignature.equals(other.bitSignature)
                && source.equals(other.source)
                && target.equals(other.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bitSignature, source, target);
    }

    @Override
    public String toString() {
        return "ComponentKey[" + bitSignature + "|" + source + "->" + target + "]";
    }
}
