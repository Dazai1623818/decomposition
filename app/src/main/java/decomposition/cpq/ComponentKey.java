package decomposition.cpq;

import java.util.BitSet;
import java.util.Objects;

/**
 * A unique key for a component based on its CQ structure: covered edges and oriented endpoints.
 * CPQs are compared purely by this structural signature; internal syntax and anchoring are ignored.
 */
public final class ComponentKey {
    private final BitSet bits;
    private final int totalEdges;
    private final String source;
    private final String target;

    public ComponentKey(BitSet bits, int totalEdges, String source, String target) {
        this.bits = (BitSet) bits.clone();
        this.totalEdges = totalEdges;
        this.source = Objects.requireNonNull(source, "source");
        this.target = Objects.requireNonNull(target, "target");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ComponentKey other)) return false;
        return bits.equals(other.bits)
                && source.equals(other.source)
                && target.equals(other.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bits, source, target);
    }

    @Override
    public String toString() {
        return "ComponentKey[" + bits + "/" + totalEdges + "|" + source + "->" + target + "]";
    }
}
