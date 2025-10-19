package decomposition.cpq;

import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.Objects;
import dev.roanh.gmark.lang.cpq.CPQ;

/**
 * Represents a recognized component together with its CPQ AST.
 * The CPQ object is the authoritative representation; toString() is only for display.
 *
 * Design invariants:
 * - CPQs are always forward-directional (source → target)
 * - Reverse edges are represented by inverse labels (r⁻) within the CPQ, never by swapping endpoints
 * - Source and target correspond to the original CQ orientation
 */
public record KnownComponent(
        CPQ cpq,
        BitSet edges,
        String source,
        String target) {

    public KnownComponent {
        Objects.requireNonNull(cpq, "cpq");
        Objects.requireNonNull(edges, "edges");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        edges = BitsetUtils.copy(edges);
    }

    public BitSet edges() {
        return BitsetUtils.copy(edges);
    }

    /**
     * Returns the CPQ rule as a string for display/logging purposes only.
     * This should not be used for equality or identity comparisons.
     */
    public String cpqRule() {
        return cpq.toString();
    }

    /**
     * Creates a component key for memoization based on edges and endpoints.
     */
    public ComponentKey toKey(int totalEdges) {
        return new ComponentKey(edges, totalEdges, source, target);
    }
}
