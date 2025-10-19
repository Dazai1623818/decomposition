package decomposition.cpq;

import decomposition.util.BitsetUtils;
import java.util.BitSet;
import java.util.Objects;
import dev.roanh.gmark.lang.cpq.CPQ;

/**
 * Represents a recognized component together with its CPQ rule.
 */
public record KnownComponent(
        boolean isCPQ,
        String cpqRule,
        BitSet edges,
        String source,
        String target) {

    public KnownComponent {
        Objects.requireNonNull(cpqRule, "cpqRule");
        Objects.requireNonNull(edges, "edges");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        edges = BitsetUtils.copy(edges);
    }

    public BitSet edges() {
        return BitsetUtils.copy(edges);
    }

    public KnownComponent flipped(String flippedRule) {
        return new KnownComponent(isCPQ, flippedRule, edges(), target, source);
    }

    public CPQ parse() {
        return CPQ.parse(cpqRule);
    }
}
