package decomposition.cpq;

import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.BitSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Generates composite CPQ candidates by joining subcomponents through
 * concatenation or intersection.
 */
final class CompositeOptionFactory {

    private CompositeOptionFactory() {
    }

    static void build(BitSet edgeBits,
                      int totalEdgeCount,
                      Function<BitSet, List<KnownComponent>> optionLookup,
                      Consumer<KnownComponent> sink) {
        List<Integer> indices = BitsetUtils.toIndexList(edgeBits);
        int combos = 1 << indices.size();
        for (int mask = 1; mask < combos - 1; mask++) {
            BitSet subsetA = new BitSet(totalEdgeCount);
            for (int i = 0; i < indices.size(); i++) {
                if ((mask & (1 << i)) != 0) {
                    subsetA.set(indices.get(i));
                }
            }

            BitSet subsetB = BitsetUtils.copy(edgeBits);
            subsetB.andNot(subsetA);

            List<KnownComponent> left = optionLookup.apply(subsetA);
            List<KnownComponent> right = optionLookup.apply(subsetB);
            if (left.isEmpty() || right.isEmpty()) {
                continue;
            }

            for (KnownComponent lhs : left) {
                for (KnownComponent rhs : right) {
                    tryConcat(edgeBits, lhs, rhs, sink);
                    tryIntersect(edgeBits, lhs, rhs, sink);
                }
            }
        }
    }

    private static void tryConcat(BitSet edgeBits,
                                  KnownComponent left,
                                  KnownComponent right,
                                  Consumer<KnownComponent> sink) {
        if (!left.target().equals(right.source())) {
            return;
        }
        String expression = "(" + left.cpq() + " ◦ " + right.cpq() + ")";
        try {
            CPQ cpq = CPQ.parse(expression);
            String derivation = "Concatenation: [" + left.cpqRule() + "] then [" + right.cpqRule()
                    + "] via " + left.target();
            sink.accept(KnownComponentFactory.create(cpq,
                    edgeBits,
                    left.source(),
                    right.target(),
                    derivation));
        } catch (RuntimeException ex) {
            // Ignore unparsable concatenation.
        }
    }

    private static void tryIntersect(BitSet edgeBits,
                                     KnownComponent left,
                                     KnownComponent right,
                                     Consumer<KnownComponent> sink) {
        if (!left.source().equals(right.source()) || !left.target().equals(right.target())) {
            return;
        }
        String expression = "(" + left.cpq() + " ∩ " + right.cpq() + ")";
        try {
            CPQ cpq = CPQ.parse(expression);
            String derivation = "Intersection: [" + left.cpqRule() + "] ∩ [" + right.cpqRule()
                    + "] at " + left.source() + "→" + left.target();
            sink.accept(KnownComponentFactory.create(cpq,
                    edgeBits,
                    left.source(),
                    left.target(),
                    derivation));
        } catch (RuntimeException ex) {
            // Ignore unparsable intersection.
        }
    }
}
