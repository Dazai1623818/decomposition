package decomposition.cpq;

import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Generates the CPQ alternatives that arise from a single CQ edge.
 */
final class SingleEdgeOptionFactory {

    private SingleEdgeOptionFactory() {
    }

    static List<KnownComponent> build(Edge edge, BitSet edgeBits) {
        List<KnownComponent> options = new ArrayList<>();

        addForward(edge, edgeBits, options);
        addInverse(edge, edgeBits, options);
        addBacktracks(edge, edgeBits, options);

        return options;
    }

    private static void addForward(Edge edge, BitSet bits, List<KnownComponent> out) {
        CPQ forward = CPQ.parse(edge.label());
        out.add(KnownComponentFactory.create(
                forward,
                bits,
                edge.source(),
                edge.target(),
                "Forward atom on label '" + edge.label() + "' (" + edge.source() + "→" + edge.target() + ")"));
    }

    private static void addInverse(Edge edge, BitSet bits, List<KnownComponent> out) {
        if (edge.source().equals(edge.target())) {
            return;
        }
        String inverseLabel = edge.label() + "⁻";
        try {
            CPQ inverse = CPQ.parse(inverseLabel);
            out.add(KnownComponentFactory.create(
                    inverse,
                    bits,
                    edge.target(),
                    edge.source(),
                    "Inverse atom on label '" + edge.label() + "' (" + edge.target() + "→" + edge.source() + ")"));
        } catch (RuntimeException ex) {
            // Ignore labels whose inverse cannot be parsed.
        }
    }

    private static void addBacktracks(Edge edge, BitSet bits, List<KnownComponent> out) {
        if (edge.source().equals(edge.target())) {
            return;
        }
        addLoop(out,
                "((" + edge.label() + " ◦ " + edge.label() + "⁻) ∩ id)",
                bits,
                edge.source(),
                "Backtrack loop via '" + edge.label() + "' at " + edge.source());

        addLoop(out,
                "((" + edge.label() + "⁻ ◦ " + edge.label() + ") ∩ id)",
                bits,
                edge.target(),
                "Backtrack loop via '" + edge.label() + "' at " + edge.target());
    }

    private static void addLoop(List<KnownComponent> out,
                                String expression,
                                BitSet bits,
                                String anchor,
                                String derivation) {
        try {
            CPQ cpq = CPQ.parse(expression);
            out.add(KnownComponentFactory.create(cpq, bits, anchor, anchor, derivation));
        } catch (RuntimeException ex) {
            // Skip unparsable backtrack form.
        }
    }
}
