package decomposition.cpq;

import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/** Generates the CPQ alternatives that arise from a single CQ edge. */
final class SingleEdgeOptionFactory {

  private SingleEdgeOptionFactory() {}

  static List<KnownComponent> build(Edge edge, BitSet edgeBits) {
    List<KnownComponent> options = new ArrayList<>();

    addForward(edge, edgeBits, options);
    addInverse(edge, edgeBits, options);
    addBacktracks(edge, edgeBits, options);

    return options;
  }

  private static void addForward(Edge edge, BitSet bits, List<KnownComponent> out) {
    CPQ forward = CPQ.parse(edge.label());
    out.add(
        new KnownComponent(
            forward,
            bits,
            edge.source(),
            edge.target(),
            "Forward atom on label '"
                + edge.label()
                + "' ("
                + edge.source()
                + "→"
                + edge.target()
                + ")"));
  }

  private static void addInverse(Edge edge, BitSet bits, List<KnownComponent> out) {
    if (edge.source().equals(edge.target())) {
      return;
    }
    String inverseLabel = edge.label() + "⁻";
    try {
      CPQ inverse = CPQ.parse(inverseLabel);
      out.add(
          new KnownComponent(
              inverse,
              bits,
              edge.target(),
              edge.source(),
              "Inverse atom on label '"
                  + edge.label()
                  + "' ("
                  + edge.target()
                  + "→"
                  + edge.source()
                  + ")"));
    } catch (RuntimeException ex) {
      // Ignore labels whose inverse cannot be parsed.
    }
  }

  private static void addBacktracks(Edge edge, BitSet bits, List<KnownComponent> out) {
    if (edge.source().equals(edge.target())) {
      // CQ already a true self-loop (single vertex, src==target); no extra backtracking variants
      // needed.
      return;
    }
    // Backtracking loops revisit the start vertex via an inverse step, so the resulting CPQ still
    // has src==target
    // even though the underlying CQ edge connects two distinct nodes.
    addLoop(
        out,
        "((" + edge.label() + " ◦ " + edge.label() + "⁻) ∩ id)",
        bits,
        edge.source(),
        "Backtrack loop via '" + edge.label() + "' at " + edge.source());

    addLoop(
        out,
        "((" + edge.label() + "⁻ ◦ " + edge.label() + ") ∩ id)",
        bits,
        edge.target(),
        "Backtrack loop via '" + edge.label() + "' at " + edge.target());
  }

  private static void addLoop(
      List<KnownComponent> out, String expression, BitSet bits, String anchor, String derivation) {
    try {
      CPQ cpq = CPQ.parse(expression);
      out.add(new KnownComponent(cpq, bits, anchor, anchor, derivation));
    } catch (RuntimeException ex) {
      // Skip unparsable backtrack form.
    }
  }
}
