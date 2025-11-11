package decomposition.cpq;

import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/** Generates the CPQ construction rules that arise from a single CQ edge. */
final class SingleEdgeRuleFactory {

  private SingleEdgeRuleFactory() {}

  static List<KnownComponent> build(Edge edge, BitSet edgeBits, Map<String, String> varToNodeMap) {
    List<KnownComponent> constructionRules = new ArrayList<>();

    addForward(edge, edgeBits, varToNodeMap, constructionRules);
    addInverse(edge, edgeBits, varToNodeMap, constructionRules);
    addBacktracks(edge, edgeBits, varToNodeMap, constructionRules);

    return constructionRules;
  }

  private static void addForward(
      Edge edge, BitSet bits, Map<String, String> varToNodeMap, List<KnownComponent> out) {
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
                + ")",
            varToNodeMap));
  }

  private static void addInverse(
      Edge edge, BitSet bits, Map<String, String> varToNodeMap, List<KnownComponent> out) {
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
                  + ")",
              varToNodeMap));
    } catch (RuntimeException ex) {
      // Ignore labels whose inverse cannot be parsed.
    }
  }

  private static void addBacktracks(
      Edge edge, BitSet bits, Map<String, String> varToNodeMap, List<KnownComponent> out) {
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
        "Backtrack loop via '" + edge.label() + "' at " + edge.source(),
        varToNodeMap);

    addLoop(
        out,
        "((" + edge.label() + "⁻ ◦ " + edge.label() + ") ∩ id)",
        bits,
        edge.target(),
        "Backtrack loop via '" + edge.label() + "' at " + edge.target(),
        varToNodeMap);
  }

  private static void addLoop(
      List<KnownComponent> out,
      String expression,
      BitSet bits,
      String anchor,
      String derivation,
      Map<String, String> varToNodeMap) {
    try {
      CPQ cpq = CPQ.parse(expression);
      out.add(new KnownComponent(cpq, bits, anchor, anchor, derivation, varToNodeMap));
    } catch (RuntimeException ex) {
      // Skip unparsable backtrack form.
    }
  }
}
