package decomposition.cpq;

import decomposition.core.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/** Generates the CPQ expressions that arise from a single CQ edge. */
final class SingleEdgeExpressionFactory {

  private SingleEdgeExpressionFactory() {}

  static List<CPQExpression> build(Edge edge, BitSet edgeBits, Map<String, String> varToNodeMap) {
    List<CPQExpression> expressions = new ArrayList<>();

    addForward(edge, edgeBits, varToNodeMap, expressions);
    addInverse(edge, edgeBits, varToNodeMap, expressions);
    addBacktracks(edge, edgeBits, varToNodeMap, expressions);

    return expressions;
  }

  private static void addForward(
      Edge edge, BitSet bits, Map<String, String> varToNodeMap, List<CPQExpression> out) {
    CPQ forward = CPQ.label(edge.predicate());
    out.add(
        new CPQExpression(
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
      Edge edge, BitSet bits, Map<String, String> varToNodeMap, List<CPQExpression> out) {
    if (edge.source().equals(edge.target())) {
      return;
    }
    CPQ inverse = CPQ.label(edge.predicate().getInverse());
    out.add(
        new CPQExpression(
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
  }

  private static void addBacktracks(
      Edge edge, BitSet bits, Map<String, String> varToNodeMap, List<CPQExpression> out) {
    if (edge.source().equals(edge.target())) {
      // CQ already a true self-loop (single vertex, src==target); no extra backtracking variants
      // needed.
      return;
    }
    // Backtracking loops revisit the start vertex via an inverse step, so the resulting CPQ still
    // has src==target
    // even though the underlying CQ edge connects two distinct nodes.
    CPQ forward = CPQ.label(edge.predicate());
    CPQ inverse = CPQ.label(edge.predicate().getInverse());
    CPQ sourceLoopBody = CPQ.concat(List.of(forward, inverse));
    CPQ targetLoopBody = CPQ.concat(List.of(inverse, forward));

    addLoop(
        out,
        CPQ.intersect(List.of(sourceLoopBody, CPQ.id())),
        bits,
        edge.source(),
        "Backtrack loop via '" + edge.label() + "' at " + edge.source(),
        varToNodeMap);

    addLoop(
        out,
        CPQ.intersect(List.of(targetLoopBody, CPQ.id())),
        bits,
        edge.target(),
        "Backtrack loop via '" + edge.label() + "' at " + edge.target(),
        varToNodeMap);
  }

  private static void addLoop(
      List<CPQExpression> out,
      CPQ cpq,
      BitSet bits,
      String anchor,
      String derivation,
      Map<String, String> varToNodeMap) {
    out.add(new CPQExpression(cpq, bits, anchor, anchor, derivation, varToNodeMap));
  }
}
