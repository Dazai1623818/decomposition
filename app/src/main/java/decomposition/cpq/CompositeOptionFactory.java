package decomposition.cpq;

import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Generates composite CPQ candidates by joining subcomponents through concatenation or
 * intersection.
 */
final class CompositeOptionFactory {

  private CompositeOptionFactory() {}

  static List<KnownComponent> build(
      BitSet edgeBits, int totalEdgeCount, Function<BitSet, List<KnownComponent>> optionLookup) {
    List<KnownComponent> results = new ArrayList<>();
    forEachSplit(
        edgeBits,
        totalEdgeCount,
        (subsetA, subsetB) -> {
          List<KnownComponent> left = optionLookup.apply(subsetA);
          List<KnownComponent> right = optionLookup.apply(subsetB);
          if (left.isEmpty() || right.isEmpty()) {
            return;
          }
          for (KnownComponent lhs : left) {
            for (KnownComponent rhs : right) {
              tryConcat(edgeBits, lhs, rhs, results);
              tryIntersect(edgeBits, lhs, rhs, results);
            }
          }
        });
    return results;
  }

  /**
   * Enumerates all two-way partitions of the given bitset (excluding empty/full subsets) and hands
   * each pair to the visitor.
   */
  private static void forEachSplit(
      BitSet edgeBits, int totalEdgeCount, BiConsumer<BitSet, BitSet> visitor) {
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
      visitor.accept(subsetA, subsetB);
    }
  }

  private static void tryConcat(
      BitSet edgeBits, KnownComponent left, KnownComponent right, List<KnownComponent> sink) {
    if (!left.target().equals(right.source())) {
      return;
    }
    String expression = "(" + left.cpq() + " ◦ " + right.cpq() + ")";
    String derivation =
        "Concatenation: ["
            + left.cpqRule()
            + "] then ["
            + right.cpqRule()
            + "] via "
            + left.target();
    emitIfParsable(edgeBits, expression, left.source(), right.target(), derivation, sink);
  }

  private static void tryIntersect(
      BitSet edgeBits, KnownComponent left, KnownComponent right, List<KnownComponent> sink) {
    if (!left.source().equals(right.source()) || !left.target().equals(right.target())) {
      return;
    }
    String expression = "(" + left.cpq() + " ∩ " + right.cpq() + ")";
    String derivation =
        "Intersection: ["
            + left.cpqRule()
            + "] ∩ ["
            + right.cpqRule()
            + "] at "
            + left.source()
            + "→"
            + left.target();
    emitIfParsable(edgeBits, expression, left.source(), left.target(), derivation, sink);
  }

  private static void emitIfParsable(
      BitSet edgeBits,
      String expression,
      String source,
      String target,
      String derivation,
      List<KnownComponent> sink) {
    try {
      CPQ cpq = CPQ.parse(expression);
      sink.add(new KnownComponent(cpq, edgeBits, source, target, derivation));
    } catch (RuntimeException ex) {
      // Ignore unparsable synthesized expression.
    }
  }
}
