package decomposition.cpq;

import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Generates composite CPQ expressions by joining subcomponents via
 * concatenation or intersection.
 */
final class CompositeExpressionFactory {

  private CompositeExpressionFactory() {
  }

  static List<CPQExpression> build(
      BitSet edgeBits,
      int totalEdgeCount,
      Function<BitSet, List<CPQExpression>> constructionRuleLookup) {
    List<CPQExpression> results = new ArrayList<>();
    forEachSplit(
        edgeBits,
        totalEdgeCount,
        (subsetA, subsetB) -> {
          List<CPQExpression> left = constructionRuleLookup.apply(subsetA);
          List<CPQExpression> right = constructionRuleLookup.apply(subsetB);
          if (left.isEmpty() || right.isEmpty()) {
            return;
          }
          for (CPQExpression lhs : left) {
            for (CPQExpression rhs : right) {
              tryConcat(edgeBits, lhs, rhs, results);
              tryIntersect(edgeBits, lhs, rhs, results);
            }
          }
        });
    return results;
  }

  /**
   * Enumerates all two-way partitions of the given bitset (excluding empty/full
   * subsets) and hands
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

      BitSet subsetB = (BitSet) edgeBits.clone();
      subsetB.andNot(subsetA);
      visitor.accept(subsetA, subsetB);
    }
  }

  private static void tryConcat(
      BitSet edgeBits, CPQExpression left, CPQExpression right, List<CPQExpression> sink) {
    if (!left.target().equals(right.source())) {
      return;
    }
    if (!left.varToNodeMap().equals(right.varToNodeMap())) {
      return;
    }
    CPQ concatenated = CPQ.concat(List.of(left.cpq(), right.cpq()));
    String derivation = "Concatenation: ["
        + left.cpqRule()
        + "] then ["
        + right.cpqRule()
        + "] via "
        + left.target();
    emitIfParsable(
        edgeBits,
        concatenated,
        left.source(),
        right.target(),
        derivation,
        left.varToNodeMap(),
        sink);
  }

  private static void tryIntersect(
      BitSet edgeBits, CPQExpression left, CPQExpression right, List<CPQExpression> sink) {
    if (!left.source().equals(right.source()) || !left.target().equals(right.target())) {
      return;
    }
    if (!left.varToNodeMap().equals(right.varToNodeMap())) {
      return;
    }
    CPQ intersection = CPQ.intersect(List.of(left.cpq(), right.cpq()));
    String derivation = "Intersection: ["
        + left.cpqRule()
        + "] ∩ ["
        + right.cpqRule()
        + "] at "
        + left.source()
        + "→"
        + left.target();
    emitIfParsable(
        edgeBits,
        intersection,
        left.source(),
        left.target(),
        derivation,
        left.varToNodeMap(),
        sink);
  }

  private static void emitIfParsable(
      BitSet edgeBits,
      CPQ cpq,
      String source,
      String target,
      String derivation,
      Map<String, String> varToNodeMap,
      List<CPQExpression> sink) {
    sink.add(new CPQExpression(cpq, edgeBits, source, target, derivation, varToNodeMap));
  }
}
