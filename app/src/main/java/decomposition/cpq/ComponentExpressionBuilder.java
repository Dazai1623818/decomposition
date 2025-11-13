package decomposition.cpq;

import decomposition.cpq.model.ComponentKey;
import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Recursively builds CPQ expressions for a single component (edge subset). Handles recursive
 * composition, loop variants, and orientation.
 */
public final class ComponentExpressionBuilder {
  private final List<Edge> edges;
  private final ReverseLoopGenerator reverseLoopGenerator;
  private final Map<RuleCacheKey, List<CPQExpression>> ruleCache = new ConcurrentHashMap<>();

  public ComponentExpressionBuilder(List<Edge> edges) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
    ComponentEdgeMatcher matcher = new ComponentEdgeMatcher(this.edges);
    this.reverseLoopGenerator = new ReverseLoopGenerator(matcher);
  }

  public List<CPQExpression> build(BitSet edgeSubset, Map<String, String> originalVarMap) {
    return build(edgeSubset, Set.of(), originalVarMap);
  }

  public List<CPQExpression> build(
      BitSet edgeSubset, Set<String> requestedJoinNodes, Map<String, String> originalVarMap) {
    Objects.requireNonNull(edgeSubset, "edgeSubset");
    Objects.requireNonNull(originalVarMap, "originalVarMap");
    if (edgeSubset.isEmpty()) {
      return List.of();
    }
    Set<String> normalizedJoinNodes =
        (requestedJoinNodes == null || requestedJoinNodes.isEmpty())
            ? Set.of()
            : Set.copyOf(requestedJoinNodes);
    return recurse(edgeSubset, normalizedJoinNodes, originalVarMap);
  }

  private List<CPQExpression> recurse(
      BitSet edgeSubset, Set<String> requestedJoinNodes, Map<String, String> originalVarMap) {

    Set<String> localJoinNodes =
        JoinNodeUtils.localJoinNodes(edgeSubset, edges, requestedJoinNodes);

    RuleCacheKey key =
        new RuleCacheKey(
            BitsetUtils.signature(edgeSubset, edges.size()),
            localJoinNodes,
            edgeSubset.cardinality());

    List<CPQExpression> cached = ruleCache.get(key);
    if (cached != null) {
      return cached;
    }

    List<CPQExpression> expressions = new ArrayList<>();
    int edgeCount = edgeSubset.cardinality();

    if (edgeCount == 1) {
      expressions.addAll(
          SingleEdgeExpressionFactory.build(
              edges.get(edgeSubset.nextSetBit(0)), edgeSubset, originalVarMap));
    } else {
      if (localJoinNodes.size() <= 1) {
        expressions.addAll(
            LoopBacktrackBuilder.build(edges, edgeSubset, localJoinNodes, originalVarMap));
      }
      Function<BitSet, List<CPQExpression>> resolver =
          subset -> recurse(subset, requestedJoinNodes, originalVarMap);
      expressions.addAll(CompositeExpressionFactory.build(edgeSubset, edges.size(), resolver));
    }

    Map<ComponentKey, CPQExpression> unique = new LinkedHashMap<>();
    for (CPQExpression expression : expressions) {
      List<CPQExpression> variants = reverseLoopGenerator.generate(expression, originalVarMap);
      for (CPQExpression variant : variants) {
        ComponentKey compKey =
            new ComponentKey(variant.edges(), variant.source(), variant.target());
        unique.putIfAbsent(compKey, variant);
      }
    }

    List<CPQExpression> result = unique.isEmpty() ? List.of() : List.copyOf(unique.values());
    ruleCache.put(key, result);
    return result;
  }
}
