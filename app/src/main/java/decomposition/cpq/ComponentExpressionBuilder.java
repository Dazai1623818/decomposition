package decomposition.cpq;

import decomposition.core.model.Edge;
import decomposition.cpq.model.CacheStats.RuleCacheKey;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
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

    List<CPQExpression> cached = resolveCached(key);
    if (cached != null) {
      return cached;
    }

    int edgeCount = edgeSubset.cardinality();
    List<CPQExpression> expressions = new ArrayList<>();
    expressions.addAll(
        generateBaseExpressions(edgeSubset, edgeCount, localJoinNodes, originalVarMap));
    if (edgeCount > 1) {
      expressions.addAll(
          generateCompositeExpressions(edgeSubset, requestedJoinNodes, originalVarMap));
    }

    List<CPQExpression> expanded = expandLoopVariants(expressions, originalVarMap);
    List<CPQExpression> result = dedupeExpressions(expanded);
    ruleCache.put(key, result);
    return result;
  }

  private List<CPQExpression> resolveCached(RuleCacheKey key) {
    return ruleCache.get(key);
  }

  private List<CPQExpression> generateBaseExpressions(
      BitSet edgeSubset,
      int edgeCount,
      Set<String> localJoinNodes,
      Map<String, String> originalVarMap) {
    if (edgeCount == 1) {
      return SingleEdgeExpressionFactory.build(
          edges.get(edgeSubset.nextSetBit(0)), edgeSubset, originalVarMap);
    }
    List<CPQExpression> base = new ArrayList<>();
    if (localJoinNodes.size() <= 1) {
      base.addAll(LoopBacktrackBuilder.build(edges, edgeSubset, localJoinNodes, originalVarMap));
    }
    return base;
  }

  private List<CPQExpression> generateCompositeExpressions(
      BitSet edgeSubset, Set<String> requestedJoinNodes, Map<String, String> originalVarMap) {
    Function<BitSet, List<CPQExpression>> resolver =
        subset -> recurse(subset, requestedJoinNodes, originalVarMap);
    return CompositeExpressionFactory.build(edgeSubset, edges.size(), resolver);
  }

  private List<CPQExpression> expandLoopVariants(
      List<CPQExpression> expressions, Map<String, String> originalVarMap) {
    List<CPQExpression> expanded = new ArrayList<>();
    for (CPQExpression expression : expressions) {
      expanded.addAll(reverseLoopGenerator.generate(expression, originalVarMap));
    }
    return expanded;
  }

  public static List<CPQExpression> dedupeExpressions(List<CPQExpression> expressions) {
    if (expressions == null || expressions.isEmpty()) {
      return List.of();
    }
    Map<ExpressionKey, CPQExpression> unique = new LinkedHashMap<>();
    for (CPQExpression expression : expressions) {
      unique.putIfAbsent(keyOf(expression), expression);
    }
    return unique.isEmpty() ? List.of() : List.copyOf(unique.values());
  }

  private static ExpressionKey keyOf(CPQExpression expression) {
    Objects.requireNonNull(expression, "expression");
    return new ExpressionKey(expression.cpq(), expression.source(), expression.target());
  }

  private record ExpressionKey(CPQ cpq, String source, String target) {}
}
