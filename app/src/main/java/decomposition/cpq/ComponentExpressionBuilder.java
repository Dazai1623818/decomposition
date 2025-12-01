package decomposition.cpq;

import decomposition.core.model.Edge;
import decomposition.cpq.model.CacheStats.ComponentKey;
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
import java.util.function.BiConsumer;
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
      return buildSingleEdgeExpressions(
          edges.get(edgeSubset.nextSetBit(0)), edgeSubset, originalVarMap);
    }
    List<CPQExpression> base = new ArrayList<>();
    if (localJoinNodes.size() <= 1) {
      base.addAll(buildLoopBacktrack(edges, edgeSubset, localJoinNodes, originalVarMap));
    }
    return base;
  }

  private List<CPQExpression> generateCompositeExpressions(
      BitSet edgeSubset, Set<String> requestedJoinNodes, Map<String, String> originalVarMap) {
    Function<BitSet, List<CPQExpression>> resolver =
        subset -> recurse(subset, requestedJoinNodes, originalVarMap);
    return buildCompositeExpressions(edgeSubset, edges.size(), resolver);
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
    Map<ComponentKey, CPQExpression> unique = new LinkedHashMap<>();
    for (CPQExpression expression : expressions) {
      unique.putIfAbsent(keyOf(expression), expression);
    }
    return unique.isEmpty() ? List.of() : List.copyOf(unique.values());
  }

  private static ComponentKey keyOf(CPQExpression expression) {
    Objects.requireNonNull(expression, "expression");
    return new ComponentKey(expression.edges(), expression.source(), expression.target());
  }

  // ===== Inlined from SingleEdgeExpressionFactory =====

  /** Generates the CPQ expressions that arise from a single CQ edge. */
  private List<CPQExpression> buildSingleEdgeExpressions(
      Edge edge, BitSet edgeBits, Map<String, String> varToNodeMap) {
    List<CPQExpression> expressions = new ArrayList<>();

    addForwardExpression(edge, edgeBits, varToNodeMap, expressions);
    addInverseExpression(edge, edgeBits, varToNodeMap, expressions);
    addBacktrackExpressions(edge, edgeBits, varToNodeMap, expressions);

    return expressions;
  }

  private void addForwardExpression(
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

  private void addInverseExpression(
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

  private void addBacktrackExpressions(
      Edge edge, BitSet bits, Map<String, String> varToNodeMap, List<CPQExpression> out) {
    if (edge.source().equals(edge.target())) {
      // CQ already a true self-loop (single vertex, src==target); no extra
      // backtracking variants
      // needed.
      return;
    }
    // Backtracking loops revisit the start vertex via an inverse step, so the
    // resulting CPQ still
    // has src==target
    // even though the underlying CQ edge connects two distinct nodes.
    CPQ forward = CPQ.label(edge.predicate());
    CPQ inverse = CPQ.label(edge.predicate().getInverse());
    CPQ sourceLoopBody = CPQ.concat(List.of(forward, inverse));
    CPQ targetLoopBody = CPQ.concat(List.of(inverse, forward));

    addLoopExpression(
        out,
        CPQ.intersect(List.of(sourceLoopBody, CPQ.id())),
        bits,
        edge.source(),
        "Backtrack loop via '" + edge.label() + "' at " + edge.source(),
        varToNodeMap);

    addLoopExpression(
        out,
        CPQ.intersect(List.of(targetLoopBody, CPQ.id())),
        bits,
        edge.target(),
        "Backtrack loop via '" + edge.label() + "' at " + edge.target(),
        varToNodeMap);
  }

  private void addLoopExpression(
      List<CPQExpression> out,
      CPQ cpq,
      BitSet bits,
      String anchor,
      String derivation,
      Map<String, String> varToNodeMap) {
    out.add(new CPQExpression(cpq, bits, anchor, anchor, derivation, varToNodeMap));
  }

  // ===== Inlined from CompositeExpressionFactory =====

  /**
   * Generates composite CPQ expressions by joining subcomponents via concatenation or intersection.
   */
  private List<CPQExpression> buildCompositeExpressions(
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
   * Enumerates all two-way partitions of the given bitset (excluding empty/full subsets) and hands
   * each pair to the visitor.
   */
  private void forEachSplit(
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

  private void tryConcat(
      BitSet edgeBits, CPQExpression left, CPQExpression right, List<CPQExpression> sink) {
    if (!left.target().equals(right.source())) {
      return;
    }
    if (!left.varToNodeMap().equals(right.varToNodeMap())) {
      return;
    }
    CPQ concatenated = CPQ.concat(List.of(left.cpq(), right.cpq()));
    String derivation =
        "Concatenation: ["
            + left.cpqRule()
            + "] then ["
            + right.cpqRule()
            + "] via "
            + left.target();
    emitCompositeExpression(
        edgeBits,
        concatenated,
        left.source(),
        right.target(),
        derivation,
        left.varToNodeMap(),
        sink);
  }

  private void tryIntersect(
      BitSet edgeBits, CPQExpression left, CPQExpression right, List<CPQExpression> sink) {
    if (!left.source().equals(right.source()) || !left.target().equals(right.target())) {
      return;
    }
    if (!left.varToNodeMap().equals(right.varToNodeMap())) {
      return;
    }
    CPQ intersection = CPQ.intersect(List.of(left.cpq(), right.cpq()));
    String derivation =
        "Intersection: ["
            + left.cpqRule()
            + "] ∩ ["
            + right.cpqRule()
            + "] at "
            + left.source()
            + "→"
            + left.target();
    emitCompositeExpression(
        edgeBits,
        intersection,
        left.source(),
        left.target(),
        derivation,
        left.varToNodeMap(),
        sink);
  }

  private void emitCompositeExpression(
      BitSet edgeBits,
      CPQ cpq,
      String source,
      String target,
      String derivation,
      Map<String, String> varToNodeMap,
      List<CPQExpression> sink) {
    sink.add(new CPQExpression(cpq, edgeBits, source, target, derivation, varToNodeMap));
  }

  // ===== Inlined from LoopBacktrackBuilder =====

  /** Synthesizes loop-shaped CPQs that cover every edge in a component via backtracking. */
  private List<CPQExpression> buildLoopBacktrack(
      List<Edge> edges,
      BitSet edgeBits,
      Set<String> allowedAnchors,
      Map<String, String> varToNodeMap) {
    Map<String, List<AdjacencyEdge>> adjacency = buildAdjacency(edges, edgeBits);
    if (adjacency.isEmpty()) {
      return List.of();
    }

    List<CPQExpression> results = new ArrayList<>();
    for (String anchor : adjacency.keySet()) {
      if (!isAnchorAllowed(anchor, allowedAnchors)) {
        continue;
      }
      BitSet visited = new BitSet(edgeBits.length());
      CPQ loopCpq = buildLoop(anchor, adjacency, visited);
      if (loopCpq == null) {
        continue;
      }
      results.add(
          new CPQExpression(
              loopCpq,
              edgeBits,
              anchor,
              anchor,
              "Loop via backtracking anchored at '" + anchor + "'",
              varToNodeMap));
    }
    return results;
  }

  private boolean isAnchorAllowed(String anchor, Set<String> allowedAnchors) {
    return allowedAnchors == null || allowedAnchors.isEmpty() || allowedAnchors.contains(anchor);
  }

  private Map<String, List<AdjacencyEdge>> buildAdjacency(List<Edge> edges, BitSet bits) {
    Map<String, List<AdjacencyEdge>> adjacency = new LinkedHashMap<>();
    for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx + 1)) {
      Edge edge = edges.get(idx);
      adjacency
          .computeIfAbsent(edge.source(), k -> new ArrayList<>())
          .add(new AdjacencyEdge(idx, edge));
      adjacency
          .computeIfAbsent(edge.target(), k -> new ArrayList<>())
          .add(new AdjacencyEdge(idx, edge));
    }
    return adjacency;
  }

  private CPQ buildLoop(String anchor, Map<String, List<AdjacencyEdge>> adjacency, BitSet visited) {
    CPQ loopBody = loopForVertex(anchor, adjacency, visited);
    if (loopBody == null) {
      return null;
    }
    return CPQ.intersect(List.of(loopBody, CPQ.id()));
  }

  private CPQ loopForVertex(
      String current, Map<String, List<AdjacencyEdge>> adjacency, BitSet visited) {
    List<CPQ> segments = new ArrayList<>();
    for (AdjacencyEdge edge : adjacency.getOrDefault(current, List.of())) {
      String neighbor = edge.other(current);
      if (visited.get(edge.index())) {
        continue;
      }
      visited.set(edge.index());

      CPQ segment;
      if (edge.isSelfLoop()) {
        segment = CPQ.intersect(List.of(edge.forwardCpq(current), CPQ.id()));
      } else {
        CPQ forward = edge.forwardCpq(current);
        CPQ nested = loopForVertex(neighbor, adjacency, visited);
        CPQ backward = edge.forwardCpq(neighbor);
        List<CPQ> path = new ArrayList<>();
        path.add(forward);
        if (nested != null && nested != CPQ.id()) {
          path.add(nested);
        }
        path.add(backward);
        CPQ concat = path.size() == 1 ? path.get(0) : CPQ.concat(path);
        segment = CPQ.intersect(List.of(concat, CPQ.id()));
      }
      segments.add(segment);
    }

    if (segments.isEmpty()) {
      return CPQ.id();
    }
    return segments.size() == 1 ? segments.get(0) : CPQ.concat(segments);
  }

  private record AdjacencyEdge(int index, Edge edge) {
    String other(String vertex) {
      return edge.source().equals(vertex) ? edge.target() : edge.source();
    }

    boolean isSelfLoop() {
      return edge.source().equals(edge.target());
    }

    CPQ forwardCpq(String vertex) {
      if (edge.source().equals(vertex)) {
        return CPQ.label(edge.predicate());
      }
      return CPQ.label(edge.predicate().getInverse());
    }
  }
}
