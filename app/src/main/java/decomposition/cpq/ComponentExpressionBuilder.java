package decomposition.cpq;

import decomposition.core.model.Component;
import decomposition.core.model.Edge;
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
  private final ComponentEdgeMatcher loopVariantValidator;
  private final Map<RuleCacheKey, List<CPQExpression>> ruleCache = new ConcurrentHashMap<>();

  private record RuleCacheKey(
      String signature,
      Set<String> joinNodes,
      int edgeCount,
      int diameterCap,
      boolean firstHit,
      boolean enforceEndpointRoles) {
    private RuleCacheKey {
      Objects.requireNonNull(signature, "signature");
      joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
    }
  }

  private record ExpressionKey(String signature, String source, String target) {
    private ExpressionKey {
      Objects.requireNonNull(signature, "signature");
      Objects.requireNonNull(source, "source");
      Objects.requireNonNull(target, "target");
    }
  }

  public ComponentExpressionBuilder(List<Edge> edges) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
    this.loopVariantValidator = new ComponentEdgeMatcher(this.edges);
  }

  public List<CPQExpression> build(Component component, int diameterCap, boolean firstHit) {
    Objects.requireNonNull(component, "component");
    if (component.edgeBits().isEmpty()) {
      return List.of();
    }
    return recurse(component, diameterCap, firstHit, true);
  }

  private List<CPQExpression> recurse(
      Component component, int diameterCap, boolean firstHit, boolean enforceEndpointRoles) {

    Set<String> localJoinNodes = component.joinNodes();
    BitSet edgeSubset = component.edgeBits();

    RuleCacheKey key =
        new RuleCacheKey(
            edgeSignature(edgeSubset),
            localJoinNodes,
            edgeSubset.cardinality(),
            diameterCap,
            firstHit,
            enforceEndpointRoles);

    List<CPQExpression> cached = ruleCache.get(key);
    if (cached != null) {
      return cached;
    }

    int edgeCount = component.edgeCount();
    List<CPQExpression> expressions = new ArrayList<>();
    if (edgeCount == 1) {
      expressions.addAll(
          buildSingleEdgeExpressions(
              edges.get(edgeSubset.nextSetBit(0)), component, diameterCap, firstHit));
    } else {
      if (localJoinNodes.size() <= 1) {
        expressions.addAll(
            buildLoopBacktrack(component, edgeSubset, localJoinNodes, diameterCap, firstHit));
      }
      Function<BitSet, List<CPQExpression>> resolver =
          subset -> recurse(component.restrictTo(subset, edges), diameterCap, firstHit, false);
      expressions.addAll(
          buildCompositeExpressions(
              component, edgeSubset, edges.size(), resolver, diameterCap, firstHit));
    }

    List<CPQExpression> expanded = expandLoopVariants(expressions, diameterCap);
    List<CPQExpression> result = dedupeExpressions(expanded);
    if (enforceEndpointRoles) {
      result =
          result.stream()
              .filter(rule -> component.endpointsAllowed(rule.source(), rule.target()))
              .toList();
    }
    if (firstHit && !result.isEmpty()) {
      result = List.of(result.get(0));
    }
    ruleCache.put(key, result);
    return result;
  }

  private List<CPQExpression> expandLoopVariants(List<CPQExpression> expressions, int diameterCap) {
    List<CPQExpression> expanded = new ArrayList<>();
    for (CPQExpression expression : expressions) {
      if (diameterCap > 0 && expression.cpq().getDiameter() > diameterCap) {
        continue;
      }
      expanded.addAll(generateLoopVariants(expression));
    }
    return expanded;
  }

  private List<CPQExpression> generateLoopVariants(CPQExpression rule) {
    CPQExpression candidate = rule;
    if (rule.source().equals(rule.target())) {
      try {
        if (!rule.cpq().toQueryGraph().isLoop()) {
          CPQ anchoredCpq = CPQ.intersect(rule.cpq(), CPQ.IDENTITY);
          candidate =
              new CPQExpression(
                  anchoredCpq,
                  rule.component(),
                  rule.source(),
                  rule.target(),
                  rule.derivation() + " + anchored with id");
        }
      } catch (RuntimeException ignored) {
        // Keep original if graph extraction fails
      }
    }

    return loopVariantValidator.isValid(candidate) ? List.of(candidate) : List.of();
  }

  private static String edgeSignature(BitSet bitSet) {
    Objects.requireNonNull(bitSet, "bitSet");
    StringBuilder builder = new StringBuilder();
    builder.append('[');
    boolean first = true;
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      if (!first) {
        builder.append(',');
      }
      builder.append(i);
      first = false;
    }
    return builder.append(']').toString();
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
    return new ExpressionKey(
        edgeSignature(expression.edges()), expression.source(), expression.target());
  }

  // ===== Inlined from SingleEdgeExpressionFactory =====

  /** Generates the CPQ expressions that arise from a single CQ edge. */
  private List<CPQExpression> buildSingleEdgeExpressions(
      Edge edge, Component component, int diameterCap, boolean firstHit) {
    List<CPQExpression> expressions = new ArrayList<>();

    addForwardExpression(edge, component, expressions, diameterCap, firstHit);
    if (!firstHit || expressions.isEmpty()) {
      addInverseExpression(edge, component, expressions, diameterCap, firstHit);
    }
    if (!firstHit || expressions.isEmpty()) {
      addBacktrackExpressions(edge, component, expressions, diameterCap, firstHit);
    }

    return expressions;
  }

  private void addForwardExpression(
      Edge edge, Component component, List<CPQExpression> out, int diameterCap, boolean firstHit) {
    CPQ forward = CPQ.label(edge.predicate());
    if (diameterCap > 0 && forward.getDiameter() > diameterCap) {
      return;
    }
    out.add(
        new CPQExpression(
            forward,
            component,
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

  private void addInverseExpression(
      Edge edge, Component component, List<CPQExpression> out, int diameterCap, boolean firstHit) {
    if (edge.source().equals(edge.target())) {
      return;
    }
    CPQ inverse = CPQ.label(edge.predicate().getInverse());
    if (diameterCap > 0 && inverse.getDiameter() > diameterCap) {
      return;
    }
    out.add(
        new CPQExpression(
            inverse,
            component,
            edge.target(),
            edge.source(),
            "Inverse atom on label '"
                + edge.label()
                + "' ("
                + edge.target()
                + "→"
                + edge.source()
                + ")"));
  }

  private void addBacktrackExpressions(
      Edge edge, Component component, List<CPQExpression> out, int diameterCap, boolean firstHit) {
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

    CPQ sourceLoop = CPQ.intersect(List.of(sourceLoopBody, CPQ.id()));
    if (diameterCap <= 0 || sourceLoop.getDiameter() <= diameterCap) {
      addLoopExpression(
          out,
          sourceLoop,
          component,
          edge.source(),
          "Backtrack loop via '" + edge.label() + "' at " + edge.source());
    }
    if (firstHit && !out.isEmpty()) {
      return;
    }

    CPQ targetLoop = CPQ.intersect(List.of(targetLoopBody, CPQ.id()));
    if (diameterCap <= 0 || targetLoop.getDiameter() <= diameterCap) {
      addLoopExpression(
          out,
          targetLoop,
          component,
          edge.target(),
          "Backtrack loop via '" + edge.label() + "' at " + edge.target());
    }
  }

  private void addLoopExpression(
      List<CPQExpression> out, CPQ cpq, Component component, String anchor, String derivation) {
    out.add(new CPQExpression(cpq, component, anchor, anchor, derivation));
  }

  // ===== Inlined from CompositeExpressionFactory =====

  /**
   * Generates composite CPQ expressions by joining subcomponents via concatenation or intersection.
   */
  private List<CPQExpression> buildCompositeExpressions(
      Component component,
      BitSet edgeBits,
      int totalEdgeCount,
      Function<BitSet, List<CPQExpression>> constructionRuleLookup,
      int diameterCap,
      boolean firstHit) {
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
              tryConcat(component, lhs, rhs, results, diameterCap);
              if (firstHit && !results.isEmpty()) {
                return;
              }
              tryIntersect(component, lhs, rhs, results, diameterCap);
              if (firstHit && !results.isEmpty()) {
                return;
              }
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
    List<Integer> indices = toIndexList(edgeBits);
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

  private static List<Integer> toIndexList(BitSet bitSet) {
    Objects.requireNonNull(bitSet, "bitSet");
    List<Integer> indices = new ArrayList<>();
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      indices.add(i);
    }
    return indices;
  }

  private void tryConcat(
      Component component,
      CPQExpression left,
      CPQExpression right,
      List<CPQExpression> sink,
      int diameterCap) {
    if (!left.target().equals(right.source())) {
      return;
    }
    if (!left.varToNodeMap().equals(right.varToNodeMap())) {
      return;
    }
    CPQ concatenated = CPQ.concat(List.of(left.cpq(), right.cpq()));
    if (diameterCap > 0 && concatenated.getDiameter() > diameterCap) {
      return;
    }
    String derivation =
        "Concatenation: ["
            + left.cpqRule()
            + "] then ["
            + right.cpqRule()
            + "] via "
            + left.target();
    emitCompositeExpression(
        component, concatenated, left.source(), right.target(), derivation, sink, diameterCap);
  }

  private void tryIntersect(
      Component component,
      CPQExpression left,
      CPQExpression right,
      List<CPQExpression> sink,
      int diameterCap) {
    if (!left.source().equals(right.source()) || !left.target().equals(right.target())) {
      return;
    }
    if (!left.varToNodeMap().equals(right.varToNodeMap())) {
      return;
    }
    CPQ intersection = CPQ.intersect(List.of(left.cpq(), right.cpq()));
    if (diameterCap > 0 && intersection.getDiameter() > diameterCap) {
      return;
    }
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
        component, intersection, left.source(), left.target(), derivation, sink, diameterCap);
  }

  private void emitCompositeExpression(
      Component component,
      CPQ cpq,
      String source,
      String target,
      String derivation,
      List<CPQExpression> sink,
      int diameterCap) {
    if (diameterCap > 0 && cpq.getDiameter() > diameterCap) {
      return;
    }
    sink.add(new CPQExpression(cpq, component, source, target, derivation));
  }

  // ===== Inlined from LoopBacktrackBuilder =====

  /** Synthesizes loop-shaped CPQs that cover every edge in a component via backtracking. */
  private List<CPQExpression> buildLoopBacktrack(
      Component component,
      BitSet edgeBits,
      Set<String> allowedAnchors,
      int diameterCap,
      boolean firstHit) {
    Map<String, List<AdjacencyEdge>> adjacency = buildAdjacency(edgeBits);
    if (adjacency.isEmpty()) {
      return List.of();
    }

    List<CPQExpression> results = new ArrayList<>();
    for (String anchor : adjacency.keySet()) {
      if (!isAnchorAllowed(anchor, allowedAnchors)) {
        continue;
      }
      BitSet visited = new BitSet(edgeBits.length());
      CPQ loopCpq = buildLoop(anchor, adjacency, visited, diameterCap);
      if (loopCpq == null) {
        continue;
      }
      results.add(
          new CPQExpression(
              loopCpq,
              component,
              anchor,
              anchor,
              "Loop via backtracking anchored at '" + anchor + "'"));
      if (firstHit && !results.isEmpty()) {
        break;
      }
    }
    return results;
  }

  private boolean isAnchorAllowed(String anchor, Set<String> allowedAnchors) {
    return allowedAnchors == null || allowedAnchors.isEmpty() || allowedAnchors.contains(anchor);
  }

  private Map<String, List<AdjacencyEdge>> buildAdjacency(BitSet bits) {
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

  private CPQ buildLoop(
      String anchor, Map<String, List<AdjacencyEdge>> adjacency, BitSet visited, int diameterCap) {
    CPQ loopBody = loopForVertex(anchor, adjacency, visited, diameterCap);
    if (loopBody == null) {
      return null;
    }
    return CPQ.intersect(List.of(loopBody, CPQ.id()));
  }

  private CPQ loopForVertex(
      String current, Map<String, List<AdjacencyEdge>> adjacency, BitSet visited, int diameterCap) {
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
        CPQ nested = loopForVertex(neighbor, adjacency, visited, diameterCap);
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
      if (diameterCap > 0 && segment.getDiameter() > diameterCap) {
        visited.clear(edge.index());
        continue;
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
