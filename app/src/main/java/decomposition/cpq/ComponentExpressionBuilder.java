package decomposition.cpq;

import decomposition.core.model.Component;
import decomposition.core.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.QueryGraphCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Builds CPQ expressions for a single {@link Component}.
 *
 * <p>Pipeline (per component):
 *
 * <ol>
 *   <li>Generate base expressions:
 *       <ul>
 *         <li>Single-edge: forward / inverse / backtrack-loop variants.
 *         <li>Multi-edge (only when ≤1 join node): one “backtracking loop” expression that covers
 *             all edges.
 *       </ul>
 *   <li>Generate composite expressions by splitting the edge set into two non-empty parts and
 *       combining sub-expressions via concatenation or intersection.
 *   <li>Normalize loop expressions by intersecting with {@code id} when the CPQ isn't already a
 *       loop-shaped query graph.
 *   <li>Validate expressions against the underlying CQ edge set (structure/labels).
 *   <li>Dedupe by (source,target) within the same component.
 *   <li>Optionally enforce endpoint admissibility using {@link
 *       Component#endpointsAllowed(String,String)}.
 * </ol>
 */
public final class ComponentExpressionBuilder {

  private final List<Edge> edges;
  private final EdgeMatcher matcher;

  private record CacheKey(String signature, int diameterCap) {
    private CacheKey {
      Objects.requireNonNull(signature, "signature");
    }
  }

  /**
   * Within one recurse() call, all expressions cover the same edge set, so endpoints are enough.
   */
  private record EndpointKey(String source, String target) {
    private EndpointKey {
      Objects.requireNonNull(source, "source");
      Objects.requireNonNull(target, "target");
    }
  }

  private record Options(int diameterCap) {}

  public ComponentExpressionBuilder(List<Edge> edges) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
    this.matcher = new EdgeMatcher(this.edges);
  }

  public List<CPQExpression> build(Component component, int diameterCap, boolean firstHit) {
    Objects.requireNonNull(component, "component");
    if (component.edgeBits().isEmpty()) return List.of();
    Map<CacheKey, List<CPQExpression>> cache = new HashMap<>();
    List<CPQExpression> result = recurse(component, new Options(diameterCap), cache);
    result = result.stream().filter(e -> component.endpointsAllowed(e.source(), e.target())).toList();
    if (firstHit && !result.isEmpty()) {
      result = List.of(result.get(0));
    }
    return result;
  }

  private List<CPQExpression> recurse(
      Component component, Options o, Map<CacheKey, List<CPQExpression>> cache) {
    BitSet bits = component.edgeBits(); // clone (Component protects mutability)
    String signature = signature(bits);

    CacheKey key = new CacheKey(signature, o.diameterCap());
    List<CPQExpression> cached = cache.get(key);
    if (cached != null) return cached;

    List<CPQExpression> expressions = new ArrayList<>();
    if (component.edgeCount() == 1) {
      expressions.addAll(singleEdge(edges.get(bits.nextSetBit(0)), component, o));
    } else {
      if (component.joinNodes().size() <= 1) {
        expressions.addAll(loopBacktrack(component, bits, component.joinNodes(), o));
      }
      expressions.addAll(composites(component, bits, o, cache));
    }

    List<CPQExpression> result = dedupeByEndpoints(expressions);

    cache.put(key, result);
    return result;
  }

  // --------------------------------------------------------------------------
  // Single-edge base rules
  // --------------------------------------------------------------------------

  private List<CPQExpression> singleEdge(Edge edge, Component component, Options o) {
    List<CPQExpression> out = new ArrayList<>(3);

    // Forward atom
    emit(
        out,
        CPQ.label(edge.predicate()),
        component,
        edge.source(),
        edge.target(),
        "Forward atom on label '"
            + edge.label()
            + "' ("
            + edge.source()
            + "→"
            + edge.target()
            + ")",
        o);
    // Inverse atom (only when it isn't already a CQ self-loop)
    if (!edge.source().equals(edge.target())) {
      emit(
          out,
          CPQ.label(edge.predicate().getInverse()),
          component,
          edge.target(),
          edge.source(),
          "Inverse atom on label '"
              + edge.label()
              + "' ("
              + edge.target()
              + "→"
              + edge.source()
              + ")",
          o);
    }

    // Backtracking loops (only when CQ edge is not a self-loop)
    if (!edge.source().equals(edge.target())) {
      CPQ forward = CPQ.label(edge.predicate());
      CPQ inverse = CPQ.label(edge.predicate().getInverse());

      CPQ sourceLoop = CPQ.intersect(List.of(CPQ.concat(List.of(forward, inverse)), CPQ.id()));
      emit(
          out,
          sourceLoop,
          component,
          edge.source(),
          edge.source(),
          "Backtrack loop via '" + edge.label() + "' at " + edge.source(),
          o);
      CPQ targetLoop = CPQ.intersect(List.of(CPQ.concat(List.of(inverse, forward)), CPQ.id()));
      emit(
          out,
          targetLoop,
          component,
          edge.target(),
          edge.target(),
          "Backtrack loop via '" + edge.label() + "' at " + edge.target(),
          o);
    }

    return out;
  }

  private void emit(
      List<CPQExpression> out,
      CPQ cpq,
      Component component,
      String source,
      String target,
      String derivation,
      Options o) {
    if (o.diameterCap() > 0 && cpq.getDiameter() > o.diameterCap()) return;
    CPQExpression candidate = new CPQExpression(cpq, component, source, target, derivation);
    CPQExpression looped = makeLoop(candidate, o);
    if (looped == null) {
      return;
    }
    if (!matcher.isValid(looped)) {
      return;
    }
    out.add(looped);
  }

  // --------------------------------------------------------------------------
  // Composite rules (split + concat / intersect)
  // --------------------------------------------------------------------------

  private List<CPQExpression> composites(
      Component owner, BitSet ownerBits, Options o, Map<CacheKey, List<CPQExpression>> cache) {
    Function<BitSet, List<CPQExpression>> buildSub =
        subset -> recurse(owner.restrictTo(subset, edges), o, cache);

    List<CPQExpression> out = new ArrayList<>();
    forEachSplit(
        ownerBits,
        edges.size(),
        (a, b) -> {
          List<CPQExpression> left = buildSub.apply(a);
          if (left.isEmpty()) return;
          List<CPQExpression> right = buildSub.apply(b);
          if (right.isEmpty()) return;

          for (CPQExpression lhs : left) {
            for (CPQExpression rhs : right) {
              // Concatenation: lhs.target == rhs.source
              if (lhs.target().equals(rhs.source())) {
                CPQ cpq = CPQ.concat(List.of(lhs.cpq(), rhs.cpq()));
	                emit(
	                    out,
	                    cpq,
	                    owner,
	                    lhs.source(),
	                    rhs.target(),
	                    "Concatenation: ["
	                        + lhs.cpq()
	                        + "] then ["
	                        + rhs.cpq()
	                        + "] via "
	                        + lhs.target(),
	                    o);
              }

              // Intersection: same endpoints
              if (lhs.source().equals(rhs.source()) && lhs.target().equals(rhs.target())) {
                CPQ cpq = CPQ.intersect(List.of(lhs.cpq(), rhs.cpq()));
	                emit(
	                    out,
	                    cpq,
	                    owner,
	                    lhs.source(),
	                    lhs.target(),
	                    "Intersection: ["
	                        + lhs.cpq()
	                        + "] ∩ ["
	                        + rhs.cpq()
	                        + "] at "
	                        + lhs.source()
	                        + "→"
	                        + lhs.target(),
	                    o);
              }
            }
          }
        });
    return out;
  }

  private static void forEachSplit(
      BitSet bits, int totalEdgeCount, BiConsumer<BitSet, BitSet> visitor) {
    List<Integer> idx = new ArrayList<>();
    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) idx.add(i);
    int k = idx.size();
    int combos = 1 << k;

    for (int mask = 1; mask < combos - 1; mask++) {
      BitSet a = new BitSet(totalEdgeCount);
      for (int i = 0; i < k; i++) if ((mask & (1 << i)) != 0) a.set(idx.get(i));

      BitSet b = (BitSet) bits.clone();
      b.andNot(a);
      visitor.accept(a, b);
    }
  }

  // --------------------------------------------------------------------------
  // Loop anchoring (the only “variant” currently used)
  // --------------------------------------------------------------------------

  private static CPQExpression makeLoop(CPQExpression e, Options o) {
    if (!e.source().equals(e.target())) return e;

    try {
      if (e.cpq().toQueryGraph().isLoop()) return e;

      CPQ anchored = CPQ.intersect(e.cpq(), CPQ.IDENTITY);
      if (o.diameterCap() > 0 && anchored.getDiameter() > o.diameterCap()) return null;

      return new CPQExpression(
          anchored, e.component(), e.source(), e.target(), e.derivation() + " + anchored with id");
    } catch (RuntimeException ignored) {
      return e;
    }
  }

  // --------------------------------------------------------------------------
  // Multi-edge base rule: cover the component via backtracking loop
  // --------------------------------------------------------------------------

  private List<CPQExpression> loopBacktrack(
      Component component, BitSet edgeBits, Set<String> allowedAnchors, Options o) {

    Map<String, List<AdjacencyEdge>> adj = buildAdjacency(edgeBits);
    if (adj.isEmpty()) return List.of();

    List<CPQExpression> out = new ArrayList<>();
    for (String anchor : adj.keySet()) {
      if (allowedAnchors != null && !allowedAnchors.isEmpty() && !allowedAnchors.contains(anchor)) {
        continue;
      }

      BitSet visited = new BitSet(edgeBits.length());
      CPQ body = loopForVertex(anchor, adj, visited, o.diameterCap());
      if (visited.cardinality() != edgeBits.cardinality()) continue;

      CPQ loop = CPQ.intersect(List.of(body, CPQ.id()));
      emit(
          out,
          loop,
          component,
          anchor,
          anchor,
          "Loop via backtracking anchored at '" + anchor + "'",
          o);

    }
    return out;
  }

  private Map<String, List<AdjacencyEdge>> buildAdjacency(BitSet bits) {
    Map<String, List<AdjacencyEdge>> adj = new LinkedHashMap<>();
    for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx + 1)) {
      Edge e = edges.get(idx);
      adj.computeIfAbsent(e.source(), k -> new ArrayList<>()).add(new AdjacencyEdge(idx, e));
      adj.computeIfAbsent(e.target(), k -> new ArrayList<>()).add(new AdjacencyEdge(idx, e));
    }
    return adj;
  }

  private CPQ loopForVertex(
      String current, Map<String, List<AdjacencyEdge>> adj, BitSet visited, int diameterCap) {

    List<CPQ> segments = new ArrayList<>();

    for (AdjacencyEdge e : adj.getOrDefault(current, List.of())) {
      if (visited.get(e.index())) continue;
      visited.set(e.index());

      CPQ segment;
      if (e.isSelfLoop()) {
        segment = CPQ.intersect(List.of(e.stepFrom(current), CPQ.id()));
      } else {
        String next = e.other(current);
        CPQ forward = e.stepFrom(current);
        CPQ nested = loopForVertex(next, adj, visited, diameterCap);
        CPQ backward = e.stepFrom(next);

        List<CPQ> path = new ArrayList<>(3);
        path.add(forward);
        if (!nested.equals(CPQ.IDENTITY)) path.add(nested);
        path.add(backward);

        CPQ concat = path.size() == 1 ? path.get(0) : CPQ.concat(path);
        segment = CPQ.intersect(List.of(concat, CPQ.id()));
      }

      if (diameterCap > 0 && segment.getDiameter() > diameterCap) {
        visited.clear(e.index());
        continue;
      }
      segments.add(segment);
    }

    if (segments.isEmpty()) return CPQ.id();
    return segments.size() == 1 ? segments.get(0) : CPQ.concat(segments);
  }

  private record AdjacencyEdge(int index, Edge edge) {
    String other(String vertex) {
      return edge.source().equals(vertex) ? edge.target() : edge.source();
    }

    boolean isSelfLoop() {
      return edge.source().equals(edge.target());
    }

    CPQ stepFrom(String vertex) {
      return edge.source().equals(vertex)
          ? CPQ.label(edge.predicate())
          : CPQ.label(edge.predicate().getInverse());
    }
  }

  // --------------------------------------------------------------------------
  // Dedupe
  // --------------------------------------------------------------------------

  private static List<CPQExpression> dedupeByEndpoints(List<CPQExpression> expressions) {
    if (expressions == null || expressions.isEmpty()) return List.of();

    Map<EndpointKey, CPQExpression> unique = new LinkedHashMap<>();
    for (CPQExpression e : expressions)
      unique.putIfAbsent(new EndpointKey(e.source(), e.target()), e);
    return unique.isEmpty() ? List.of() : List.copyOf(unique.values());
  }

  /**
   * Public deduper (kept for compatibility). Uses (edge-set signature, source, target) as the key.
   * Within one component, the signature is constant, so this reduces to endpoint dedupe.
   */
	  public static List<CPQExpression> dedupeExpressions(List<CPQExpression> expressions) {
	    if (expressions == null || expressions.isEmpty()) return List.of();

	    Map<String, CPQExpression> unique = new LinkedHashMap<>();
	    for (CPQExpression e : expressions) {
	      String k = signature(e.component().edgeBits()) + "|" + e.source() + "|" + e.target();
	      unique.putIfAbsent(k, e);
	    }
	    return unique.isEmpty() ? List.of() : List.copyOf(unique.values());
	  }

  // --------------------------------------------------------------------------
  // Signature (stable for an edge subset)
  // --------------------------------------------------------------------------

  private static String signature(BitSet bits) {
    StringBuilder sb = new StringBuilder().append('[');
    boolean first = true;
    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      if (!first) sb.append(',');
      sb.append(i);
      first = false;
    }
    return sb.append(']').toString();
  }

  // --------------------------------------------------------------------------
  // Edge-set validity check (only used to reject invalid loop-normalized variants)
  // --------------------------------------------------------------------------

  /**
   * Matches a {@link CPQExpression}'s CQ/CPQ structure to concrete decomposition edges.
   *
   * <p>We keep this check local to the builder because it's only used to ensure "loop anchored with
   * id" does not accidentally create a CPQ that no longer corresponds to the original component
   * edge set.
   */
  private static final class EdgeMatcher {
    private final List<Edge> allEdges;

    EdgeMatcher(List<Edge> allEdges) {
      this.allEdges = List.copyOf(Objects.requireNonNull(allEdges, "allEdges"));
    }

	    boolean isValid(CPQExpression rule) {
	      BitSet edgeBits = rule.component().edgeBits();

      List<Edge> componentEdges = new ArrayList<>(edgeBits.cardinality());
      Set<String> vertices = new LinkedHashSet<>();
      for (int idx = edgeBits.nextSetBit(0); idx >= 0; idx = edgeBits.nextSetBit(idx + 1)) {
        Edge e = allEdges.get(idx);
        componentEdges.add(e);
        vertices.add(e.source());
        vertices.add(e.target());
      }
      if (componentEdges.isEmpty()) return false;
      if (!vertices.contains(rule.source()) || !vertices.contains(rule.target())) return false;

      List<GraphEdge<VarCQ, AtomCQ>> cpqEdges;
      QueryGraphCPQ cpqGraph;
      String sourceVar;
      String targetVar;
      try {
        CPQ cpq = rule.cpq();
        CQ cqPattern = cpq.toCQ();
        QueryGraphCQ cqGraph = cqPattern.toQueryGraph();
        UniqueGraph<VarCQ, AtomCQ> graph = cqGraph.toUniqueGraph();
        cpqEdges = graph.getEdges();
        cpqGraph = cpq.toQueryGraph();
        sourceVar = cpqGraph.getVertexLabel(cpqGraph.getSourceVertex());
        targetVar = cpqGraph.getVertexLabel(cpqGraph.getTargetVertex());
        if (cpqEdges.size() != componentEdges.size()) return false;
      } catch (RuntimeException ex) {
        return false;
      }

      boolean ruleIsLoop = rule.source().equals(rule.target());
      if (cpqGraph.isLoop() != ruleIsLoop) return false;

      return matchEdges(
          cpqEdges, componentEdges, sourceVar, targetVar, rule.source(), rule.target());
    }

    private boolean matchEdges(
        List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
        List<Edge> componentEdges,
        String sourceVar,
        String targetVar,
        String sourceNode,
        String targetNode) {

      Map<String, String> mapping = new HashMap<>();
      Set<String> usedNodes = new HashSet<>();
      mapping.put(sourceVar, sourceNode);
      mapping.put(targetVar, targetNode);
      usedNodes.add(sourceNode);
      usedNodes.add(targetNode);

      Deque<State> stack = new ArrayDeque<>();
      stack.push(new State(0, new BitSet(componentEdges.size()), mapping, usedNodes));

      while (!stack.isEmpty()) {
        State s = stack.pop();

        if (s.i == cpqEdges.size()) {
          if (s.usedEdges.cardinality() == componentEdges.size()
              && sourceNode.equals(s.map.get(sourceVar))
              && targetNode.equals(s.map.get(targetVar))) {
            return true;
          }
          continue;
        }

        GraphEdge<VarCQ, AtomCQ> cpqEdge = cpqEdges.get(s.i);
        AtomCQ atom = cpqEdge.getData();
        String label = atom.getLabel().getAlias();
        String cpqSrc = cpqEdge.getSourceNode().getData().getName();
        String cpqTrg = cpqEdge.getTargetNode().getData().getName();

        for (int eIdx = 0; eIdx < componentEdges.size(); eIdx++) {
          if (s.usedEdges.get(eIdx)) continue;

          Edge ce = componentEdges.get(eIdx);
          if (!label.equals(ce.label())) continue;

          String mappedSrc = s.map.get(cpqSrc);
          String mappedTrg = s.map.get(cpqTrg);

          if (mappedSrc != null && !mappedSrc.equals(ce.source())) continue;
          if (mappedTrg != null && !mappedTrg.equals(ce.target())) continue;
          if (mappedSrc == null && s.usedNodes.contains(ce.source())) continue;
          if (mappedTrg == null && s.usedNodes.contains(ce.target())) continue;

          Map<String, String> nextMap = new HashMap<>(s.map);
          Set<String> nextUsedNodes = new HashSet<>(s.usedNodes);

          if (mappedSrc == null) {
            nextMap.put(cpqSrc, ce.source());
            nextUsedNodes.add(ce.source());
          }
          if (mappedTrg == null) {
            nextMap.put(cpqTrg, ce.target());
            nextUsedNodes.add(ce.target());
          }

          BitSet nextUsedEdges = (BitSet) s.usedEdges.clone();
          nextUsedEdges.set(eIdx);
          stack.push(new State(s.i + 1, nextUsedEdges, nextMap, nextUsedNodes));
        }
      }

      return false;
    }

    private static final class State {
      final int i;
      final BitSet usedEdges;
      final Map<String, String> map;
      final Set<String> usedNodes;

      State(int i, BitSet usedEdges, Map<String, String> map, Set<String> usedNodes) {
        this.i = i;
        this.usedEdges = usedEdges;
        this.map = map;
        this.usedNodes = usedNodes;
      }
    }
  }
}
