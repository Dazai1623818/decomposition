package decomposition.cpq;

import decomposition.core.Component;
import decomposition.core.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 *   <li>Optionally enforce endpoint admissibility using {@link
 *       Component#endpointsAllowed(String,String)}.
 * </ol>
 */
public final class ComponentExpressionBuilder {

  private final List<Edge> edges;

  private record CacheKey(String signature, int diameterCap) {
    private CacheKey {
      Objects.requireNonNull(signature, "signature");
    }
  }

  private record Options(int diameterCap) {}

  public ComponentExpressionBuilder(List<Edge> edges) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
  }

  public List<CPQExpression> build(Component component, int diameterCap, boolean firstHit) {
    Objects.requireNonNull(component, "component");
    if (component.edgeBits().isEmpty()) return List.of();
    Map<CacheKey, List<CPQExpression>> cache = new HashMap<>();
    List<CPQExpression> result = recurse(component, new Options(diameterCap), cache, firstHit);
    // Optimized: return immediately if we only need one result and found it.
    // We also pass firstHit down into recurse() to stop generation early.
    if (firstHit && !result.isEmpty()) {
      return List.of(result.get(0));
    }
    return result;
  }

  private List<CPQExpression> recurse(
      Component component, Options o, Map<CacheKey, List<CPQExpression>> cache, boolean firstHit) {
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
        expressions.addAll(
            BacktrackingLoops.findAll(
                edges, component, bits, component.joinNodes(), o.diameterCap()));
        expressions.removeIf(e -> !component.endpointsAllowed(e.source(), e.target()));
      }

      // Optimization: if we found a simple loop/backtrack solution and only need one,
      // stop here.
      if (firstHit && !expressions.isEmpty()) {
        List<CPQExpression> result = expressions;
        cache.put(key, result);
        return result;
      }

      expressions.addAll(composites(component, bits, o, cache, firstHit));
    }

    List<CPQExpression> result = expressions;

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
    if (!component.endpointsAllowed(source, target)) return;
    CPQExpression candidate = new CPQExpression(cpq, component, source, target, derivation);
    CPQExpression looped = makeLoop(candidate, o);
    if (looped == null) {
      return;
    }
    out.add(looped);
  }

  // --------------------------------------------------------------------------
  // Composite rules (split + concat / intersect)
  // --------------------------------------------------------------------------

  private List<CPQExpression> composites(
      Component owner,
      BitSet ownerBits,
      Options o,
      Map<CacheKey, List<CPQExpression>> cache,
      boolean firstHit) {
    Function<BitSet, List<CPQExpression>> buildSub =
        subset -> recurse(owner.restrictTo(subset, edges), o, cache, firstHit);

    List<CPQExpression> out = new ArrayList<>();
    forEachSplit(
        ownerBits,
        edges.size(),
        (a, b) -> {
          // Optimization: stop if we already have a result and only need one.
          if (firstHit && !out.isEmpty()) return;

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
}
