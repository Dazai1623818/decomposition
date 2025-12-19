package decomposition.decompose;

import decomposition.core.Component;
import decomposition.core.Edge;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/** Exhaustive enumeration of CQ → CPQ decompositions. */
final class ExhaustiveEnumerator {

  private ExhaustiveEnumerator() {}

  static List<List<CPQExpression>> decompose(ConjunctiveQuery query) {
    var g = new QueryGraph(query);
    var components = g.enumerateComponents();
    var builder = new ComponentExpressionBuilder(g.edges);
    Map<Component, CPQExpression> expressionCache = new HashMap<>();

    Map<String, List<CPQExpression>> unique = new LinkedHashMap<>();
    for (List<Component> partition : partitions(components, g.fullMask())) {
      List<CPQExpression> tuple = toCpqList(partition, builder, expressionCache);
      if (tuple != null) {
        unique.putIfAbsent(tupleKey(tuple), tuple);
      }
    }
    return List.copyOf(unique.values());
  }

  /** All ways to partition fullMask using components. */
  private static List<List<Component>> partitions(List<Component> components, BitSet remaining) {
    if (remaining.isEmpty()) return List.of(List.of());
    int first = remaining.nextSetBit(0);

    return components.stream()
        .flatMap(
            component -> {
              var edgeBits = component.edgeBits();
              if (!edgeBits.get(first) || !isSubset(edgeBits, remaining)) {
                return Stream.<List<Component>>empty();
              }
              return partitions(components, minus(remaining, edgeBits)).stream()
                  .map(rest -> prepend(component, rest));
            })
        .toList();
  }

  /** Convert a partition of edge-sets into all valid CPQ decompositions. */
  private static List<CPQExpression> toCpqList(
      List<Component> partition,
      ComponentExpressionBuilder builder,
      Map<Component, CPQExpression> expressionCache) {
    List<CPQExpression> tuple = new ArrayList<>(partition.size());
    for (Component component : partition) {
      CPQExpression chosen =
          expressionCache.computeIfAbsent(component, c -> chooseOne(builder.build(c, 0, true)));
      if (chosen == null) {
        return null;
      }
      tuple.add(chosen);
    }
    return List.copyOf(tuple);
  }

  /**
   * Picks a single representative expression for a component.
   *
   * <p>Heuristic: prefer smaller diameter, then shorter normalized string, then lexicographic.
   */
  private static CPQExpression chooseOne(List<CPQExpression> options) {
    if (options == null || options.isEmpty()) {
      return null;
    }
    CPQExpression best = null;
    for (CPQExpression candidate : options) {
      if (candidate == null) {
        continue;
      }
      if (best == null) {
        best = candidate;
        continue;
      }
      int cd = candidate.cpq().getDiameter();
      int bd = best.cpq().getDiameter();
      if (cd != bd) {
        if (cd < bd) {
          best = candidate;
        }
        continue;
      }
      String cs = sanitize(candidate.cpq().toString());
      String bs = sanitize(best.cpq().toString());
      if (cs.length() != bs.length()) {
        if (cs.length() < bs.length()) {
          best = candidate;
        }
        continue;
      }
      if (cs.compareTo(bs) < 0) {
        best = candidate;
      }
    }
    return best;
  }

  // ----- BitSet helpers -----

  private static boolean isSubset(BitSet sub, BitSet sup) {
    var diff = (BitSet) sub.clone();
    diff.andNot(sup);
    return diff.isEmpty();
  }

  private static BitSet minus(BitSet a, BitSet b) {
    var r = (BitSet) a.clone();
    r.andNot(b);
    return r;
  }

  private static <T> List<T> prepend(T head, List<T> tail) {
    var r = new ArrayList<T>(tail.size() + 1);
    r.add(head);
    r.addAll(tail);
    return r;
  }

  private static String tupleKey(List<CPQExpression> tuple) {
    List<String> keys = new ArrayList<>(tuple.size());
    for (CPQExpression e : tuple) {
      keys.add(expressionKey(e));
    }
    keys.sort(String::compareTo);
    return String.join("|", keys);
  }

  private static String expressionKey(CPQExpression e) {
    String cpq = sanitize(e.cpq().toString());
    String source = e.source();
    String target = e.target();
    String edgeSig = e.component() == null ? "" : bitsetSignature(e.component().edgeBits());
    return edgeSig + ":" + source + ">" + target + ":" + cpq;
  }

  private static String sanitize(String s) {
    return s.replace(" ", "");
  }

  private static String bitsetSignature(BitSet bits) {
    StringBuilder sb = new StringBuilder();
    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      sb.append(i).append(',');
    }
    return sb.toString();
  }

  // ----- Query Graph (precomputed structure) -----

  private static final class QueryGraph {
    final int n; // edge count
    final BitSet[] incident; // incident[v] = edges touching variable v
    final BitSet freeVars;
    final int[][] edgeEndpoints; // edgeEndpoints[e] = {src, dst}
    final List<Edge> edges;
    final Map<Integer, String> varNames;
    final Map<String, String> varMap;

    QueryGraph(ConjunctiveQuery query) {
      var graph = query.graph();
      var graphEdges = new ArrayList<>(graph.getEdges());
      this.n = graphEdges.size();
      int v = graph.getNodeCount();

      this.edgeEndpoints = new int[n][2];
      this.incident = new BitSet[v];
      for (int i = 0; i < v; i++) incident[i] = new BitSet(n);

      this.edges = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        var e = graphEdges.get(i);
        int src = e.getSourceNode().getID(), dst = e.getTargetNode().getID();
        edgeEndpoints[i][0] = src;
        edgeEndpoints[i][1] = dst;
        incident[src].set(i);
        incident[dst].set(i);
        var atom = e.getData();
        edges.add(
            new Edge(atom.getSource().getName(), atom.getTarget().getName(), atom.getLabel(), i));
      }

      this.freeVars = new BitSet(v);
      this.varNames = new LinkedHashMap<>();
      this.varMap = new LinkedHashMap<>();
      for (var node : graph.getNodes()) {
        varNames.put(node.getID(), node.getData().getName());
        varMap.put(node.getData().getName(), node.getData().getName());
      }
      for (var free : query.gmarkCQ().getFreeVariables()) {
        var node = graph.getNode(free);
        if (node != null) freeVars.set(node.getID());
      }
    }

    BitSet fullMask() {
      var m = new BitSet(n);
      m.set(0, n);
      return m;
    }

    /** All connected subgraphs with ≤2 join variables. */
    List<Component> enumerateComponents() {
      var out = new ArrayList<Component>();
      Set<String> seenEdgeSets = new HashSet<>();
      for (int seed = 0; seed < n; seed++) {
        var edges = new BitSet();
        edges.set(seed);
        grow(seed, edges, varsOf(edges), out, seenEdgeSets);
      }
      return out;
    }

    private void grow(
        int seed, BitSet edges, BitSet vars, List<Component> out, Set<String> seenEdgeSets) {
      Set<String> joinNodes = joinNodes(edges, vars);
      if (joinNodes.size() <= 2) {
        String signature = bitsetSignature(edges);
        if (seenEdgeSets.add(signature)) {
          out.add(new Component(edges, vertexNames(vars), joinNodes, varMap));
        }
      }

      // Add adjacent edges with index > seed
      for (int v = vars.nextSetBit(0); v >= 0; v = vars.nextSetBit(v + 1)) {
        for (int e = incident[v].nextSetBit(seed + 1); e >= 0; e = incident[v].nextSetBit(e + 1)) {
          if (edges.get(e)) continue;
          var newE = (BitSet) edges.clone();
          newE.set(e);
          grow(seed, newE, varsOf(newE), out, seenEdgeSets);
        }
      }
    }

    private BitSet varsOf(BitSet edges) {
      var vars = new BitSet();
      for (int e = edges.nextSetBit(0); e >= 0; e = edges.nextSetBit(e + 1)) {
        vars.set(edgeEndpoints[e][0]);
        vars.set(edgeEndpoints[e][1]);
      }
      return vars;
    }

    private Set<String> joinNodes(BitSet edges, BitSet vars) {
      var names = new LinkedHashSet<String>();
      for (int v = vars.nextSetBit(0); v >= 0; v = vars.nextSetBit(v + 1)) {
        if (freeVars.get(v) || !isSubset(incident[v], edges)) {
          var name = varNames.get(v);
          if (name != null) names.add(name);
        }
      }
      return names;
    }

    private Set<String> vertexNames(BitSet vars) {
      var names = new LinkedHashSet<String>();
      for (int v = vars.nextSetBit(0); v >= 0; v = vars.nextSetBit(v + 1)) {
        var name = varNames.get(v);
        if (name != null) names.add(name);
      }
      return names;
    }
  }
}
