package decomposition.decompose;

import decomposition.core.model.Component;
import decomposition.core.model.Edge;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/** Exhaustive enumeration of CQ → CPQ decompositions. */
final class ExhaustiveEnumerator {

  private ExhaustiveEnumerator() {}

  static List<List<CPQ>> decompose(ConjunctiveQuery query) {
    var g = new QueryGraph(query);
    var components = g.enumerateComponents();
    var builder = new ComponentExpressionBuilder(g.edges);
    Map<Component, List<CPQExpression>> expressionCache = new HashMap<>();

    return partitions(components, g.fullMask()).stream()
        .flatMap(partition -> toCpqLists(partition, builder, expressionCache))
        .toList();
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
  private static Stream<List<CPQ>> toCpqLists(
      List<Component> partition,
      ComponentExpressionBuilder builder,
      Map<Component, List<CPQExpression>> expressionCache) {
    var choices =
        partition.stream()
            .map(
                component ->
                    expressionCache.computeIfAbsent(component, c -> builder.build(c, 0, false)))
            .toList();

    if (choices.stream().anyMatch(List::isEmpty)) return Stream.empty();
    return cartesianProduct(choices).stream().map(List::copyOf);
  }

  /** Cartesian product of CPQ choices per component. */
  private static List<List<CPQ>> cartesianProduct(List<List<CPQExpression>> choices) {
    List<List<CPQ>> result = List.of(List.of());
    for (var options : choices) {
      result =
          result.stream()
              .flatMap(prefix -> options.stream().map(e -> append(prefix, e.cpq())))
              .toList();
    }
    return result;
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

  private static <T> List<T> append(List<T> list, T item) {
    var r = new ArrayList<>(list);
    r.add(item);
    return r;
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
      for (int seed = 0; seed < n; seed++) {
        var edges = new BitSet();
        edges.set(seed);
        grow(seed, edges, varsOf(edges), out);
      }
      return out;
    }

    private void grow(int seed, BitSet edges, BitSet vars, List<Component> out) {
      Set<String> joinNodes = joinNodes(edges, vars);
      if (joinNodes.size() <= 2) {
        out.add(new Component(edges, vertexNames(vars), joinNodes, varMap));
      }

      // Add adjacent edges with index > seed
      for (int v = vars.nextSetBit(0); v >= 0; v = vars.nextSetBit(v + 1)) {
        for (int e = incident[v].nextSetBit(seed + 1); e >= 0; e = incident[v].nextSetBit(e + 1)) {
          if (edges.get(e)) continue;
          var newE = (BitSet) edges.clone();
          newE.set(e);
          grow(seed, newE, varsOf(newE), out);
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
