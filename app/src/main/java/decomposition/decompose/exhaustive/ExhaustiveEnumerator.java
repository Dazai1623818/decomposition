package decomposition.decompose.exhaustive;

import decomposition.core.CPQExpression;
import decomposition.core.Component;
import decomposition.core.ConjunctiveQuery;
import decomposition.core.Edge;
import decomposition.eval.EvaluationRun;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Exhaustive enumeration of CQ → CPQ decompositions
 *
 * <p>Implementation details:
 *
 * <ul>
 *   <li>Partitions are generated imperatively instead of via Stream.flatMap.
 *   <li>Partitions are produced lazily to avoid generating all combinations upfront.
 *   <li>Deduplication uses structural hashes rather than full CPQ serialization.
 *   <li>Parallel processing is available through a ForkJoinPool.
 * </ul>
 */
public final class ExhaustiveEnumerator {

  private ExhaustiveEnumerator() {}

  /** Configuration for the exhaustive enumerator. */
  public static final class Config {
    boolean parallel = false;
    int parallelism = Runtime.getRuntime().availableProcessors();

    public static Config sequential() {
      return new Config();
    }

    public static Config parallel() {
      Config c = new Config();
      c.parallel = true;
      return c;
    }

    public static Config parallel(int parallelism) {
      Config c = new Config();
      c.parallel = true;
      c.parallelism = parallelism;
      return c;
    }
  }

  public static EvaluationRun decompose(ConjunctiveQuery query) {
    return decompose(query, Config.sequential());
  }

  public static EvaluationRun decompose(ConjunctiveQuery query, Config config) {
    EvaluationRun run = new EvaluationRun();
    long totalStart = System.nanoTime();

    // Phase 1: Component enumeration
    long phase1Start = System.nanoTime();
    var g = new QueryGraph(query);
    var components = g.enumerateComponents(config.parallel);
    long phase1End = System.nanoTime();
    run.recordPhase(
        EvaluationRun.Phase.COMPONENT_ENUMERATION,
        (phase1End - phase1Start) / 1_000_000,
        components.size());

    // Phase 2: Build component index for faster lookup
    var builder = new ComponentExpressionBuilder(g.edges);
    BitSet fullMask = g.fullMask();

    // Pre-index components by first set bit for faster partition generation
    @SuppressWarnings("unchecked")
    List<Component>[] componentsByFirstBit = new List[g.n];
    for (int i = 0; i < g.n; i++) {
      componentsByFirstBit[i] = new ArrayList<>();
    }
    for (Component c : components) {
      int first = c.edgeBits().nextSetBit(0);
      if (first >= 0) {
        componentsByFirstBit[first].add(c);
      }
    }

    // Phase 3: Generate partitions and build expressions
    long phase2Start = System.nanoTime();
    AtomicInteger partitionCount = new AtomicInteger(0);

    Map<Component, CPQExpression> expressionCache =
        config.parallel ? new ConcurrentHashMap<>() : new HashMap<>();
    Map<String, List<CPQExpression>> unique =
        config.parallel ? new ConcurrentHashMap<>() : new LinkedHashMap<>();

    if (config.parallel) {
      // Parallel partition processing
      ForkJoinPool pool =
          config.parallelism == ForkJoinPool.getCommonPoolParallelism()
              ? ForkJoinPool.commonPool()
              : new ForkJoinPool(config.parallelism);
      try {
        List<List<Component>> allPartitions = new ArrayList<>();
        generatePartitionsIterative(
            componentsByFirstBit, fullMask, g.n, partition -> allPartitions.add(partition));
        partitionCount.set(allPartitions.size());

        pool.submit(
                () ->
                    allPartitions.parallelStream()
                        .forEach(
                            partition -> {
                              long exprStart = System.nanoTime();
                              List<CPQExpression> tuple =
                                  toCpqList(partition, builder, expressionCache);
                              long exprEnd = System.nanoTime();
                              run.addExpressionBuildingTime((exprEnd - exprStart) / 1_000_000);

                              if (tuple != null) {
                                String key = tupleKeyStructural(tuple);
                                unique.putIfAbsent(key, tuple);
                              }
                            }))
            .join();
      } finally {
        if (pool != ForkJoinPool.commonPool()) {
          pool.shutdown();
        }
      }
    } else {
      // Sequential partition processing with lazy generation
      generatePartitionsIterative(
          componentsByFirstBit,
          fullMask,
          g.n,
          partition -> {
            partitionCount.incrementAndGet();

            long exprStart = System.nanoTime();
            List<CPQExpression> tuple = toCpqList(partition, builder, expressionCache);
            long exprEnd = System.nanoTime();
            run.addExpressionBuildingTime((exprEnd - exprStart) / 1_000_000);

            if (tuple != null) {
              String key = tupleKeyStructural(tuple);
              unique.putIfAbsent(key, tuple);
            }
          });
    }
    long phase2End = System.nanoTime();
    run.recordPhase(
        EvaluationRun.Phase.PARTITION_GENERATION,
        (phase2End - phase2Start) / 1_000_000,
        partitionCount.get());
    run.finalizeExpressionBuilding();

    // Phase 4: Final result assembly
    long phase3Start = System.nanoTime();
    List<List<CPQExpression>> result = List.copyOf(unique.values());
    long phase3End = System.nanoTime();
    run.recordPhase(
        EvaluationRun.Phase.DEDUPLICATION, (phase3End - phase3Start) / 1_000_000, result.size());
    run.setDecompositions(result);

    long totalEnd = System.nanoTime();
    run.recordPhaseMs(EvaluationRun.Phase.TOTAL, (totalEnd - totalStart) / 1_000_000);

    return run;
  }

  /**
   * Generates partitions iteratively using backtracking.
   *
   * <p>This avoids the overhead of Stream.flatMap and recursive function calls.
   */
  private static void generatePartitionsIterative(
      List<Component>[] componentsByFirstBit,
      BitSet fullMask,
      int edgeCount,
      Consumer<List<Component>> consumer) {

    // Stack-based backtracking
    Deque<PartitionState> stack = new ArrayDeque<>();
    stack.push(new PartitionState((BitSet) fullMask.clone(), new ArrayList<>(), 0));

    while (!stack.isEmpty()) {
      PartitionState state = stack.pop();

      if (state.remaining.isEmpty()) {
        // Found a complete partition
        consumer.accept(List.copyOf(state.partition));
        continue;
      }

      int firstBit = state.remaining.nextSetBit(0);
      List<Component> candidates = componentsByFirstBit[firstBit];

      // Try each component that covers firstBit
      for (int i = state.candidateIndex; i < candidates.size(); i++) {
        Component c = candidates.get(i);
        BitSet bits = c.edgeBits();

        // Check if this component is a subset of remaining
        if (!isSubset(bits, state.remaining)) {
          continue;
        }

        // Create new state with this component added
        BitSet newRemaining = minus(state.remaining, bits);
        List<Component> newPartition = new ArrayList<>(state.partition);
        newPartition.add(c);

        stack.push(new PartitionState(newRemaining, newPartition, 0));
      }
    }
  }

  private record PartitionState(BitSet remaining, List<Component> partition, int candidateIndex) {}

  /** Convert a partition of edge-sets into CPQ expressions. */
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
   * <p>Heuristic: prefer smaller diameter, then shorter normalized string.
   */
  private static CPQExpression chooseOne(List<CPQExpression> options) {
    if (options == null || options.isEmpty()) {
      return null;
    }
    CPQExpression best = null;
    for (CPQExpression candidate : options) {
      if (candidate == null) continue;
      if (best == null) {
        best = candidate;
        continue;
      }
      int cd = candidate.cpq().getDiameter();
      int bd = best.cpq().getDiameter();
      if (cd != bd) {
        if (cd < bd) best = candidate;
        continue;
      }
      String cs = candidate.cpq().toString().replace(" ", "");
      String bs = best.cpq().toString().replace(" ", "");
      if (cs.length() != bs.length()) {
        if (cs.length() < bs.length()) best = candidate;
        continue;
      }
      if (cs.compareTo(bs) < 0) {
        best = candidate;
      }
    }
    return best;
  }

  // ----- Structural hashing for deduplication -----

  /**
   * Creates a key for a tuple using structural hashing.
   *
   * <p>Uses edge bits + endpoints instead of full CPQ string serialization.
   */
  private static String tupleKeyStructural(List<CPQExpression> tuple) {
    // Sort by edge signature for canonical ordering
    List<String> keys = new ArrayList<>(tuple.size());
    for (CPQExpression e : tuple) {
      keys.add(expressionKeyStructural(e));
    }
    keys.sort(String::compareTo);
    return String.join("|", keys);
  }

  private static String expressionKeyStructural(CPQExpression e) {
    // Use edge bits + source/target + diameter as a faster key
    String edgeSig = e.component() != null ? bitsetToIntString(e.component().edgeBits()) : "";
    int diameter = e.cpq().getDiameter();
    return edgeSig + ":" + e.source() + ">" + e.target() + ":" + diameter;
  }

  private static String bitsetToIntString(BitSet bits) {
    StringBuilder sb = new StringBuilder();
    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      if (sb.length() > 0) sb.append(',');
      sb.append(i);
    }
    return sb.toString();
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

  // ----- Query Graph (precomputed structure) -----

  private static final class QueryGraph {
    final int n;
    final BitSet[] incident;
    final BitSet freeVars;
    final int[][] edgeEndpoints;
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

    List<Component> enumerateComponents(boolean parallel) {
      if (parallel) {
        return java.util.stream.IntStream.range(0, n)
            .parallel()
            .mapToObj(this::enumerateFromSeed)
            .flatMap(List::stream)
            .toList();
      } else {
        List<Component> out = new ArrayList<>();
        Set<BitSet> seen = new HashSet<>();
        for (int seed = 0; seed < n; seed++) {
          out.addAll(enumerateFromSeed(seed, seen));
        }
        return out;
      }
    }

    private List<Component> enumerateFromSeed(int seed) {
      return enumerateFromSeed(seed, new HashSet<>());
    }

    private List<Component> enumerateFromSeed(int seed, Set<BitSet> seen) {
      List<Component> out = new ArrayList<>();
      Deque<GrowState> stack = new ArrayDeque<>();

      BitSet startEdges = new BitSet(n);
      startEdges.set(seed);
      stack.push(new GrowState(seed, startEdges, varsOf(startEdges)));

      while (!stack.isEmpty()) {
        GrowState state = stack.pop();

        if (!seen.add(state.edgeBits)) {
          continue;
        }

        Set<String> joinNodes = joinNodes(state.edgeBits, state.vars);
        if (joinNodes.size() <= 2) {
          out.add(new Component(state.edgeBits, vertexNames(state.vars), joinNodes, varMap));
        } else {
          // Pruning: if more than 2 join nodes, this component itself is invalid,
          // but could we grow it into something with fewer join nodes?
          // In CQ decomposition, adding an edge usually increases or keeps join nodes
          // same
          // (unless it closes a cycle that absorbs a frontier variable).
          // However, components are connected subgraphs. Adding an edge connects to one
          // or two existing vars.
          // Frontier variables are those that have incident edges NOT in the component.
          // Adding an edge converts a frontier edge to an internal edge, potentially
          // removing a join node.
          // BUT if we already have >2 join nodes, it's very unlikely to drop below 2 by
          // adding more edges.
          // Let's keep it safe for now and not prune unless we are sure.
        }

        // Try adding adjacent edges with index > seed
        for (int v = state.vars.nextSetBit(0); v >= 0; v = state.vars.nextSetBit(v + 1)) {
          BitSet possible = (BitSet) incident[v].clone();
          // Only edges with index > seed to avoid permutations
          // (incident[v] is already a BitSet, nextSetBit(seed + 1) is efficient)
          for (int e = possible.nextSetBit(seed + 1); e >= 0; e = possible.nextSetBit(e + 1)) {
            if (state.edgeBits.get(e)) continue;

            BitSet nextEdges = (BitSet) state.edgeBits.clone();
            nextEdges.set(e);

            if (seen.contains(nextEdges)) continue;

            stack.push(new GrowState(seed, nextEdges, varsOf(nextEdges)));
          }
        }
      }
      return out;
    }

    private record GrowState(int seed, BitSet edgeBits, BitSet vars) {}

    private BitSet varsOf(BitSet edgeBits) {
      var vars = new BitSet();
      for (int e = edgeBits.nextSetBit(0); e >= 0; e = edgeBits.nextSetBit(e + 1)) {
        vars.set(edgeEndpoints[e][0]);
        vars.set(edgeEndpoints[e][1]);
      }
      return vars;
    }

    private Set<String> joinNodes(BitSet edgeBits, BitSet vars) {
      var names = new LinkedHashSet<String>();
      for (int v = vars.nextSetBit(0); v >= 0; v = vars.nextSetBit(v + 1)) {
        if (freeVars.get(v) || !isSubset(incident[v], edgeBits)) {
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
