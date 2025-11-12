package decomposition.cpq;

import decomposition.cpq.model.CacheStats;
import decomposition.cpq.model.ComponentCPQExpressions;
import decomposition.cpq.model.ComponentKey;
import decomposition.cpq.model.PartitionAnalysis;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * End-to-end CPQ enumerator: handles rule synthesis, memoization, component analysis, and tuple
 * enumeration for partitions.
 */
public final class CPQEnumerator {
  private final List<Edge> edges;
  private final Map<RuleCacheKey, List<KnownComponent>> ruleCache = new ConcurrentHashMap<>();
  private final Map<ComponentCacheKey, ComponentCPQExpressions> componentCache =
      new ConcurrentHashMap<>();
  private final CacheStats cacheStats;
  private final ComponentEdgeMatcher edgeMatcher;
  private final ReverseLoopGenerator reverseLoopGenerator;
  private final PartitionDiagnostics partitionDiagnostics = new PartitionDiagnostics();

  public CPQEnumerator(List<Edge> edges) {
    this(edges, new CacheStats());
  }

  /**
   * Returns the diagnostics recorded during the last failed {@link #analyzePartition} call. Empty
   * when no failure has been recorded.
   */
  public List<String> lastComponentDiagnostics() {
    return partitionDiagnostics.lastComponentDiagnostics();
  }

  public CPQEnumerator(List<Edge> edges, CacheStats stats) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
    this.cacheStats = Objects.requireNonNull(stats, "stats");
    this.edgeMatcher = new ComponentEdgeMatcher(this.edges);
    this.reverseLoopGenerator = new ReverseLoopGenerator(edgeMatcher);
  }

  public CacheStats cacheStats() {
    return cacheStats;
  }

  // --------------------------------------------------------------------------
  // Rule construction
  // --------------------------------------------------------------------------

  public List<KnownComponent> constructionRules(
      BitSet edgeSubset, Map<String, String> originalVarMap) {
    return constructionRules(edgeSubset, Set.of(), originalVarMap);
  }

  public List<KnownComponent> constructionRules(
      BitSet edgeSubset, Set<String> requestedJoinNodes, Map<String, String> originalVarMap) {
    Objects.requireNonNull(edgeSubset, "edgeSubset");
    Objects.requireNonNull(originalVarMap, "originalVarMap");
    return componentToCPQExpressions(edgeSubset, normalize(requestedJoinNodes), originalVarMap);
  }

  public ComponentCPQExpressions componentCPQExpressions(
      Component component,
      Set<String> requestedJoinNodes,
      Set<String> freeVariables,
      int totalComponents,
      Map<String, String> originalVarMap) {

    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(originalVarMap, "originalVarMap");

    Set<String> joinNodes = normalize(requestedJoinNodes);
    Set<String> freeVars = normalize(freeVariables);

    ComponentCacheKey key =
        new ComponentCacheKey(
            BitsetUtils.signature(component.edgeBits(), edges.size()),
            joinNodes,
            freeVars,
            component.edgeCount(),
            totalComponents,
            varContextHash(originalVarMap));

    ComponentCPQExpressions cached = componentCache.get(key);
    if (cached != null) {
      cacheStats.recordHit();
      return cached;
    }

    cacheStats.recordMiss();

    // Build component rules directly
    List<KnownComponent> raw =
        componentToCPQExpressions(component.edgeBits(), joinNodes, originalVarMap);

    // Filter by join-node roles for multi-edge components
    List<KnownComponent> joinFiltered = raw;
    if (!joinNodes.isEmpty() && component.edgeCount() > 1) {
      Set<String> local = JoinNodeUtils.localJoinNodes(component, joinNodes);
      joinFiltered =
          raw.stream()
              .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, local))
              .toList();
    }

    List<KnownComponent> finals = joinFiltered;
    if (joinNodes.size() == 2 && !joinFiltered.isEmpty()) {
      List<String> ordered = new ArrayList<>(joinNodes);
      ordered.sort(
          Comparator.comparing((String node) -> freeVars.contains(node) ? 0 : 1)
              .thenComparingInt(node -> getOriginalVarOrder(node, originalVarMap))
              .thenComparing(node -> node));
      String preferredSource = ordered.get(0);
      String preferredTarget = ordered.get(1);

      List<KnownComponent> oriented =
          joinFiltered.stream()
              .filter(
                  rule ->
                      preferredSource.equals(rule.source())
                          && preferredTarget.equals(rule.target()))
              .toList();
      if (!oriented.isEmpty()) {
        finals = oriented;
      }
    }

    ComponentCPQExpressions computed =
        new ComponentCPQExpressions(component, raw, joinFiltered, finals);
    componentCache.put(key, computed);
    return computed;
  }

  // --------------------------------------------------------------------------
  // Partition analysis + enumeration
  // --------------------------------------------------------------------------

  public PartitionAnalysis analyzePartition(
      Partition partition,
      Set<String> requestedJoinNodes,
      Set<String> freeVariables,
      Map<String, String> originalVarMap) {

    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(originalVarMap, "originalVarMap");

    Set<String> joinNodes = normalize(requestedJoinNodes);
    Set<String> freeVars = normalize(freeVariables);
    List<Component> components = partition.components();
    int totalComponents = components.size();
    partitionDiagnostics.beginPartition();

    List<ComponentCPQExpressions> perComponent = new ArrayList<>(totalComponents);
    for (int idx = 0; idx < totalComponents; idx++) {
      Component component = components.get(idx);
      ComponentCPQExpressions rules =
          componentCPQExpressions(component, joinNodes, freeVars, totalComponents, originalVarMap);

      String componentSig = BitsetUtils.signature(component.edgeBits(), edges.size());
      partitionDiagnostics.recordComponent(idx + 1, componentSig, rules, joinNodes.isEmpty());

      if (rules.finalRules().isEmpty()) {
        partitionDiagnostics.failPartition();
        return null;
      }
      perComponent.add(rules);
    }

    List<KnownComponent> preferred =
        perComponent.stream().map(ComponentCPQExpressions::preferred).toList();
    List<Integer> ruleCounts = perComponent.stream().map(r -> r.finalRules().size()).toList();

    partitionDiagnostics.succeedPartition();
    return new PartitionAnalysis(perComponent, preferred, ruleCounts);
  }

  public List<List<KnownComponent>> enumerateTuples(PartitionAnalysis analysis, int limit) {
    if (analysis == null) return List.of();

    List<List<KnownComponent>> perComponent =
        analysis.components().stream().map(ComponentCPQExpressions::finalRules).toList();
    if (perComponent.isEmpty()) return List.of();

    int n = perComponent.size();
    int[] idx = new int[n];
    List<List<KnownComponent>> out = new ArrayList<>();
    while (true) {
      // build current tuple
      List<KnownComponent> tuple = new ArrayList<>(n);
      for (int i = 0; i < n; i++) tuple.add(perComponent.get(i).get(idx[i]));
      out.add(tuple);
      if (limit > 0 && out.size() >= limit) break;

      // advance odometer
      int p = n - 1;
      while (p >= 0) {
        idx[p]++;
        if (idx[p] < perComponent.get(p).size()) break;
        idx[p] = 0;
        p--;
      }
      if (p < 0) break; // finished
    }
    return out;
  }

  // --------------------------------------------------------------------------
  // Core recursive derivation
  // --------------------------------------------------------------------------

  private List<KnownComponent> componentToCPQExpressions(
      BitSet edgeSubset, Set<String> requestedJoinNodes, Map<String, String> originalVarMap) {
    if (edgeSubset.isEmpty()) return List.of();
    Objects.requireNonNull(originalVarMap, "originalVarMap");

    Set<String> localJoinNodes =
        JoinNodeUtils.localJoinNodes(edgeSubset, edges, requestedJoinNodes);

    RuleCacheKey key =
        new RuleCacheKey(
            BitsetUtils.signature(edgeSubset, edges.size()),
            localJoinNodes,
            edgeSubset.cardinality());

    List<KnownComponent> cached = ruleCache.get(key);
    if (cached != null) return cached;

    List<KnownComponent> rules = new ArrayList<>();
    int edgeCount = edgeSubset.cardinality();

    if (edgeCount == 1) {
      rules.addAll(
          SingleEdgeRuleFactory.build(
              edges.get(edgeSubset.nextSetBit(0)), edgeSubset, originalVarMap));
    } else {
      if (localJoinNodes.size() <= 1) {
        rules.addAll(LoopBacktrackBuilder.build(edges, edgeSubset, localJoinNodes, originalVarMap));
      }
      Function<BitSet, List<KnownComponent>> resolver =
          subset -> componentToCPQExpressions(subset, requestedJoinNodes, originalVarMap);
      rules.addAll(CompositeRuleFactory.build(edgeSubset, edges.size(), resolver));
    }

    // --- Merged: expandValidateAndDedup, variantsFor, ensureLoopAnchored, tryReverse, reverse,
    // matchesComponent ---

    Map<ComponentKey, KnownComponent> unique = new LinkedHashMap<>();

    for (KnownComponent rule : rules) {
      List<KnownComponent> variants = reverseLoopGenerator.generate(rule, originalVarMap);

      for (KnownComponent variant : variants) {
        ComponentKey compKey =
            new ComponentKey(variant.edges(), variant.source(), variant.target());
        unique.putIfAbsent(compKey, variant);
      }
    }

    List<KnownComponent> result = unique.isEmpty() ? List.of() : List.copyOf(unique.values());
    ruleCache.put(key, result);
    return result;
  }

  private int getOriginalVarOrder(String node, Map<String, String> originalVarMap) {
    if (node == null || originalVarMap == null || originalVarMap.isEmpty()) {
      return Integer.MAX_VALUE;
    }
    int idx = 0;
    for (Map.Entry<String, String> entry : originalVarMap.entrySet()) {
      if (Objects.equals(node, entry.getValue())) {
        return idx;
      }
      idx++;
    }
    return Integer.MAX_VALUE;
  }

  private static Set<String> normalize(Set<String> values) {
    return (values == null || values.isEmpty()) ? Set.of() : Set.copyOf(values);
  }

  private static int varContextHash(Map<String, String> originalVarMap) {
    if (originalVarMap == null || originalVarMap.isEmpty()) {
      return 0;
    }
    int hash = 1;
    for (Map.Entry<String, String> entry : originalVarMap.entrySet()) {
      hash = 31 * hash + Objects.hash(entry.getKey(), entry.getValue());
    }
    return hash;
  }
}
