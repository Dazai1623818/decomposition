package decomposition.cpq;

import decomposition.cpq.model.CacheStats;
import decomposition.cpq.model.ComponentKey;
import decomposition.cpq.model.ComponentRules;
import decomposition.cpq.model.PartitionAnalysis;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
// REMOVED: import dev.roanh.gmark.ast.QueryTree;  // No longer needed
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
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * End-to-end CPQ engine: handles rule synthesis, memoization, component analysis, and tuple
 * enumeration for partitions.
 */
public final class CPQEngine {
  private final List<Edge> edges;
  private final Map<CacheKey, List<KnownComponent>> ruleCache = new ConcurrentHashMap<>();
  private final Map<CacheKey, ComponentRules> componentCache = new ConcurrentHashMap<>();
  private final CacheStats cacheStats;

  public CPQEngine(List<Edge> edges) {
    this(edges, new CacheStats());
  }

  public CPQEngine(List<Edge> edges, CacheStats stats) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
    this.cacheStats = Objects.requireNonNull(stats, "stats");
  }

  public CacheStats cacheStats() {
    return cacheStats;
  }

  // --------------------------------------------------------------------------
  // Rule construction
  // --------------------------------------------------------------------------

  public List<KnownComponent> constructionRules(BitSet edgeSubset) {
    return constructionRules(edgeSubset, Set.of());
  }

  public List<KnownComponent> constructionRules(BitSet edgeSubset, Set<String> requestedJoinNodes) {
    Objects.requireNonNull(edgeSubset, "edgeSubset");
    return deriveRules(edgeSubset, normalize(requestedJoinNodes));
  }

  public ComponentRules componentRules(
      Component component,
      Set<String> requestedJoinNodes,
      Set<String> freeVariables,
      int totalComponents) {

    Objects.requireNonNull(component, "component");

    Set<String> joinNodes = normalize(requestedJoinNodes);
    Set<String> freeVars = normalize(freeVariables);

    CacheKey key =
        new CacheKey(
            BitsetUtils.signature(component.edgeBits(), edges.size()),
            joinNodes,
            freeVars,
            component.edgeCount(),
            totalComponents);

    ComponentRules cached = componentCache.get(key);
    if (cached != null) {
      cacheStats.recordHit();
      return cached;
    }

    cacheStats.recordMiss();

    // Build component rules directly
    List<KnownComponent> raw = deriveRules(component.edgeBits(), joinNodes);

    // Filter by join-node roles for multi-edge components
    List<KnownComponent> joinFiltered = raw;
    if (!joinNodes.isEmpty() && component.edgeCount() > 1) {
      Set<String> local = JoinNodeUtils.localJoinNodes(component, joinNodes);
      joinFiltered =
          raw.stream()
              .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, local))
              .toList();
    }

    // Prefer canonical orientation
    List<KnownComponent> finals = joinFiltered;
    if (joinNodes.size() == 2 && !joinFiltered.isEmpty()) {
      List<String> ordered = new ArrayList<>(joinNodes);
      ordered.sort(
          Comparator.comparing((String node) -> freeVars.contains(node) ? 0 : 1)
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

    ComponentRules computed = new ComponentRules(component, raw, joinFiltered, finals);
    componentCache.put(key, computed);
    return computed;
  }

  // --------------------------------------------------------------------------
  // Partition analysis + enumeration
  // --------------------------------------------------------------------------

  public PartitionAnalysis analyzePartition(
      Partition partition, Set<String> requestedJoinNodes, Set<String> freeVariables) {

    Objects.requireNonNull(partition, "partition");

    Set<String> joinNodes = normalize(requestedJoinNodes);
    Set<String> freeVars = normalize(freeVariables);
    List<Component> components = partition.components();
    int totalComponents = components.size();

    List<ComponentRules> perComponent = new ArrayList<>(totalComponents);
    for (Component component : components) {
      ComponentRules rules = componentRules(component, joinNodes, freeVars, totalComponents);
      if (rules.finalRules().isEmpty()) {
        return null;
      }
      perComponent.add(rules);
    }

    List<KnownComponent> preferred = perComponent.stream().map(ComponentRules::preferred).toList();
    List<Integer> ruleCounts = perComponent.stream().map(r -> r.finalRules().size()).toList();

    return new PartitionAnalysis(perComponent, preferred, ruleCounts);
  }

  public List<List<KnownComponent>> enumerateTuples(PartitionAnalysis analysis, int limit) {
    if (analysis == null) return List.of();

    List<List<KnownComponent>> perComponent =
        analysis.components().stream().map(ComponentRules::finalRules).toList();
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

  private List<KnownComponent> deriveRules(BitSet edgeSubset, Set<String> requestedJoinNodes) {
    if (edgeSubset.isEmpty()) return List.of();

    Set<String> localJoinNodes =
        JoinNodeUtils.localJoinNodes(edgeSubset, edges, requestedJoinNodes);

    CacheKey key =
        new CacheKey(
            BitsetUtils.signature(edgeSubset, edges.size()),
            localJoinNodes,
            requestedJoinNodes,
            edgeSubset.cardinality(),
            edges.size());

    List<KnownComponent> cached = ruleCache.get(key);
    if (cached != null) return cached;

    List<KnownComponent> rules = new ArrayList<>();
    int edgeCount = edgeSubset.cardinality();

    if (edgeCount == 1) {
      rules.addAll(SingleEdgeRuleFactory.build(edges.get(edgeSubset.nextSetBit(0)), edgeSubset));
    } else {
      if (localJoinNodes.size() <= 1) {
        rules.addAll(LoopBacktrackBuilder.build(edges, edgeSubset, localJoinNodes));
      }
      Function<BitSet, List<KnownComponent>> resolver =
          subset -> deriveRules(subset, requestedJoinNodes);
      rules.addAll(CompositeRuleFactory.build(edgeSubset, edges.size(), resolver));
    }

    // --- Merged: expandValidateAndDedup, variantsFor, ensureLoopAnchored, tryReverse, reverse,
    // matchesComponent ---

    Map<ComponentKey, KnownComponent> unique = new LinkedHashMap<>();

    for (KnownComponent rule : rules) {
      // Generate variants for this rule
      List<KnownComponent> variants = generateVariants(rule);

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

  private List<KnownComponent> generateVariants(KnownComponent rule) {
    List<KnownComponent> variants = new ArrayList<>(2);

    // First variant: anchored (if loop)
    KnownComponent candidate = rule;
    if (rule.source().equals(rule.target())) {
      try {
        if (!rule.cpq().toQueryGraph().isLoop()) {
          CPQ anchoredCpq = CPQ.intersect(rule.cpq(), CPQ.IDENTITY);
          candidate =
              new KnownComponent(
                  anchoredCpq,
                  rule.edges(),
                  rule.source(),
                  rule.target(),
                  rule.derivation() + " + anchored with id");
        }
      } catch (RuntimeException ignored) {
        // Keep original if graph extraction fails
      }
    }

    // Add candidate if valid
    if (isValidComponent(candidate)) {
      variants.add(candidate);
    }

    // Second variant: reversed (if not self-loop)
    if (!candidate.source().equals(candidate.target())) {
      KnownComponent reversed = createReversedVariant(candidate);
      if (reversed != null && isValidComponent(reversed)) {
        variants.add(reversed);
      }
    }

    return variants.isEmpty() ? List.of() : List.copyOf(variants);
  }

  private KnownComponent createReversedVariant(KnownComponent rule) {
    try {
      // Use gMark's built-in graph reversal for efficiency and correctness
      QueryGraphCPQ queryGraph = rule.cpq().toQueryGraph();
      QueryGraphCPQ reversedGraph = queryGraph.reverse();
      CPQ reversedCpq = reversedGraph.toCPQ();
      
      return new KnownComponent(
          reversedCpq,
          rule.edges(),
          rule.target(),
          rule.source(),
          rule.derivation() + " + reversed orientation");
    } catch (RuntimeException ex) {
      return null;
    }
  }

  // REMOVED: reverseQueryTree(QueryTree) method - no longer needed

  private boolean isValidComponent(KnownComponent rule) {
    // Get component edges
    List<Edge> componentEdges = new ArrayList<>(rule.edges().cardinality());
    BitsetUtils.stream(rule.edges()).forEach(idx -> componentEdges.add(edges.get(idx)));

    if (componentEdges.isEmpty()) return false;

    // Check endpoints coverage
    Set<String> vertices = new HashSet<>();
    for (Edge edge : componentEdges) {
      vertices.add(edge.source());
      vertices.add(edge.target());
    }
    if (!vertices.contains(rule.source()) || !vertices.contains(rule.target())) return false;

    // Parse rule and check edge count
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

    // Check loop consistency
    boolean ruleIsLoop = rule.source().equals(rule.target());
    if (cpqGraph.isLoop() != ruleIsLoop) return false;

    // Match edges using backtracking
    return matchEdges(cpqEdges, componentEdges, sourceVar, targetVar, rule);
  }

  private boolean matchEdges(
      List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
      List<Edge> componentEdges,
      String sourceVar,
      String targetVar,
      KnownComponent rule) {

    Map<String, String> initialMapping = new HashMap<>();
    Set<String> initialUsedNodes = new HashSet<>();
    initialMapping.put(sourceVar, rule.source());
    initialMapping.put(targetVar, rule.target());
    initialUsedNodes.add(rule.source());
    initialUsedNodes.add(rule.target());

    BitSet initialUsedEdges = new BitSet(componentEdges.size());
    Deque<MatchState> stack = new ArrayDeque<>();
    stack.push(new MatchState(0, initialUsedEdges, initialMapping, initialUsedNodes));

    while (!stack.isEmpty()) {
      MatchState state = stack.pop();
      if (state.index() == cpqEdges.size()) {
        if (state.usedEdges().cardinality() == componentEdges.size()
            && rule.source().equals(state.mapping().get(sourceVar))
            && rule.target().equals(state.mapping().get(targetVar))) {
          return true;
        }
        continue;
      }

      GraphEdge<VarCQ, AtomCQ> cpqEdge = cpqEdges.get(state.index());
      AtomCQ atom = cpqEdge.getData();
      String label = atom.getLabel().getAlias();
      String cpqSrcName = cpqEdge.getSourceNode().getData().getName();
      String cpqTrgName = cpqEdge.getTargetNode().getData().getName();

      for (int edgeIdx = 0; edgeIdx < componentEdges.size(); edgeIdx++) {
        if (state.usedEdges().get(edgeIdx)) continue;

        Edge componentEdge = componentEdges.get(edgeIdx);
        if (!label.equals(componentEdge.label())) continue;

        String mappedSrc = state.mapping().get(cpqSrcName);
        String mappedTrg = state.mapping().get(cpqTrgName);
        if (mappedSrc != null && !mappedSrc.equals(componentEdge.source())) continue;
        if (mappedTrg != null && !mappedTrg.equals(componentEdge.target())) continue;
        if (mappedSrc == null && state.usedNodes().contains(componentEdge.source())) continue;
        if (mappedTrg == null && state.usedNodes().contains(componentEdge.target())) continue;

        Map<String, String> nextMapping = new HashMap<>(state.mapping());
        Set<String> nextUsedNodes = new HashSet<>(state.usedNodes());
        if (mappedSrc == null) {
          nextMapping.put(cpqSrcName, componentEdge.source());
          nextUsedNodes.add(componentEdge.source());
        }
        if (mappedTrg == null) {
          nextMapping.put(cpqTrgName, componentEdge.target());
          nextUsedNodes.add(componentEdge.target());
        }

        BitSet nextUsedEdges = (BitSet) state.usedEdges().clone();
        nextUsedEdges.set(edgeIdx);

        stack.push(new MatchState(state.index() + 1, nextUsedEdges, nextMapping, nextUsedNodes));
      }
    }

    return false;
  }

  private static Set<String> normalize(Set<String> values) {
    return (values == null || values.isEmpty()) ? Set.of() : Set.copyOf(values);
  }

  private record MatchState(
      int index, BitSet usedEdges, Map<String, String> mapping, Set<String> usedNodes) {}

  private record CacheKey(
      String signature, Set<String> joinNodes, Set<String> freeVars, int sizeHintA, int sizeHintB) {
    CacheKey {
      joinNodes = Set.copyOf(joinNodes);
      freeVars = Set.copyOf(freeVars);
    }
  }
}