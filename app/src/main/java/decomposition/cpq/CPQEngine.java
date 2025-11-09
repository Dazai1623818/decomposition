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
import dev.roanh.gmark.ast.QueryTree;
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

    ComponentRules computed = buildComponentRules(component, joinNodes, freeVars, totalComponents);
    componentCache.put(key, computed);
    cacheStats.recordMiss();
    return computed;
  }

  private ComponentRules buildComponentRules(
      Component component, Set<String> joinNodes, Set<String> freeVars, int totalComponents) {
    List<KnownComponent> raw = deriveRules(component.edgeBits(), joinNodes);

    List<KnownComponent> joinFiltered = raw;
    if (shouldEnforceJoinNodes(joinNodes, totalComponents, component)) {
      Set<String> local = JoinNodeUtils.localJoinNodes(component, joinNodes);
      joinFiltered =
          raw.stream()
              .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, local))
              .toList();
    }

    List<KnownComponent> oriented = preferCanonicalOrientation(joinFiltered, joinNodes, freeVars);
    List<KnownComponent> finals = oriented.isEmpty() ? joinFiltered : oriented;

    return new ComponentRules(component, raw, joinFiltered, finals);
  }

  // --------------------------------------------------------------------------
  // Partition analysis + enumeration
  // --------------------------------------------------------------------------

  /**
   * Returns a {@link PartitionAnalysis} for the partition or {@code null} if any component lacks
   * viable rules.
   */
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
    List<Integer> ruleCounts =
        perComponent.stream().map(rules -> rules.finalRules().size()).toList();

    return new PartitionAnalysis(perComponent, preferred, ruleCounts);
  }

  public List<List<KnownComponent>> enumerateTuples(PartitionAnalysis analysis, int limit) {
    if (analysis == null) {
      return List.of();
    }
    List<List<KnownComponent>> perComponent =
        analysis.components().stream().map(ComponentRules::finalRules).toList();
    if (perComponent.isEmpty()) {
      return List.of();
    }
    List<List<KnownComponent>> out = new ArrayList<>();
    backtrack(perComponent, 0, new ArrayList<>(), out, limit);
    return out;
  }

  private void backtrack(
      List<List<KnownComponent>> lists,
      int index,
      List<KnownComponent> current,
      List<List<KnownComponent>> out,
      int limit) {
    if (limit > 0 && out.size() >= limit) {
      return;
    }
    if (index == lists.size()) {
      out.add(List.copyOf(current));
      return;
    }
    for (KnownComponent kc : lists.get(index)) {
      current.add(kc);
      backtrack(lists, index + 1, current, out, limit);
      current.remove(current.size() - 1);
      if (limit > 0 && out.size() >= limit) {
        return;
      }
    }
  }

  // --------------------------------------------------------------------------
  // Core recursive derivation
  // --------------------------------------------------------------------------

  private List<KnownComponent> deriveRules(BitSet edgeSubset, Set<String> requestedJoinNodes) {
    if (edgeSubset.isEmpty()) {
      return List.of();
    }

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
    if (cached != null) {
      return cached;
    }

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

    List<KnownComponent> result = rules.isEmpty() ? List.of() : validateAndDeduplicate(rules);
    ruleCache.put(key, result);
    return result;
  }

  private List<KnownComponent> validateAndDeduplicate(List<KnownComponent> rawRules) {
    Map<ComponentKey, KnownComponent> unique = new LinkedHashMap<>();
    for (KnownComponent rule : rawRules) {
      for (KnownComponent variant : validateAndExpand(rule)) {
        ComponentKey key = new ComponentKey(variant.edges(), variant.source(), variant.target());
        unique.putIfAbsent(key, variant);
      }
    }
    return unique.isEmpty() ? List.of() : List.copyOf(unique.values());
  }

  private List<KnownComponent> validateAndExpand(KnownComponent rule) {
    List<KnownComponent> variants = new ArrayList<>(2);
    KnownComponent anchored = ensureLoopAnchored(rule);
    if (matchesComponent(anchored)) {
      variants.add(anchored);
    }
    if (!anchored.source().equals(anchored.target())) {
      KnownComponent reversed = reverseRule(anchored);
      if (reversed != null) {
        KnownComponent anchoredReverse = ensureLoopAnchored(reversed);
        if (matchesComponent(anchoredReverse)) {
          variants.add(anchoredReverse);
        }
      }
    }
    return variants.isEmpty() ? List.of() : List.copyOf(variants);
  }

  private KnownComponent ensureLoopAnchored(KnownComponent rule) {
    if (!rule.source().equals(rule.target())) {
      return rule;
    }
    try {
      if (rule.cpq().toQueryGraph().isLoop()) {
        return rule;
      }
    } catch (RuntimeException ex) {
      return rule;
    }
    CPQ anchored = CPQ.intersect(rule.cpq(), CPQ.IDENTITY);
    return new KnownComponent(
        anchored,
        rule.edges(),
        rule.source(),
        rule.target(),
        rule.derivation() + " + anchored with id");
  }

  private KnownComponent reverseRule(KnownComponent rule) {
    try {
      CPQ reversed = reverseTree(rule.cpq().toAbstractSyntaxTree());
      return new KnownComponent(
          reversed,
          rule.edges(),
          rule.target(),
          rule.source(),
          rule.derivation() + " + reversed orientation");
    } catch (RuntimeException ex) {
      return null;
    }
  }

  private CPQ reverseTree(QueryTree tree) {
    return switch (tree.getOperation()) {
      case EDGE -> CPQ.label(tree.getEdgeAtom().getLabel().getInverse());
      case IDENTITY -> CPQ.IDENTITY;
      case CONCATENATION ->
          CPQ.concat(reverseTree(tree.getOperand(1)), reverseTree(tree.getOperand(0)));
      case INTERSECTION ->
          CPQ.intersect(reverseTree(tree.getOperand(0)), reverseTree(tree.getOperand(1)));
      default ->
          throw new IllegalArgumentException("Unsupported CPQ operation: " + tree.getOperation());
    };
  }

  private boolean matchesComponent(KnownComponent rule) {
    List<Edge> componentEdges = edgesFor(rule.edges());
    if (componentEdges.isEmpty() || !coversEndpoints(rule, componentEdges)) {
      return false;
    }

    ParsedRule parsed = parseRule(rule);
    if (parsed == null || parsed.cpqEdges().size() != componentEdges.size()) {
      return false;
    }

    boolean ruleIsLoop = rule.source().equals(rule.target());
    if (parsed.cpqGraph().isLoop() != ruleIsLoop) {
      return false;
    }

    return matchEdges(parsed, componentEdges, rule);
  }

  private boolean coversEndpoints(KnownComponent rule, List<Edge> componentEdges) {
    Set<String> vertices = new HashSet<>();
    for (Edge edge : componentEdges) {
      vertices.add(edge.source());
      vertices.add(edge.target());
    }
    return vertices.contains(rule.source()) && vertices.contains(rule.target());
  }

  private ParsedRule parseRule(KnownComponent rule) {
    try {
      CPQ cpq = rule.cpq();
      CQ cqPattern = cpq.toCQ();
      QueryGraphCQ cqGraph = cqPattern.toQueryGraph();
      UniqueGraph<VarCQ, AtomCQ> graph = cqGraph.toUniqueGraph();
      QueryGraphCPQ cpqGraph = cpq.toQueryGraph();
      String sourceVarName = cpqGraph.getVertexLabel(cpqGraph.getSourceVertex());
      String targetVarName = cpqGraph.getVertexLabel(cpqGraph.getTargetVertex());
      return new ParsedRule(graph.getEdges(), cpqGraph, sourceVarName, targetVarName);
    } catch (RuntimeException ex) {
      return null;
    }
  }

  private List<Edge> edgesFor(BitSet bits) {
    List<Edge> selected = new ArrayList<>(bits.cardinality());
    BitsetUtils.stream(bits).forEach(idx -> selected.add(edges.get(idx)));
    return selected;
  }

  private boolean matchEdges(ParsedRule parsed, List<Edge> componentEdges, KnownComponent rule) {
    List<GraphEdge<VarCQ, AtomCQ>> cpqEdges = parsed.cpqEdges();
    if (cpqEdges.size() != componentEdges.size()) {
      return false;
    }

    Map<String, String> initialMapping = new HashMap<>();
    Set<String> initialUsedNodes = new HashSet<>();
    initialMapping.put(parsed.sourceVar(), rule.source());
    initialMapping.put(parsed.targetVar(), rule.target());
    initialUsedNodes.add(rule.source());
    initialUsedNodes.add(rule.target());

    BitSet initialUsedEdges = new BitSet(componentEdges.size());
    Deque<MatchState> stack = new ArrayDeque<>();
    stack.push(new MatchState(0, initialUsedEdges, initialMapping, initialUsedNodes));

    while (!stack.isEmpty()) {
      MatchState state = stack.pop();
      if (state.index() == cpqEdges.size()) {
        if (state.usedEdges().cardinality() == componentEdges.size()
            && rule.source().equals(state.mapping().get(parsed.sourceVar()))
            && rule.target().equals(state.mapping().get(parsed.targetVar()))) {
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
        if (state.usedEdges().get(edgeIdx)) {
          continue;
        }
        Edge componentEdge = componentEdges.get(edgeIdx);
        if (!label.equals(componentEdge.label())) {
          continue;
        }

        String mappedSrc = state.mapping().get(cpqSrcName);
        String mappedTrg = state.mapping().get(cpqTrgName);
        if (mappedSrc != null && !mappedSrc.equals(componentEdge.source())) {
          continue;
        }
        if (mappedTrg != null && !mappedTrg.equals(componentEdge.target())) {
          continue;
        }
        if (mappedSrc == null && state.usedNodes().contains(componentEdge.source())) {
          continue;
        }
        if (mappedTrg == null && state.usedNodes().contains(componentEdge.target())) {
          continue;
        }

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

  private boolean shouldEnforceJoinNodes(
      Set<String> joinNodes, int totalComponents, Component component) {
    return !joinNodes.isEmpty() && (totalComponents > 1 || component.edgeCount() > 1);
  }

  private List<KnownComponent> preferCanonicalOrientation(
      List<KnownComponent> rules, Set<String> joinNodes, Set<String> freeVariables) {
    if (rules.isEmpty() || joinNodes.size() != 2) {
      return List.of();
    }
    List<String> ordered = new ArrayList<>(joinNodes);
    ordered.sort(
        Comparator.comparing((String node) -> freeVariables.contains(node) ? 0 : 1)
            .thenComparing(node -> node));

    String preferredSource = ordered.get(0);
    String preferredTarget = ordered.get(1);

    return rules.stream()
        .filter(
            rule -> preferredSource.equals(rule.source()) && preferredTarget.equals(rule.target()))
        .toList();
  }

  private Set<String> normalize(Set<String> values) {
    return (values == null || values.isEmpty()) ? Set.of() : Set.copyOf(values);
  }

  private record ParsedRule(
      List<GraphEdge<VarCQ, AtomCQ>> cpqEdges,
      QueryGraphCPQ cpqGraph,
      String sourceVar,
      String targetVar) {}

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
