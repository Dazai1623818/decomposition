package decomposition.cpq;

import decomposition.model.Component;
import decomposition.model.Edge;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Builds CPQ expressions for connected components using gMark's CPQ model. */
public final class ComponentCPQBuilder {
  private final List<Edge> edges;
  private final ComponentRuleValidator ruleValidator;
  private final Map<MemoKey, MemoizedRuleSet> memoizedRules = new ConcurrentHashMap<>();

  public ComponentCPQBuilder(List<Edge> edges) {
    this.edges = List.copyOf(edges);
    this.ruleValidator = new ComponentRuleValidator(this.edges);
  }

  public List<KnownComponent> constructionRules(BitSet edgeSubset) {
    return constructionRules(edgeSubset, Set.of());
  }

  public List<KnownComponent> constructionRules(BitSet edgeSubset, Set<String> requestedJoinNodes) {
    Objects.requireNonNull(edgeSubset, "edgeSubset");
    Set<String> normalizedJoinNodes =
        requestedJoinNodes == null || requestedJoinNodes.isEmpty()
            ? Set.of()
            : Set.copyOf(requestedJoinNodes);
    return lookupConstructionRules(edgeSubset, normalizedJoinNodes);
  }

  /**
   * Returns the cached rule bundle for a component, including raw, join-filtered, and preferred
   * orientations.
   *
   * <p>The inputs {@code requestedJoinNodes} and {@code freeVariables} should already be normalized
   * (non-null, immutable) upstream.
   */
  public ComponentRuleSet componentRules(
      Component component,
      Set<String> requestedJoinNodes,
      Set<String> freeVariables,
      int totalComponents) {
    Objects.requireNonNull(component, "component");
    Objects.requireNonNull(requestedJoinNodes, "requestedJoinNodes");
    Objects.requireNonNull(freeVariables, "freeVariables");

    BitSet componentEdges = component.edgeBits();
    List<KnownComponent> rawRules = lookupConstructionRules(componentEdges, requestedJoinNodes);
    List<KnownComponent> joinFilteredRules = rawRules;
    if (shouldEnforceJoinNodes(requestedJoinNodes, totalComponents, component)) {
      Set<String> localJoinNodes = JoinNodeUtils.localJoinNodes(component, requestedJoinNodes);
      joinFilteredRules =
          rawRules.stream()
              .filter(
                  kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes))
              .collect(Collectors.toList());
    }

    List<KnownComponent> oriented =
        preferCanonicalOrientation(joinFilteredRules, requestedJoinNodes, freeVariables);
    List<KnownComponent> finalRules = oriented.isEmpty() ? joinFilteredRules : oriented;
    return new ComponentRuleSet(rawRules, joinFilteredRules, finalRules);
  }

  /**
   * Returns the memoized construction rule list for the given edge subset and join-node
   * requirements.
   *
   * @param edgeSubset edges that define the subgraph
   * @param requestedJoinNodes join nodes that must be covered by the component
   */
  private List<KnownComponent> lookupConstructionRules(
      BitSet edgeSubset, Set<String> requestedJoinNodes) {
    Set<String> localJoinNodes =
        JoinNodeUtils.localJoinNodes(edgeSubset, edges, requestedJoinNodes);
    MemoKey key = new MemoKey(BitsetUtils.signature(edgeSubset, edges.size()), localJoinNodes);
    MemoizedRuleSet entry = memoizedRules.computeIfAbsent(key, unused -> new MemoizedRuleSet());
    Function<BitSet, List<KnownComponent>> subsetResolver =
        subset -> resolveSubgraphWithCache(subset, requestedJoinNodes);
    return entry.resolve(
        () -> generateConstructionRules(edgeSubset, localJoinNodes, subsetResolver));
  }

  /**
   * Gathers construction rules across all stages, validates them, and returns the deduplicated
   * result.
   */
  private List<KnownComponent> generateConstructionRules(
      BitSet edgeSubset,
      Set<String> localJoinNodes,
      Function<BitSet, List<KnownComponent>> subsetResolver) {
    int edgeCount = edgeSubset.cardinality();
    if (edgeCount == 0) {
      return List.of();
    }
    List<KnownComponent> rawRules = new ArrayList<>();
    if (edgeCount == 1) {
      rawRules.addAll(buildSingleEdgeRules(edgeSubset));
    } else {
      rawRules.addAll(buildLoopBacktrackRules(edgeSubset, localJoinNodes));
      rawRules.addAll(buildCompositeRules(edgeSubset, subsetResolver));
    }
    if (rawRules.isEmpty()) {
      return List.of();
    }
    return validateAndDeduplicate(List.copyOf(rawRules));
  }

  /**
   * Validates raw construction rules, expands them as needed, and merges duplicates while
   * preserving emission order.
   */
  private List<KnownComponent> validateAndDeduplicate(List<KnownComponent> rawRules) {
    ComponentDeduplicator deduplicator = new ComponentDeduplicator();
    for (KnownComponent rule : rawRules) {
      for (KnownComponent variant : ruleValidator.validateAndExpand(rule)) {
        deduplicator.include(variant);
      }
    }
    return deduplicator.snapshot();
  }

  private List<KnownComponent> buildSingleEdgeRules(BitSet edgeSubset) {
    int edgeIndex = edgeSubset.nextSetBit(0);
    Edge edge = edges.get(edgeIndex);
    return SingleEdgeRuleFactory.build(edge, edgeSubset);
  }

  private List<KnownComponent> buildLoopBacktrackRules(
      BitSet edgeSubset, Set<String> localJoinNodes) {
    if (localJoinNodes.size() > 1) {
      return List.of();
    }
    return LoopBacktrackBuilder.build(edges, edgeSubset, localJoinNodes);
  }

  private List<KnownComponent> buildCompositeRules(
      BitSet edgeSubset, Function<BitSet, List<KnownComponent>> subsetResolver) {
    return CompositeRuleFactory.build(edgeSubset, edges.size(), subsetResolver);
  }

  private boolean shouldEnforceJoinNodes(
      Set<String> joinNodes, int totalComponents, Component component) {
    if (joinNodes.isEmpty()) {
      return false;
    }
    if (totalComponents > 1) {
      return true;
    }
    return component.edgeCount() > 1;
  }

  private List<KnownComponent> preferCanonicalOrientation(
      List<KnownComponent> rules, Set<String> joinNodes, Set<String> freeVariables) {
    if (rules.isEmpty() || joinNodes.size() != 2) {
      return List.of();
    }

    List<String> orderedJoinNodes = new ArrayList<>(joinNodes);
    orderedJoinNodes.sort(
        Comparator.comparing((String node) -> freeVariables.contains(node) ? 0 : 1)
            .thenComparing(node -> node));

    String preferredSource = orderedJoinNodes.get(0);
    String preferredTarget = orderedJoinNodes.get(1);

    return rules.stream()
        .filter(
            rule -> preferredSource.equals(rule.source()) && preferredTarget.equals(rule.target()))
        .collect(Collectors.toList());
  }

  /**
   * Resolves component construction rules for a subgraph while honoring the memoized cache and
   * avoiding nested computeIfAbsent recursion.
   */
  private List<KnownComponent> resolveSubgraphWithCache(
      BitSet edgeSubset, Set<String> requestedJoinNodes) {
    Set<String> localJoinNodes =
        JoinNodeUtils.localJoinNodes(edgeSubset, edges, requestedJoinNodes);
    MemoKey key = new MemoKey(BitsetUtils.signature(edgeSubset, edges.size()), localJoinNodes);
    MemoizedRuleSet entry = memoizedRules.computeIfAbsent(key, unused -> new MemoizedRuleSet());
    Function<BitSet, List<KnownComponent>> subsetResolver =
        subset -> resolveSubgraphWithCache(subset, requestedJoinNodes);
    return entry.resolve(
        () -> generateConstructionRules(edgeSubset, localJoinNodes, subsetResolver));
  }

  /** Deduplicates validated construction rules based on their structural signature. */
  private final class ComponentDeduplicator {
    private final Map<ComponentKey, KnownComponent> unique = new LinkedHashMap<>();

    void include(KnownComponent rule) {
      ComponentKey key = rule.toKey(edges.size());
      unique.putIfAbsent(key, rule);
    }

    List<KnownComponent> snapshot() {
      return List.copyOf(unique.values());
    }
  }

  public static final class ComponentRuleSet {
    private final List<KnownComponent> rawRules;
    private final List<KnownComponent> joinFilteredRules;
    private final List<KnownComponent> finalRules;

    public ComponentRuleSet(
        List<KnownComponent> rawRules,
        List<KnownComponent> joinFilteredRules,
        List<KnownComponent> finalRules) {
      this.rawRules = List.copyOf(Objects.requireNonNull(rawRules, "rawRules"));
      this.joinFilteredRules =
          List.copyOf(Objects.requireNonNull(joinFilteredRules, "joinFilteredRules"));
      this.finalRules = List.copyOf(Objects.requireNonNull(finalRules, "finalRules"));
    }

    public List<KnownComponent> rawRules() {
      return rawRules;
    }

    public List<KnownComponent> joinFilteredRules() {
      return joinFilteredRules;
    }

    public List<KnownComponent> finalRules() {
      return finalRules;
    }
  }

  private static final class MemoizedRuleSet {
    private volatile List<KnownComponent> value;

    List<KnownComponent> resolve(Supplier<List<KnownComponent>> computer) {
      List<KnownComponent> resolved = value;
      if (resolved != null) {
        return resolved;
      }
      synchronized (this) {
        if (value == null) {
          value = Objects.requireNonNull(computer.get(), "computedComponents");
        }
        return value;
      }
    }
  }

  private record MemoKey(String signature, Set<String> joinNodes) {}
}
