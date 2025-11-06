package decomposition.cpq;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

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
        () ->
            generateConstructionRules(
                edgeSubset, requestedJoinNodes, localJoinNodes, subsetResolver));
  }

  /**
   * Gathers construction rules across all stages, validates them, and returns the deduplicated
   * result.
   */
  private List<KnownComponent> generateConstructionRules(
      BitSet edgeSubset,
      Set<String> requestedJoinNodes,
      Set<String> localJoinNodes,
      Function<BitSet, List<KnownComponent>> subsetResolver) {
    List<KnownComponent> stageRules =
        collectStageConstructionRules(
            edgeSubset, requestedJoinNodes, localJoinNodes, subsetResolver);
    if (stageRules.isEmpty()) {
      return List.of();
    }
    return validateAndDeduplicate(stageRules);
  }

  /**
   * Collects the raw construction rules emitted by the individual construction stages. Each stage
   * handles a distinct shape: single-edge atoms, loop backtracks, and composite joins.
   */
  private List<KnownComponent> collectStageConstructionRules(
      BitSet edgeSubset,
      Set<String> requestedJoinNodes,
      Set<String> localJoinNodes,
      Function<BitSet, List<KnownComponent>> subsetResolver) {
    int edgeCount = edgeSubset.cardinality();
    if (edgeCount == 0) {
      return List.of();
    }
    List<KnownComponent> rules = new ArrayList<>();
    if (edgeCount == 1) {
      rules.addAll(buildSingleEdgeRules(edgeSubset));
    } else {
      rules.addAll(buildLoopBacktrackRules(edgeSubset, localJoinNodes));
      rules.addAll(buildCompositeRules(edgeSubset, subsetResolver));
    }
    return List.copyOf(rules);
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
        () ->
            generateConstructionRules(
                edgeSubset, requestedJoinNodes, localJoinNodes, subsetResolver));
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
