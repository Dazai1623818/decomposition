package decomposition.cpq;

import decomposition.model.Edge;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Builds CPQ expressions for connected components using gMark's CPQ model. */
public final class ComponentCPQBuilder {
  private final List<Edge> edges;
  private final Map<MemoKey, List<KnownComponent>> memo = new HashMap<>();
  private final ComponentCandidateValidator candidateValidator;

  public ComponentCPQBuilder(List<Edge> edges) {
    this.edges = List.copyOf(edges);
    this.candidateValidator = new ComponentCandidateValidator(this.edges);
  }

  public List<Edge> allEdges() {
    return edges;
  }

  public List<KnownComponent> options(BitSet edgeBits) {
    return options(edgeBits, Set.of());
  }

  public List<KnownComponent> options(BitSet edgeBits, Set<String> joinNodes) {
    Objects.requireNonNull(edgeBits, "edgeBits");
    Set<String> normalizedJoinNodes =
        joinNodes == null || joinNodes.isEmpty() ? Set.of() : Set.copyOf(joinNodes);
    return enumerate(edgeBits, normalizedJoinNodes);
  }

  private List<KnownComponent> enumerate(BitSet edgeBits, Set<String> joinNodes) {
    Set<String> localJoinNodes = JoinNodeUtils.localJoinNodes(edgeBits, edges, joinNodes);
    MemoKey key = new MemoKey(BitsetUtils.signature(edgeBits, edges.size()), localJoinNodes);
    List<KnownComponent> cached = memo.get(key);
    if (cached != null) {
      return cached;
    }
    List<KnownComponent> computed = collectOptions(edgeBits, joinNodes, localJoinNodes);
    memo.put(key, computed);
    return computed;
  }

  private List<KnownComponent> collectOptions(
      BitSet edgeBits, Set<String> originalJoinNodes, Set<String> localJoinNodes) {
    int cardinality = edgeBits.cardinality();
    if (cardinality == 0) {
      return List.of();
    }

    Map<ComponentKey, KnownComponent> results = new LinkedHashMap<>();
    if (cardinality == 1) {
      addSingleEdgeOptions(edgeBits, results);
    } else {
      addLoopBacktrackOptions(edgeBits, localJoinNodes, results);
      addCompositeOptions(edgeBits, originalJoinNodes, results);
    }
    return List.copyOf(results.values());
  }

  private void addSingleEdgeOptions(BitSet edgeBits, Map<ComponentKey, KnownComponent> results) {
    int edgeIndex = edgeBits.nextSetBit(0);
    Edge edge = edges.get(edgeIndex);
    SingleEdgeOptionFactory.build(edge, edgeBits).forEach(option -> tryAdd(results, option));
  }

  private void addLoopBacktrackOptions(
      BitSet edgeBits, Set<String> localJoinNodes, Map<ComponentKey, KnownComponent> results) {
    if (localJoinNodes.size() > 1) {
      return;
    }
    LoopBacktrackBuilder.build(edges, edgeBits, localJoinNodes)
        .forEach(option -> tryAdd(results, option));
  }

  private void addCompositeOptions(
      BitSet edgeBits, Set<String> originalJoinNodes, Map<ComponentKey, KnownComponent> results) {
    Function<BitSet, List<KnownComponent>> lookup = subset -> enumerate(subset, originalJoinNodes);
    CompositeOptionFactory.build(
        edgeBits, edges.size(), lookup, candidate -> tryAdd(results, candidate));
  }

  private void tryAdd(Map<ComponentKey, KnownComponent> results, KnownComponent candidate) {
    for (KnownComponent variant : candidateValidator.validateAndExpand(candidate)) {
      ComponentKey key = variant.toKey(edges.size());
      results.putIfAbsent(key, variant);
    }
  }

  private static final class MemoKey {
    private final String signature;
    private final Set<String> anchors;

    MemoKey(String signature, Set<String> anchors) {
      this.signature = signature;
      this.anchors = anchors;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MemoKey other)) {
        return false;
      }
      return signature.equals(other.signature) && anchors.equals(other.anchors);
    }

    @Override
    public int hashCode() {
      return Objects.hash(signature, anchors);
    }
  }
}
