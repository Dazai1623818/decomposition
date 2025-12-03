package decomposition.cpq;

import decomposition.core.model.Component;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.cpq.model.CacheStats;
import decomposition.pipeline.partitioning.FilteredPartition;
import decomposition.util.BitsetUtils;
import decomposition.util.DecompositionPipelineUtils;
import decomposition.util.JoinAnalysis;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Synthesizes complete expression sets for partitions by orchestrating per-component building,
 * join-node filtering, and orientation logic.
 */
public final class PartitionExpressionAssembler {
  private final List<Edge> edges;
  private final ComponentExpressionBuilder componentBuilder;

  public PartitionExpressionAssembler(List<Edge> edges) {
    List<Edge> safeEdges = List.copyOf(Objects.requireNonNull(edges, "edges"));
    this.edges = safeEdges;
    this.componentBuilder = new ComponentExpressionBuilder(safeEdges);
  }

  PartitionExpressionAssembler(List<Edge> edges, ComponentExpressionBuilder componentBuilder) {
    this.edges = List.copyOf(Objects.requireNonNull(edges, "edges"));
    this.componentBuilder = Objects.requireNonNull(componentBuilder, "componentBuilder");
  }

  public List<List<CPQExpression>> synthesize(
      FilteredPartition filteredPartition,
      Set<String> freeVariables,
      Map<String, String> originalVarMap,
      Map<ComponentCacheKey, CachedComponentExpressions> componentCache,
      CacheStats cacheStats) {

    Objects.requireNonNull(filteredPartition, "filteredPartition");
    Objects.requireNonNull(originalVarMap, "originalVarMap");
    Objects.requireNonNull(componentCache, "componentCache");
    Objects.requireNonNull(cacheStats, "cacheStats");
    Partition partition = filteredPartition.partition();
    JoinAnalysis joinAnalysis =
        Objects.requireNonNull(filteredPartition.joinAnalysis(), "joinAnalysis");
    Set<String> joinNodes = JoinNodeUtils.normalizeNodeSet(joinAnalysis.globalJoinNodes());
    Set<String> freeVars = JoinNodeUtils.normalizeNodeSet(freeVariables);
    List<Component> components = partition.components();
    int totalComponents = components.size();

    List<List<CPQExpression>> finalExpressionsPerComponent = new ArrayList<>(totalComponents);

    for (int idx = 0; idx < totalComponents; idx++) {
      Component component = components.get(idx);
      Set<String> localJoinNodes = filteredPartition.joinNodesForComponent(component);
      ComponentCacheKey key =
          new ComponentCacheKey(
              BitsetUtils.signature(component.edgeBits(), edges.size()),
              joinNodes,
              freeVars,
              component.edgeCount(),
              totalComponents,
              DecompositionPipelineUtils.hashVarContext(originalVarMap));

      CachedComponentExpressions cached = lookupComponentCache(key, componentCache, cacheStats);
      if (cached == null) {
        List<CPQExpression> raw =
            buildRawExpressions(component.edgeBits(), joinNodes, originalVarMap);
        List<CPQExpression> joinFiltered = applyJoinFiltering(raw, component, localJoinNodes);
        List<CPQExpression> finals =
            applyOrientationPreferences(joinFiltered, joinNodes, freeVars, originalVarMap);
        cached =
            cacheFinalExpressions(
                componentCache, key, raw, joinFiltered, finals, localJoinNodes.isEmpty());
      }

      List<CPQExpression> finalExpressions = cached.finalExpressions();
      if (finalExpressions.isEmpty()) {
        return null;
      }
      finalExpressionsPerComponent.add(finalExpressions);
    }

    return List.copyOf(finalExpressionsPerComponent);
  }

  public List<CPQExpression> synthesizeGlobal(
      BitSet edgeSubset, Set<String> joinNodes, Map<String, String> originalVarMap) {
    Objects.requireNonNull(edgeSubset, "edgeSubset");
    Objects.requireNonNull(originalVarMap, "originalVarMap");
    return componentBuilder.build(edgeSubset, joinNodes, originalVarMap);
  }

  public record CachedComponentExpressions(
      List<CPQExpression> finalExpressions,
      boolean hasRawExpressions,
      boolean hasJoinFilteredExpressions,
      List<CPQExpression> diagnosticCandidates) {

    public CachedComponentExpressions {
      finalExpressions = List.copyOf(Objects.requireNonNull(finalExpressions, "finalExpressions"));
      diagnosticCandidates =
          diagnosticCandidates == null || diagnosticCandidates.isEmpty()
              ? List.of()
              : List.copyOf(diagnosticCandidates);
    }
  }

  private CachedComponentExpressions lookupComponentCache(
      ComponentCacheKey key,
      Map<ComponentCacheKey, CachedComponentExpressions> componentCache,
      CacheStats cacheStats) {
    CachedComponentExpressions cached = componentCache.get(key);
    if (cached != null) {
      cacheStats.recordHit();
    } else {
      cacheStats.recordMiss();
    }
    return cached;
  }

  private List<CPQExpression> buildRawExpressions(
      BitSet edgeBits, Set<String> joinNodes, Map<String, String> originalVarMap) {
    return componentBuilder.build(edgeBits, joinNodes, originalVarMap);
  }

  private List<CPQExpression> applyJoinFiltering(
      List<CPQExpression> raw, Component component, Set<String> localJoinNodes) {
    if (localJoinNodes.isEmpty() || component.edgeCount() <= 1) {
      return raw;
    }
    return raw.stream()
        .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes))
        .toList();
  }

  private List<CPQExpression> applyOrientationPreferences(
      List<CPQExpression> joinFiltered,
      Set<String> joinNodes,
      Set<String> freeVars,
      Map<String, String> originalVarMap) {
    if (joinNodes.size() != 2 || joinFiltered.isEmpty()) {
      return joinFiltered;
    }
    List<String> ordered = new ArrayList<>(joinNodes);
    ordered.sort(
        Comparator.comparing((String node) -> freeVars.contains(node) ? 0 : 1)
            .thenComparingInt(node -> JoinNodeUtils.originalVariableOrder(node, originalVarMap))
            .thenComparing(node -> node));
    String preferredSource = ordered.get(0);
    String preferredTarget = ordered.get(1);

    List<CPQExpression> oriented =
        joinFiltered.stream()
            .filter(
                rule ->
                    preferredSource.equals(rule.source()) && preferredTarget.equals(rule.target()))
            .toList();
    return oriented.isEmpty() ? joinFiltered : oriented;
  }

  private CachedComponentExpressions cacheFinalExpressions(
      Map<ComponentCacheKey, CachedComponentExpressions> componentCache,
      ComponentCacheKey key,
      List<CPQExpression> raw,
      List<CPQExpression> joinFiltered,
      List<CPQExpression> finals,
      boolean localJoinNodesEmpty) {
    List<CPQExpression> diagnosticCandidates =
        (!localJoinNodesEmpty && joinFiltered.isEmpty() && !raw.isEmpty())
            ? List.copyOf(raw)
            : List.of();
    CachedComponentExpressions cached =
        new CachedComponentExpressions(
            finals.isEmpty() ? List.of() : List.copyOf(finals),
            !raw.isEmpty(),
            !joinFiltered.isEmpty(),
            diagnosticCandidates);
    componentCache.put(key, cached);
    return cached;
  }

  /** Key used for memoizing cached component expression sets. */
  public static record ComponentCacheKey(
      String signature,
      Set<String> joinNodes,
      Set<String> freeVars,
      int componentSize,
      int totalComponents,
      int varContextHash) {

    public ComponentCacheKey {
      joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
      freeVars = (freeVars == null || freeVars.isEmpty()) ? Set.of() : Set.copyOf(freeVars);
    }
  }
}
