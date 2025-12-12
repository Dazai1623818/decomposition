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
      CacheStats cacheStats,
      int diameterCap,
      boolean firstHit) {

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

    @SuppressWarnings("unchecked")
    List<CPQExpression>[] expressionsByIndex = new List[totalComponents];

    java.util.stream.IntStream.range(0, totalComponents)
        .parallel()
        .forEach(
            idx -> {
              Component component = components.get(idx);
              ComponentCacheKey key =
                  new ComponentCacheKey(
                      BitsetUtils.signature(component.edgeBits(), edges.size()),
                      joinNodes,
                      freeVars,
                      component.edgeCount(),
                      totalComponents,
                      DecompositionPipelineUtils.hashVarContext(originalVarMap),
                      diameterCap,
                      firstHit);

              CachedComponentExpressions cached =
                  lookupComponentCache(key, componentCache, cacheStats);
              if (cached == null) {
                List<CPQExpression> raw =
                    buildRawExpressions(
                        component.edgeBits(), joinNodes, originalVarMap, diameterCap, firstHit);
                List<CPQExpression> finals =
                    firstHit
                        ? raw
                        : applyOrientationPreferences(raw, joinNodes, freeVars, originalVarMap);
                cached = cacheFinalExpressions(componentCache, key, raw, finals);
              }

              List<CPQExpression> finalExpressions = cached.finalExpressions();
              expressionsByIndex[idx] = finalExpressions;
            });

    for (List<CPQExpression> expressions : expressionsByIndex) {
      if (expressions == null || expressions.isEmpty()) {
        return null;
      }
    }

    return java.util.Arrays.stream(expressionsByIndex).map(List::copyOf).toList();
  }

  public List<CPQExpression> synthesizeGlobal(
      BitSet edgeSubset, Set<String> joinNodes, Map<String, String> originalVarMap) {
    Objects.requireNonNull(edgeSubset, "edgeSubset");
    Objects.requireNonNull(originalVarMap, "originalVarMap");
    return componentBuilder.build(edgeSubset, joinNodes, originalVarMap, 0, false);
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
      BitSet edgeBits,
      Set<String> joinNodes,
      Map<String, String> originalVarMap,
      int diameterCap,
      boolean firstHit) {
    return componentBuilder.build(edgeBits, joinNodes, originalVarMap, diameterCap, firstHit);
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
      List<CPQExpression> finals) {
    CachedComponentExpressions cached =
        new CachedComponentExpressions(
            finals.isEmpty() ? List.of() : List.copyOf(finals),
            !raw.isEmpty(),
            !finals.isEmpty(),
            List.of());
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
      int varContextHash,
      int diameterCap,
      boolean firstHit) {

    public ComponentCacheKey {
      joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
      freeVars = (freeVars == null || freeVars.isEmpty()) ? Set.of() : Set.copyOf(freeVars);
    }
  }
}
