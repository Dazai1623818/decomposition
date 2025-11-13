package decomposition.cpq;

import decomposition.cpq.model.CacheStats;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.BitsetUtils;
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
      Partition partition,
      Set<String> requestedJoinNodes,
      Set<String> freeVariables,
      Map<String, String> originalVarMap,
      Map<ComponentCacheKey, CachedComponentExpressions> componentCache,
      CacheStats cacheStats,
      PartitionDiagnostics partitionDiagnostics) {

    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(originalVarMap, "originalVarMap");
    Objects.requireNonNull(componentCache, "componentCache");
    Objects.requireNonNull(cacheStats, "cacheStats");
    Objects.requireNonNull(partitionDiagnostics, "partitionDiagnostics");

    partitionDiagnostics.beginPartition();
    Set<String> joinNodes = normalize(requestedJoinNodes);
    Set<String> freeVars = normalize(freeVariables);
    List<Component> components = partition.components();
    int totalComponents = components.size();

    List<List<CPQExpression>> finalExpressionsPerComponent = new ArrayList<>(totalComponents);

    for (int idx = 0; idx < totalComponents; idx++) {
      Component component = components.get(idx);
      ComponentCacheKey key =
          new ComponentCacheKey(
              BitsetUtils.signature(component.edgeBits(), edges.size()),
              joinNodes,
              freeVars,
              component.edgeCount(),
              totalComponents,
              varContextHash(originalVarMap));

      CachedComponentExpressions cached = componentCache.get(key);
      if (cached != null) {
        cacheStats.recordHit();
      } else {
        cacheStats.recordMiss();
        List<CPQExpression> raw =
            componentBuilder.build(component.edgeBits(), joinNodes, originalVarMap);
        List<CPQExpression> joinFiltered = raw;
        if (!joinNodes.isEmpty() && component.edgeCount() > 1) {
          Set<String> local = JoinNodeUtils.localJoinNodes(component, joinNodes);
          joinFiltered =
              raw.stream()
                  .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, local))
                  .toList();
        }

        List<CPQExpression> finals = joinFiltered;
        if (joinNodes.size() == 2 && !joinFiltered.isEmpty()) {
          List<String> ordered = new ArrayList<>(joinNodes);
          ordered.sort(
              Comparator.comparing((String node) -> freeVars.contains(node) ? 0 : 1)
                  .thenComparingInt(node -> getOriginalVarOrder(node, originalVarMap))
                  .thenComparing(node -> node));
          String preferredSource = ordered.get(0);
          String preferredTarget = ordered.get(1);

          List<CPQExpression> oriented =
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

        cached =
            new CachedComponentExpressions(
                finals.isEmpty() ? List.of() : List.copyOf(finals),
                !raw.isEmpty(),
                !joinFiltered.isEmpty());
        componentCache.put(key, cached);
      }

      String componentSig = BitsetUtils.signature(component.edgeBits(), edges.size());
      partitionDiagnostics.recordComponent(
          idx + 1,
          componentSig,
          cached.hasRawExpressions(),
          cached.hasJoinFilteredExpressions(),
          joinNodes.isEmpty());

      List<CPQExpression> finalExpressions = cached.finalExpressions();
      if (finalExpressions.isEmpty()) {
        partitionDiagnostics.failPartition();
        return null;
      }
      finalExpressionsPerComponent.add(finalExpressions);
    }

    partitionDiagnostics.succeedPartition();
    return List.copyOf(finalExpressionsPerComponent);
  }

  public List<CPQExpression> synthesizeGlobal(
      BitSet edgeSubset, Set<String> joinNodes, Map<String, String> originalVarMap) {
    Objects.requireNonNull(edgeSubset, "edgeSubset");
    Objects.requireNonNull(originalVarMap, "originalVarMap");
    return componentBuilder.build(edgeSubset, joinNodes, originalVarMap);
  }

  public static List<List<CPQExpression>> enumerateTuples(
      List<List<CPQExpression>> perComponent, int limit) {
    if (perComponent == null || perComponent.isEmpty()) {
      return List.of();
    }

    int n = perComponent.size();
    int[] idx = new int[n];
    List<List<CPQExpression>> out = new ArrayList<>();
    while (true) {
      List<CPQExpression> tuple = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        tuple.add(perComponent.get(i).get(idx[i]));
      }
      out.add(tuple);
      if (limit > 0 && out.size() >= limit) {
        break;
      }

      int p = n - 1;
      while (p >= 0) {
        idx[p]++;
        if (idx[p] < perComponent.get(p).size()) {
          break;
        }
        idx[p] = 0;
        p--;
      }
      if (p < 0) {
        break;
      }
    }
    return out;
  }

  public record CachedComponentExpressions(
      List<CPQExpression> finalExpressions,
      boolean hasRawExpressions,
      boolean hasJoinFilteredExpressions) {

    public CachedComponentExpressions {
      finalExpressions = List.copyOf(Objects.requireNonNull(finalExpressions, "finalExpressions"));
    }
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

  private static int getOriginalVarOrder(String node, Map<String, String> originalVarMap) {
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
}
