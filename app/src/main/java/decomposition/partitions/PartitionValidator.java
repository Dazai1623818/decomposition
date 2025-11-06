package decomposition.partitions;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Validates partitions against the CPQ builder and enumerates component combinations. */
public final class PartitionValidator {
  private final Map<ComponentRuleCacheKey, ComponentCPQBuilder.ComponentRuleSet> ruleCache =
      new HashMap<>();
  private final ComponentRuleCacheStats cacheStats;

  public PartitionValidator() {
    this(new ComponentRuleCacheStats());
  }

  public PartitionValidator(ComponentRuleCacheStats cacheStats) {
    this.cacheStats = Objects.requireNonNull(cacheStats, "cacheStats");
  }

  public boolean isValidCPQDecomposition(
      Partition partition,
      Set<String> joinNodes,
      ComponentCPQBuilder builder,
      Set<String> freeVariables,
      List<Edge> allEdges) {
    return componentConstructionRules(partition, joinNodes, builder, freeVariables, allEdges)
        .stream()
        .allMatch(ComponentConstructionRules::hasRules);
  }

  public List<List<KnownComponent>> enumerateDecompositions(
      Partition partition,
      ComponentCPQBuilder builder,
      int limit,
      Set<String> freeVariables,
      List<Edge> allEdges) {
    return enumerateDecompositions(
        partition,
        JoinNodeUtils.computeJoinNodes(partition.components(), freeVariables),
        builder,
        limit,
        freeVariables,
        allEdges);
  }

  public List<List<KnownComponent>> enumerateDecompositions(
      Partition partition,
      Set<String> joinNodes,
      ComponentCPQBuilder builder,
      int limit,
      Set<String> freeVariables,
      List<Edge> allEdges) {
    List<ComponentConstructionRules> perComponentRules =
        componentConstructionRules(partition, joinNodes, builder, freeVariables, allEdges);
    return enumerateDecompositions(perComponentRules, limit);
  }

  public List<List<KnownComponent>> enumerateDecompositions(
      List<ComponentConstructionRules> componentRules, int limit) {
    if (componentRules.stream().anyMatch(rules -> rules.finalRules().isEmpty())) {
      return List.of();
    }
    List<List<KnownComponent>> perComponentRuleLists =
        componentRules.stream()
            .map(ComponentConstructionRules::finalRules)
            .collect(Collectors.toList());
    return cartesian(perComponentRuleLists, limit);
  }

  public List<ComponentConstructionRules> componentConstructionRules(
      Partition partition,
      Set<String> joinNodes,
      ComponentCPQBuilder builder,
      Set<String> freeVariables,
      List<Edge> allEdges) {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(joinNodes, "joinNodes");
    Objects.requireNonNull(builder, "builder");
    Objects.requireNonNull(freeVariables, "freeVariables");
    Objects.requireNonNull(allEdges, "allEdges");

    List<Component> components = partition.components();
    List<ComponentConstructionRules> results = new ArrayList<>(components.size());
    Set<String> normalizedJoinNodes = normalize(joinNodes);
    Set<String> normalizedFreeVars = normalize(freeVariables);
    int totalComponents = components.size();

    for (Component component : components) {
      ComponentRuleCacheKey key =
          new ComponentRuleCacheKey(
              BitsetUtils.signature(component.edgeBits(), allEdges.size()),
              totalComponents,
              normalizedJoinNodes,
              normalizedFreeVars);
      ComponentCPQBuilder.ComponentRuleSet cached = ruleCache.get(key);
      if (cached == null) {
        cacheStats.recordMiss();
        cached =
            builder.componentRules(
                component, normalizedJoinNodes, normalizedFreeVars, totalComponents);
        ruleCache.put(key, cached);
      } else {
        cacheStats.recordHit();
      }
      results.add(
          new ComponentConstructionRules(
              component, cached.rawRules(), cached.joinFilteredRules(), cached.finalRules()));
    }
    return List.copyOf(results);
  }

  private Set<String> normalize(Set<String> values) {
    return values == null || values.isEmpty() ? Set.of() : Set.copyOf(values);
  }

  public ComponentRuleCacheStats cacheStats() {
    return cacheStats;
  }

  private List<List<KnownComponent>> cartesian(List<List<KnownComponent>> lists, int limit) {
    List<List<KnownComponent>> output = new ArrayList<>();
    backtrack(lists, 0, new ArrayList<>(), output, limit);
    return output;
  }

  private void backtrack(
      List<List<KnownComponent>> lists,
      int index,
      List<KnownComponent> current,
      List<List<KnownComponent>> output,
      int limit) {
    if (limit > 0 && output.size() >= limit) {
      return;
    }
    if (index == lists.size()) {
      output.add(List.copyOf(current));
      return;
    }
    for (KnownComponent rule : lists.get(index)) {
      current.add(rule);
      backtrack(lists, index + 1, current, output, limit);
      current.remove(current.size() - 1);
      if (limit > 0 && output.size() >= limit) {
        return;
      }
    }
  }

  public static record ComponentConstructionRules(
      Component component,
      List<KnownComponent> rawRules,
      List<KnownComponent> joinFilteredRules,
      List<KnownComponent> finalRules) {

    public ComponentConstructionRules(
        Component component,
        List<KnownComponent> rawRules,
        List<KnownComponent> joinFilteredRules,
        List<KnownComponent> finalRules) {
      this.component = Objects.requireNonNull(component, "component");
      this.rawRules = List.copyOf(Objects.requireNonNull(rawRules, "rawRules"));
      this.joinFilteredRules =
          List.copyOf(Objects.requireNonNull(joinFilteredRules, "joinFilteredRules"));
      this.finalRules = List.copyOf(Objects.requireNonNull(finalRules, "finalRules"));
    }

    public boolean hasRules() {
      return !finalRules.isEmpty();
    }

    public int ruleCount() {
      return finalRules.size();
    }
  }

  private record ComponentRuleCacheKey(
      String componentSignature,
      int totalComponents,
      Set<String> joinNodes,
      Set<String> freeVars) {}

  public static final class ComponentRuleCacheStats {
    private long hits;
    private long misses;

    public ComponentRuleCacheStats() {}

    public void recordHit() {
      hits++;
    }

    public void recordMiss() {
      misses++;
    }

    public long hits() {
      return hits;
    }

    public long misses() {
      return misses;
    }

    public long lookups() {
      return hits + misses;
    }

    public double hitRate() {
      long lookups = lookups();
      if (lookups == 0) {
        return 0.0;
      }
      return hits / (double) lookups;
    }

    public CacheSnapshot snapshot() {
      return new CacheSnapshot(hits, misses);
    }

    public record CacheSnapshot(long hits, long misses) {
      public long lookups() {
        return hits + misses;
      }

      public double hitRate() {
        long lookups = lookups();
        if (lookups == 0) {
          return 0.0;
        }
        return hits / (double) lookups;
      }
    }
  }
}
