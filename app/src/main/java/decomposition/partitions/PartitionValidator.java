package decomposition.partitions;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.BitsetUtils;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Validates partitions against the CPQ builder and enumerates component combinations. */
public final class PartitionValidator {
  private final Map<ComponentOptionsKey, CachedComponentOptions> optionsCache = new HashMap<>();
  private final ComponentOptionsCacheStats cacheStats;

  public PartitionValidator() {
    this(new ComponentOptionsCacheStats());
  }

  public PartitionValidator(ComponentOptionsCacheStats cacheStats) {
    this.cacheStats = Objects.requireNonNull(cacheStats, "cacheStats");
  }

  public boolean isValidCPQDecomposition(
      Partition partition,
      Set<String> joinNodes,
      ComponentCPQBuilder builder,
      Set<String> freeVariables,
      List<Edge> allEdges) {
    return componentOptions(partition, joinNodes, builder, freeVariables, allEdges).stream()
        .allMatch(ComponentOptions::hasCandidates);
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
    List<ComponentOptions> perComponent =
        componentOptions(partition, joinNodes, builder, freeVariables, allEdges);
    return enumerateDecompositions(perComponent, limit);
  }

  public List<List<KnownComponent>> enumerateDecompositions(
      List<ComponentOptions> componentOptions, int limit) {
    if (componentOptions.stream().anyMatch(options -> options.finalOptions().isEmpty())) {
      return List.of();
    }
    List<List<KnownComponent>> perComponentOptions =
        componentOptions.stream().map(ComponentOptions::finalOptions).collect(Collectors.toList());
    return cartesian(perComponentOptions, limit);
  }

  public List<ComponentOptions> componentOptions(
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
    List<ComponentOptions> results = new ArrayList<>(components.size());
    Set<String> normalizedJoinNodes = normalize(joinNodes);
    Set<String> normalizedFreeVars = normalize(freeVariables);
    int totalComponents = components.size();

    for (Component component : components) {
      ComponentOptionsKey key =
          new ComponentOptionsKey(
              BitsetUtils.signature(component.edgeBits(), allEdges.size()),
              totalComponents,
              normalizedJoinNodes,
              normalizedFreeVars);
      CachedComponentOptions cached = optionsCache.get(key);
      if (cached == null) {
        cacheStats.recordMiss();
        cached =
            buildComponentOptions(
                component, normalizedJoinNodes, builder, normalizedFreeVars, totalComponents);
        optionsCache.put(key, cached);
      } else {
        cacheStats.recordHit();
      }
      results.add(
          new ComponentOptions(
              component, cached.raw(), cached.joinFiltered(), cached.finalOptions()));
    }
    return List.copyOf(results);
  }

  private CachedComponentOptions buildComponentOptions(
      Component component,
      Set<String> joinNodes,
      ComponentCPQBuilder builder,
      Set<String> freeVariables,
      int totalComponents) {
    List<KnownComponent> raw = builder.options(component.edgeBits(), joinNodes);
    List<KnownComponent> joinFiltered = raw;
    if (shouldEnforceJoinNodes(joinNodes, totalComponents, component)) {
      Set<String> localJoinNodes = JoinNodeUtils.localJoinNodes(component, joinNodes);
      joinFiltered =
          raw.stream()
              .filter(
                  kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes))
              .collect(Collectors.toList());
    }
    List<KnownComponent> oriented =
        preferCanonicalOrientation(joinFiltered, joinNodes, freeVariables);
    List<KnownComponent> finalOptions = oriented.isEmpty() ? joinFiltered : oriented;
    return new CachedComponentOptions(raw, joinFiltered, finalOptions);
  }

  private Set<String> normalize(Set<String> values) {
    return values == null || values.isEmpty() ? Set.of() : Set.copyOf(values);
  }

  public ComponentOptionsCacheStats cacheStats() {
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
    for (KnownComponent option : lists.get(index)) {
      current.add(option);
      backtrack(lists, index + 1, current, output, limit);
      current.remove(current.size() - 1);
      if (limit > 0 && output.size() >= limit) {
        return;
      }
    }
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
      List<KnownComponent> candidates, Set<String> joinNodes, Set<String> freeVariables) {
    if (candidates.isEmpty() || joinNodes.size() != 2) {
      return List.of();
    }

    List<String> orderedJoinNodes = new ArrayList<>(joinNodes);
    orderedJoinNodes.sort(
        Comparator.comparing((String node) -> freeVariables.contains(node) ? 0 : 1)
            .thenComparing(node -> node));

    String preferredSource = orderedJoinNodes.get(0);
    String preferredTarget = orderedJoinNodes.get(1);

    return candidates.stream()
        .filter(
            candidate ->
                preferredSource.equals(candidate.source())
                    && preferredTarget.equals(candidate.target()))
        .collect(Collectors.toList());
  }

  public static record ComponentOptions(
      Component component,
      List<KnownComponent> rawOptions,
      List<KnownComponent> joinFilteredOptions,
      List<KnownComponent> finalOptions) {

    public ComponentOptions(
        Component component,
        List<KnownComponent> rawOptions,
        List<KnownComponent> joinFilteredOptions,
        List<KnownComponent> finalOptions) {
      this.component = Objects.requireNonNull(component, "component");
      this.rawOptions = List.copyOf(Objects.requireNonNull(rawOptions, "rawOptions"));
      this.joinFilteredOptions =
          List.copyOf(Objects.requireNonNull(joinFilteredOptions, "joinFilteredOptions"));
      this.finalOptions = List.copyOf(Objects.requireNonNull(finalOptions, "finalOptions"));
    }

    public boolean hasCandidates() {
      return !finalOptions.isEmpty();
    }

    public int candidateCount() {
      return finalOptions.size();
    }
  }

  private record ComponentOptionsKey(
      String componentSignature,
      int totalComponents,
      Set<String> joinNodes,
      Set<String> freeVars) {}

  private record CachedComponentOptions(
      List<KnownComponent> raw,
      List<KnownComponent> joinFiltered,
      List<KnownComponent> finalOptions) {}

  public static final class ComponentOptionsCacheStats {
    private long hits;
    private long misses;

    public ComponentOptionsCacheStats() {}

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
