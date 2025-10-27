package decomposition.partitions;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Validates partitions against the CPQ builder and enumerates component combinations. */
public final class PartitionValidator {

  public boolean isValidCPQDecomposition(
      Partition partition,
      Set<String> joinNodes,
      ComponentCPQBuilder builder,
      Set<String> freeVariables,
      List<String> freeVariableOrder,
      List<Edge> allEdges) {
    return componentOptions(
            partition, joinNodes, builder, freeVariables, freeVariableOrder, allEdges)
        .stream()
        .allMatch(ComponentOptions::hasCandidates);
  }

  public List<List<KnownComponent>> enumerateDecompositions(
      Partition partition,
      ComponentCPQBuilder builder,
      int limit,
      Set<String> freeVariables,
      List<String> freeVariableOrder,
      List<Edge> allEdges) {
    return enumerateDecompositions(
        partition,
        JoinNodeUtils.computeJoinNodes(partition.components(), freeVariables),
        builder,
        limit,
        freeVariables,
        freeVariableOrder,
        allEdges);
  }

  public List<List<KnownComponent>> enumerateDecompositions(
      Partition partition,
      Set<String> joinNodes,
      ComponentCPQBuilder builder,
      int limit,
      Set<String> freeVariables,
      List<String> freeVariableOrder,
      List<Edge> allEdges) {
    List<ComponentOptions> perComponent =
        componentOptions(partition, joinNodes, builder, freeVariables, freeVariableOrder, allEdges);
    return enumerateDecompositions(perComponent, limit);
  }

  // Legacy method for backward compatibility
  public List<List<KnownComponent>> enumerateDecompositions(
      Partition partition, ComponentCPQBuilder builder, int limit) {
    Set<String> joinNodes = JoinNodeUtils.computeJoinNodes(partition.components(), Set.of());
    return enumerateDecompositions(
        partition, joinNodes, builder, limit, Set.of(), List.of(), builder.allEdges());
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
      List<String> freeVariableOrder,
      List<Edge> allEdges) {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(joinNodes, "joinNodes");
    Objects.requireNonNull(builder, "builder");
    Objects.requireNonNull(freeVariables, "freeVariables");
    Objects.requireNonNull(freeVariableOrder, "freeVariableOrder");
    Objects.requireNonNull(allEdges, "allEdges");

    List<Component> components = partition.components();
    boolean singleComponent = components.size() == 1;
    List<ComponentOptions> results = new ArrayList<>(components.size());

    for (Component component : components) {
      List<KnownComponent> raw = builder.options(component.edgeBits(), joinNodes);
      List<KnownComponent> joinFiltered = raw;
      if (shouldEnforceJoinNodes(joinNodes, components.size(), component)) {
        Set<String> localJoinNodes = JoinNodeUtils.localJoinNodes(component, joinNodes);
        joinFiltered =
            raw.stream()
                .filter(
                    kc ->
                        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes))
                .collect(Collectors.toList());
      }
      List<KnownComponent> ordered = joinFiltered;
      if (singleComponent) {
        ordered =
            JoinNodeUtils.filterByFreeVariableOrdering(joinFiltered, component, freeVariableOrder);
      }
      results.add(new ComponentOptions(component, raw, joinFiltered, ordered));
    }
    return List.copyOf(results);
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
}
