package decomposition.partitions;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.JoinNodeUtils;
import java.util.ArrayList;
import java.util.List;
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
    List<Component> components = partition.components();
    boolean singleComponent = components.size() == 1;
    for (Component component : components) {
      List<KnownComponent> options = builder.options(component.edgeBits(), joinNodes);
      Set<String> localJoinNodes = JoinNodeUtils.localJoinNodes(component, joinNodes);
      if (shouldEnforceJoinNodes(joinNodes, components.size(), component)) {
        options =
            options.stream()
                .filter(
                    kc ->
                        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes))
                .collect(Collectors.toList());
      }
      if (singleComponent) {
        options = JoinNodeUtils.filterByFreeVariableOrdering(options, component, freeVariableOrder);
      }
      if (options.isEmpty()) {
        return false;
      }
    }
    return true;
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
    List<Component> components = partition.components();
    List<Set<String>> componentVariables = new ArrayList<>();
    for (Component component : components) {
      componentVariables.add(component.vertices());
    }

    List<List<KnownComponent>> perComponentOptions = new ArrayList<>();
    boolean singleComponent = components.size() == 1;
    for (Component component : components) {
      List<KnownComponent> options = builder.options(component.edgeBits(), joinNodes);
      Set<String> localJoinNodes = JoinNodeUtils.localJoinNodes(component, joinNodes);
      if (shouldEnforceJoinNodes(joinNodes, components.size(), component)) {
        options =
            options.stream()
                .filter(
                    kc ->
                        JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes))
                .collect(Collectors.toList());
      }
      if (singleComponent) {
        options = JoinNodeUtils.filterByFreeVariableOrdering(options, component, freeVariableOrder);
      }
      if (options.isEmpty()) {
        return List.of();
      }
      perComponentOptions.add(options);
    }

    return cartesian(perComponentOptions, componentVariables, freeVariables, allEdges, limit);
  }

  // Legacy method for backward compatibility
  public List<List<KnownComponent>> enumerateDecompositions(
      Partition partition, ComponentCPQBuilder builder, int limit) {
    Set<String> joinNodes = JoinNodeUtils.computeJoinNodes(partition.components(), Set.of());
    return enumerateDecompositions(
        partition, joinNodes, builder, limit, Set.of(), List.of(), builder.allEdges());
  }

  private List<List<KnownComponent>> cartesian(
      List<List<KnownComponent>> lists,
      List<Set<String>> componentVariables,
      Set<String> freeVariables,
      List<Edge> allEdges,
      int limit) {
    List<List<KnownComponent>> output = new ArrayList<>();
    backtrack(
        lists, componentVariables, freeVariables, allEdges, 0, new ArrayList<>(), output, limit);
    return output;
  }

  private void backtrack(
      List<List<KnownComponent>> lists,
      List<Set<String>> componentVariables,
      Set<String> freeVariables,
      List<Edge> allEdges,
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
      backtrack(
          lists, componentVariables, freeVariables, allEdges, index + 1, current, output, limit);
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
}
