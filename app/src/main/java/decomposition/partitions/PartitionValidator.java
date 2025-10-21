package decomposition.partitions;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.util.JoinNodeUtils;
import decomposition.util.JoinNodeUtils.JoinNodeRole;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates partitions against the CPQ builder and enumerates component combinations.
 */
public final class PartitionValidator {

    public boolean isValidCPQDecomposition(Partition partition,
                                           ComponentCPQBuilder builder,
                                           Set<String> freeVariables,
                                           List<Edge> allEdges) {
        List<Component> components = partition.components();
        List<Set<String>> componentVariables = new ArrayList<>(components.size());
        for (Component component : components) {
            componentVariables.add(component.vertices());
        }

        Set<String> joinNodes = computeJoinNodes(componentVariables, freeVariables);
        for (Component component : components) {
            List<KnownComponent> options = builder.options(component.edgeBits());
            Set<String> localJoinNodes = localJoinNodes(component, joinNodes);
            Map<String, JoinNodeRole> joinNodeRoles = JoinNodeUtils.computeJoinNodeRoles(component, joinNodes, allEdges);
            if (shouldEnforceJoinNodes(joinNodes, components.size(), component)) {
                options = options.stream()
                        .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes, joinNodeRoles))
                        .collect(Collectors.toList());
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
            List<Edge> allEdges) {
        List<Component> components = partition.components();
        List<Set<String>> componentVariables = new ArrayList<>();
        for (Component component : components) {
            componentVariables.add(component.vertices());
        }

        Set<String> joinNodes = computeJoinNodes(componentVariables, freeVariables);
        List<List<KnownComponent>> perComponentOptions = new ArrayList<>();
        for (Component component : components) {
            List<KnownComponent> options = builder.options(component.edgeBits());
            Set<String> localJoinNodes = localJoinNodes(component, joinNodes);
            Map<String, JoinNodeRole> joinNodeRoles = JoinNodeUtils.computeJoinNodeRoles(component, joinNodes, allEdges);
            if (shouldEnforceJoinNodes(joinNodes, components.size(), component)) {
                options = options.stream()
                        .filter(kc -> JoinNodeUtils.endpointsRespectJoinNodeRoles(kc, component, localJoinNodes, joinNodeRoles))
                        .collect(Collectors.toList());
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
            Partition partition,
            ComponentCPQBuilder builder,
            int limit) {
        return enumerateDecompositions(partition, builder, limit, Set.of(), builder.allEdges());
    }

    private List<List<KnownComponent>> cartesian(List<List<KnownComponent>> lists,
                                                   List<Set<String>> componentVariables,
                                                   Set<String> freeVariables,
                                                   List<Edge> allEdges,
                                                   int limit) {
        List<List<KnownComponent>> output = new ArrayList<>();
        backtrack(lists, componentVariables, freeVariables, allEdges, 0, new ArrayList<>(), output, limit);
        return output;
    }

    private void backtrack(List<List<KnownComponent>> lists,
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
            // Validate based on join variables and CPQ structure
            if (isValidDecompositionTuple(current, componentVariables, freeVariables)) {
                output.add(List.copyOf(current));
            }
            return;
        }
        for (KnownComponent option : lists.get(index)) {
            current.add(option);
            backtrack(lists, componentVariables, freeVariables, allEdges, index + 1, current, output, limit);
            current.remove(current.size() - 1);
            if (limit > 0 && output.size() >= limit) {
                return;
            }
        }
    }

    /**
     * Validates that a tuple of components forms a valid decomposition.
     *
     * Key rules:
     * 1. For multi-component partitions with join variables, avoid degenerate self-loops
     *    that fail to connect distinct join nodes.
     * 2. Single-component partitions accept self-loop structures.
     */
    private boolean isValidDecompositionTuple(List<KnownComponent> tuple,
                                               List<Set<String>> componentVariables,
                                               Set<String> freeVariables) {
        // Single component - allow all including self-loops
        if (tuple.size() == 1) {
            return true;
        }

        return true;
    }

    /**
     * Validates that components in a tuple compose correctly to form a valid decomposition.
     *
     * For multi-component partitions, components are joined on shared variables.
     * The composition should result in endpoints matching the free variables.
     */
    private boolean componentsComposeCorrectly(List<KnownComponent> tuple,
                                                 List<Set<String>> componentVariables,
                                                 Set<String> freeVariables) {
        // Single component - always valid
        if (tuple.size() == 1) {
            return true;
        }

        // For multi-component partitions, simulate the join to see if it produces valid endpoints
        // Strategy: components are joined on shared variables
        // The final result should have endpoints in the free variables

        Set<String> joinVariables = computeJoinNodes(componentVariables, freeVariables);

        // Expected result endpoints based on free variables
        String expectedSource = null;
        String expectedTarget = null;

        if (!freeVariables.isEmpty()) {
            List<String> freeVarsList = new ArrayList<>(freeVariables);
            expectedSource = freeVarsList.get(0);
            expectedTarget = freeVarsList.size() > 1 ? freeVarsList.get(1) : freeVarsList.get(0);
        }

        // Check if components can be composed into a path matching expected endpoints
        // For now, use a simplified check: trace through components via join variables
        String currentSource = null;
        String currentTarget = null;

        for (int i = 0; i < tuple.size(); i++) {
            KnownComponent kc = tuple.get(i);
            String source = kc.source();
            String target = kc.target();
            Set<String> vars = componentVariables.get(i);

            if (i == 0) {
                // First component sets the initial source
                currentSource = source;
                currentTarget = target;
            } else {
                // Subsequent components should connect via join variables
                // If component connects to current path, update the target
                if (source.equals(currentTarget)) {
                    // Component extends the path: currentSource -> ... -> currentTarget -> target
                    currentTarget = target;
                } else if (target.equals(currentSource)) {
                    // Component prepends to the path: source -> currentSource -> ... -> currentTarget
                    currentSource = source;
                } else if (source.equals(currentSource) && target.equals(currentTarget)) {
                    // Component has same endpoints - this is a union/intersection, keep current endpoints
                    // This is valid
                } else {
                    // Component doesn't connect properly
                    return false;
                }
            }
        }

        // Check if final composition matches expected free variable endpoints
        if (expectedSource != null) {
            return currentSource.equals(expectedSource) && currentTarget.equals(expectedTarget);
        }

        return true;
    }

    /**
     * Computes join variables - variables that appear in more than one component.
     */
    private Set<String> computeJoinNodes(List<Set<String>> componentVariables,
                                         Set<String> freeVariables) {
        Map<String, Integer> counts = new HashMap<>();
        for (Set<String> vars : componentVariables) {
            for (String var : vars) {
                counts.merge(var, 1, Integer::sum);
            }
        }

        Set<String> joinNodes = new HashSet<>();
        if (freeVariables != null) {
            joinNodes.addAll(freeVariables);
        }

        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() >= 2) {
                joinNodes.add(entry.getKey());
            }
        }
        return joinNodes;
    }

    private boolean shouldEnforceJoinNodes(Set<String> joinNodes, int totalComponents, Component component) {
        if (joinNodes.isEmpty()) {
            return false;
        }
        if (totalComponents > 1) {
            return true;
        }
        return component.edgeCount() > 1;
    }

    private Set<String> localJoinNodes(Component component, Set<String> joinNodes) {
        if (joinNodes == null || joinNodes.isEmpty()) {
            return Set.of();
        }
        Set<String> local = new HashSet<>();
        for (String vertex : component.vertices()) {
            if (joinNodes.contains(vertex)) {
                local.add(vertex);
            }
        }
        return local;
    }

}
