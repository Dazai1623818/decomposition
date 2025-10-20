package decomposition.partitions;

import decomposition.cpq.ComponentCPQBuilder;
import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import decomposition.model.Partition;
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
                                           Set<String> freeVariables) {
        List<Component> components = partition.components();
        List<Set<String>> componentVariables = new ArrayList<>(components.size());
        for (Component component : components) {
            componentVariables.add(component.vertices());
        }

        Set<String> joinNodes = computeJoinNodes(componentVariables, freeVariables);
        for (Component component : components) {
            List<KnownComponent> options = builder.options(component.edgeBits());
            if (shouldEnforceJoinNodes(joinNodes, components.size(), component)) {
                options = options.stream()
                        .filter(kc -> joinNodes.contains(kc.source()) && joinNodes.contains(kc.target()))
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
            if (shouldEnforceJoinNodes(joinNodes, components.size(), component)) {
                options = options.stream()
                        .filter(kc -> joinNodes.contains(kc.source()) && joinNodes.contains(kc.target()))
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
        return enumerateDecompositions(partition, builder, limit, Set.of(), List.of());
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
     * 1. For multi-component partitions with join variables:
     *    - Components should NOT be self-loops (source == target)
     *    - CPQs with ∩ id collapse to self-loops and don't connect properly
     * 2. For single-component partitions:
     *    - Self-loops with ∩ id are valid
     */
    private boolean isValidDecompositionTuple(List<KnownComponent> tuple,
                                               List<Set<String>> componentVariables,
                                               Set<String> freeVariables) {
        // Single component - allow all including self-loops with ∩ id
        if (tuple.size() == 1) {
            return true;
        }

        // Multi-component partition - check for join variables
        Set<String> joinVariables = computeJoinNodes(componentVariables, freeVariables);

        // If there are join variables, components should connect through them
        // Components with ∩ id create self-loops which don't connect properly
        if (!joinVariables.isEmpty()) {
            for (KnownComponent kc : tuple) {
                String source = kc.source();
                String target = kc.target();
                String cpqStr = kc.cpq().toString();

                // Reject self-loops (source == target) for multi-component cases
                // These don't properly connect through join variables
                if (source.equals(target)) {
                    return false;
                }

                // Also reject if CPQ contains ∩ id in multi-component case
                // because ∩ id forces endpoints to be the same (creates self-loop)
                if (cpqStr.contains("∩ id") || cpqStr.contains("∩id")) {
                    return false;
                }
            }
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
}
