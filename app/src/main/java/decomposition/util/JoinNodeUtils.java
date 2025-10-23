package decomposition.util;

import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utilities for reasoning about join nodes within components and CPQ options.
 */
public final class JoinNodeUtils {

    private JoinNodeUtils() {
    }

    /**
     * Computes the set of join nodes given a collection of components and optional free variables.
     * A vertex qualifies as a join node if it either appears in at least two components or is a free variable.
     */
    public static Set<String> computeJoinNodes(Collection<Component> components, Set<String> freeVariables) {
        Objects.requireNonNull(components, "components");
        Map<String, Integer> counts = new HashMap<>();
        for (Component component : components) {
            for (String vertex : component.vertices()) {
                counts.merge(vertex, 1, Integer::sum);
            }
        }
        return computeJoinNodesFromCounts(counts, freeVariables);
    }

    /**
     * Computes the set of join nodes from precomputed component variable sets and optional free variables.
     * A vertex qualifies as a join node if it either appears in at least two components or is a free variable.
     */
    public static Set<String> computeJoinNodesFromVariables(Collection<? extends Set<String>> componentVariables,
                                                            Set<String> freeVariables) {
        Objects.requireNonNull(componentVariables, "componentVariables");
        Map<String, Integer> counts = new HashMap<>();
        for (Set<String> vars : componentVariables) {
            for (String var : vars) {
                counts.merge(var, 1, Integer::sum);
            }
        }
        return computeJoinNodesFromCounts(counts, freeVariables);
    }

    private static Set<String> computeJoinNodesFromCounts(Map<String, Integer> counts, Set<String> freeVariables) {
        Set<String> joinNodes = new HashSet<>();
        if (freeVariables != null) {
            joinNodes.addAll(freeVariables);
        }
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() >= 2) {
                joinNodes.add(entry.getKey());
            }
        }
        return Collections.unmodifiableSet(joinNodes);
    }

    /**
     * Checks whether the endpoints of the provided CPQ option respect the directional
     * roles inferred for the local join nodes of its component.
     *
     * @param option         CPQ option to validate
     * @param localJoinNodes join nodes present inside the component
     * @return {@code true} if endpoints comply with join node expectations
     */
    public static boolean endpointsRespectJoinNodeRoles(KnownComponent option,
                                                        decomposition.model.Component component,
                                                        Set<String> localJoinNodes) {
        Objects.requireNonNull(option, "option");
        Objects.requireNonNull(component, "component");
        Objects.requireNonNull(localJoinNodes, "localJoinNodes");

        if (localJoinNodes.isEmpty()) {
            return true;
        }

        String source = option.source();
        String target = option.target();

        if (localJoinNodes.size() == 1) {
            String join = localJoinNodes.iterator().next();
            if (component.edgeCount() == 1) {
                boolean matchesSource = source.equals(join);
                boolean matchesTarget = target.equals(join);
                return matchesSource || matchesTarget;
            }
            if (!source.equals(join) || !target.equals(join)) {
                return false;
            }
            return true;
        }

        if (localJoinNodes.size() == 2) {
            Set<String> endpoints = new HashSet<>();
            endpoints.add(source);
            endpoints.add(target);
            if (endpoints.size() != localJoinNodes.size() || !endpoints.containsAll(localJoinNodes)) {
                return false;
            }
            return true;
        }

        // Components with more than two join nodes are filtered earlier; treat as invalid.
        return false;
    }
}
