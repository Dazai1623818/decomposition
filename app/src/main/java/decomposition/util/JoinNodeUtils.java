package decomposition.util;

import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

    /**
     * Captures whether a join node can serve as the source and/or target endpoint
     * based on the incident edges inside the component.
     */
    public static final class JoinNodeRole {
        private boolean allowSource;
        private boolean allowTarget;

        public boolean allowSource() {
            return allowSource;
        }

        public boolean allowTarget() {
            return allowTarget;
        }

        private void markSource() {
            this.allowSource = true;
        }

        private void markTarget() {
            this.allowTarget = true;
        }
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
     * Computes the directional roles that each join node may play inside a component.
     * A join node is allowed as a source/target if there exists an incident edge that
     * originates/terminates at that node within the component.
     *
     * @param component component under consideration
     * @param joinNodes global join node set
     * @param edges     full edge list for the CQ
     * @return map from join node name to its allowed roles (may be empty)
     */
    public static Map<String, JoinNodeRole> computeJoinNodeRoles(Component component,
                                                                 Set<String> joinNodes,
                                                                 List<Edge> edges) {
        Objects.requireNonNull(component, "component");
        Objects.requireNonNull(joinNodes, "joinNodes");
        Objects.requireNonNull(edges, "edges");

        if (joinNodes.isEmpty()) {
            return Map.of();
        }

        Map<String, JoinNodeRole> roles = new HashMap<>();
        for (String vertex : component.vertices()) {
            if (joinNodes.contains(vertex)) {
                roles.put(vertex, new JoinNodeRole());
            }
        }

        if (roles.isEmpty()) {
            return Map.of();
        }

        BitSet bits = component.edgeBits();
        for (int idx = bits.nextSetBit(0); idx >= 0; idx = bits.nextSetBit(idx + 1)) {
            Edge edge = edges.get(idx);
            JoinNodeRole srcRole = roles.get(edge.source());
            if (srcRole != null) {
                srcRole.markSource();
                // The presence of the edge also allows reaching the join node via its inverse.
                srcRole.markTarget();
            }
            JoinNodeRole tgtRole = roles.get(edge.target());
            if (tgtRole != null) {
                tgtRole.markTarget();
                // Likewise, the inverse enables using the node as a source.
                tgtRole.markSource();
            }
        }

        return Collections.unmodifiableMap(roles);
    }

    /**
     * Checks whether the endpoints of the provided CPQ option respect the directional
     * roles inferred for the local join nodes of its component.
     *
     * @param option         CPQ option to validate
     * @param localJoinNodes join nodes present inside the component
     * @param joinNodeRoles  directional roles for relevant join nodes
     * @return {@code true} if endpoints comply with join node expectations
     */
    public static boolean endpointsRespectJoinNodeRoles(KnownComponent option,
                                                        decomposition.model.Component component,
                                                        Set<String> localJoinNodes,
                                                        Map<String, JoinNodeRole> joinNodeRoles) {
        Objects.requireNonNull(option, "option");
        Objects.requireNonNull(component, "component");
        Objects.requireNonNull(localJoinNodes, "localJoinNodes");
        Objects.requireNonNull(joinNodeRoles, "joinNodeRoles");

        if (localJoinNodes.isEmpty()) {
            return true;
        }

        String source = option.source();
        String target = option.target();

        if (localJoinNodes.size() == 1) {
            String join = localJoinNodes.iterator().next();
            JoinNodeRole role = joinNodeRoles.get(join);
            boolean allowSource = allowsSource(role);
            boolean allowTarget = allowsTarget(role);
            if (component.edgeCount() == 1) {
                boolean matchesSource = source.equals(join) && allowSource;
                boolean matchesTarget = target.equals(join) && allowTarget;
                return matchesSource || matchesTarget;
            }
            if (!source.equals(join) || !target.equals(join) || !allowSource) {
                return false;
            }
            try {
                boolean loopEnforced = option.cpq().toQueryGraph().isLoop();
                return allowTarget || loopEnforced;
            } catch (RuntimeException ex) {
                return allowTarget;
            }
        }

        if (localJoinNodes.size() == 2) {
            Set<String> endpoints = new HashSet<>();
            endpoints.add(source);
            endpoints.add(target);
            if (endpoints.size() != localJoinNodes.size() || !endpoints.containsAll(localJoinNodes)) {
                return false;
            }

            for (String join : localJoinNodes) {
                JoinNodeRole role = joinNodeRoles.get(join);
                if (source.equals(join) && !allowsSource(role)) {
                    return false;
                }
                if (target.equals(join) && !allowsTarget(role)) {
                    return false;
                }
            }
            return true;
        }

        // Components with more than two join nodes are filtered earlier; treat as invalid.
        return false;
    }

    private static boolean allowsSource(JoinNodeRole role) {
        return role != null && role.allowSource();
    }

    private static boolean allowsTarget(JoinNodeRole role) {
        return role != null && role.allowTarget();
    }
}
