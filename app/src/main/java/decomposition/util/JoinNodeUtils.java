package decomposition.util;

import decomposition.cpq.KnownComponent;
import decomposition.model.Component;
import decomposition.model.Edge;
import java.util.BitSet;
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
            }
            JoinNodeRole tgtRole = roles.get(edge.target());
            if (tgtRole != null) {
                tgtRole.markTarget();
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
            boolean loopEnforced = false;
            try {
                loopEnforced = option.cpq().toQueryGraph().isLoop();
            } catch (RuntimeException ex) {
                loopEnforced = false;
            }
            return allowTarget || loopEnforced;
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
