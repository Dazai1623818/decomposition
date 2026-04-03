package evaluator.decomposition;

import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Checks whether a finished decomposition is terminal with respect to the
 * series/parallel reduction rules used by the planner.
 * <p>
 * Free variables are treated as terminals. A plan is non-terminal when it
 * still admits a legal parallel or series merge that also satisfies the
 * provided component filter.
 */
final class TerminalLeafFilter {
    private static final Comparator<VarCQ> VAR_ORDER = Comparator.comparing(VarCQ::getName);

    private TerminalLeafFilter() {
    }

    /**
     * Builds a query-local terminality predicate that precomputes all legal
     * series/parallel merges over the enumerated component pool once, then
     * checks completed plans using only lightweight structural lookups.
     */
    static java.util.function.Predicate<Plan> precomputed(
            List<Component> componentPool,
            Set<VarCQ> terminals,
            java.util.function.Predicate<CPQ> componentFilter) {
        Objects.requireNonNull(componentPool, "componentPool");
        Objects.requireNonNull(terminals, "terminals");
        return new Oracle(componentPool, terminals, componentFilter)::isTerminalLeaf;
    }

    static boolean isTerminalLeaf(Plan plan, java.util.function.Predicate<CPQ> componentFilter) {
        Objects.requireNonNull(plan, "plan");
        List<Component> components = plan.components();
        if (components.size() < 2) {
            return true;
        }
        return !hasParallelMerge(components, componentFilter)
                && !hasSeriesMerge(components, plan.freeVars(), componentFilter);
    }

    private static boolean hasParallelMerge(
            List<Component> components,
            java.util.function.Predicate<CPQ> componentFilter) {
        Map<EndpointPair, List<Component>> groups = new HashMap<>();
        for (Component component : components) {
            EndpointPair endpoints = EndpointPair.ordered(component.s(), component.t());
            groups.computeIfAbsent(endpoints, ignored -> new ArrayList<>()).add(component);
        }

        for (Map.Entry<EndpointPair, List<Component>> entry : groups.entrySet()) {
            List<Component> group = entry.getValue();
            if (group.size() < 2) {
                continue;
            }
            VarCQ s = entry.getKey().s();
            VarCQ t = entry.getKey().t();
            for (int i = 0; i < group.size(); i++) {
                Component left = group.get(i);
                for (int j = i + 1; j < group.size(); j++) {
                    Component right = group.get(j);
                    CPQ merged = ensureUnary(CPQ.intersect(
                            oriented(left, s, t),
                            oriented(right, s, t)), s, t);
                    if (componentFilter == null || componentFilter.test(merged)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean hasSeriesMerge(
            List<Component> components,
            Set<VarCQ> terminals,
            java.util.function.Predicate<CPQ> componentFilter) {
        Map<VarCQ, List<Component>> incident = new HashMap<>();
        for (Component component : components) {
            incident.computeIfAbsent(component.s(), ignored -> new ArrayList<>()).add(component);
            if (!component.s().equals(component.t())) {
                incident.computeIfAbsent(component.t(), ignored -> new ArrayList<>()).add(component);
            }
        }

        List<VarCQ> vertices = new ArrayList<>(incident.keySet());
        vertices.sort(VAR_ORDER);
        for (VarCQ vertex : vertices) {
            if (terminals.contains(vertex)) {
                continue;
            }
            List<Component> neighbors = incident.get(vertex);
            if (neighbors.size() != 2) {
                continue;
            }

            Component left = neighbors.get(0);
            Component right = neighbors.get(1);
            if (left == right || isLoop(left) || isLoop(right)) {
                continue;
            }

            VarCQ a = other(left, vertex);
            VarCQ b = other(right, vertex);
            if (a == null || b == null) {
                throw new IllegalStateException("Series-reduction component not incident to shared variable");
            }
            if (a.equals(b)) {
                continue;
            }

            CPQ merged = ensureUnary(CPQ.concat(
                    oriented(left, a, vertex),
                    oriented(right, vertex, b)), a, b);
            if (componentFilter == null || componentFilter.test(merged)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLoop(Component component) {
        return component.s().equals(component.t());
    }

    private static VarCQ other(Component component, VarCQ vertex) {
        if (component.s().equals(vertex)) {
            return component.t();
        }
        if (component.t().equals(vertex)) {
            return component.s();
        }
        return null;
    }

    private static CPQ oriented(Component component, VarCQ from, VarCQ to) {
        if (component.s().equals(from) && component.t().equals(to)) {
            return component.cpq();
        }
        if (component.s().equals(to) && component.t().equals(from)) {
            return invert(component.cpq());
        }
        throw new IllegalStateException("Component endpoints do not match requested orientation");
    }

    private static CPQ ensureUnary(CPQ cpq, VarCQ s, VarCQ t) {
        if (s.equals(t)) {
            return CPQ.intersect(cpq, CPQ.id());
        }
        return cpq;
    }

    private static CPQ invert(CPQ cpq) {
        return invertTree(cpq.toAbstractSyntaxTree());
    }

    private static CPQ invertTree(QueryTree node) {
        return switch (node.getOperation()) {
            case IDENTITY -> CPQ.id();
            case EDGE -> CPQ.label(node.getEdgeAtom().getLabel().getInverse());
            case CONCATENATION -> {
                int arity = node.getArity();
                CPQ out = invertTree(node.getOperand(arity - 1));
                for (int i = arity - 2; i >= 0; i--) {
                    out = CPQ.concat(out, invertTree(node.getOperand(i)));
                }
                yield out;
            }
            case INTERSECTION -> {
                int arity = node.getArity();
                CPQ out = invertTree(node.getOperand(0));
                for (int i = 1; i < arity; i++) {
                    out = CPQ.intersect(out, invertTree(node.getOperand(i)));
                }
                yield out;
            }
            default -> throw new IllegalArgumentException("Unsupported CPQ operation: " + node.getOperation());
        };
    }

    private static final class Oracle {
        private final IdentityHashMap<Component, Integer> poolIndex = new IdentityHashMap<>();
        private final Set<VarCQ> terminals;
        private final java.util.function.Predicate<CPQ> componentFilter;
        private final Set<Long> parallelPairs = new HashSet<>();
        private final Map<VarCQ, Set<Long>> seriesPairsByVertex = new HashMap<>();

        private Oracle(
                List<Component> componentPool,
                Set<VarCQ> terminals,
                java.util.function.Predicate<CPQ> componentFilter) {
            this.terminals = Set.copyOf(terminals);
            this.componentFilter = componentFilter;
            for (int i = 0; i < componentPool.size(); i++) {
                poolIndex.put(componentPool.get(i), i);
            }
            precomputeParallelPairs(componentPool);
            precomputeSeriesPairs(componentPool);
        }

        private boolean isTerminalLeaf(Plan plan) {
            Objects.requireNonNull(plan, "plan");
            List<Component> components = plan.components();
            if (components.size() < 2) {
                return true;
            }
            if (hasPrecomputedParallelMerge(components)) {
                return false;
            }
            return !hasPrecomputedSeriesMerge(components);
        }

        private boolean hasPrecomputedParallelMerge(List<Component> components) {
            for (int i = 0; i < components.size(); i++) {
                Integer leftIndex = poolIndex.get(components.get(i));
                if (leftIndex == null) {
                    return hasParallelMerge(components, componentFilter);
                }
                for (int j = i + 1; j < components.size(); j++) {
                    Integer rightIndex = poolIndex.get(components.get(j));
                    if (rightIndex == null) {
                        return hasParallelMerge(components, componentFilter);
                    }
                    if (parallelPairs.contains(unorderedPairKey(leftIndex, rightIndex))) {
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean hasPrecomputedSeriesMerge(List<Component> components) {
            Map<VarCQ, List<Component>> incident = new HashMap<>();
            for (Component component : components) {
                incident.computeIfAbsent(component.s(), ignored -> new ArrayList<>()).add(component);
                if (!component.s().equals(component.t())) {
                    incident.computeIfAbsent(component.t(), ignored -> new ArrayList<>()).add(component);
                }
            }

            List<VarCQ> vertices = new ArrayList<>(incident.keySet());
            vertices.sort(VAR_ORDER);
            for (VarCQ vertex : vertices) {
                if (terminals.contains(vertex)) {
                    continue;
                }
                List<Component> neighbors = incident.get(vertex);
                if (neighbors.size() != 2) {
                    continue;
                }

                Component left = neighbors.get(0);
                Component right = neighbors.get(1);
                if (left == right || isLoop(left) || isLoop(right)) {
                    continue;
                }

                Integer leftIndex = poolIndex.get(left);
                Integer rightIndex = poolIndex.get(right);
                if (leftIndex == null || rightIndex == null) {
                    return hasSeriesMerge(components, terminals, componentFilter);
                }

                Set<Long> mergeablePairs = seriesPairsByVertex.get(vertex);
                if (mergeablePairs != null && mergeablePairs.contains(orderedPairKey(leftIndex, rightIndex))) {
                    return true;
                }
            }
            return false;
        }

        private void precomputeParallelPairs(List<Component> componentPool) {
            Map<EndpointPair, List<IndexedComponent>> groups = new HashMap<>();
            for (int i = 0; i < componentPool.size(); i++) {
                Component component = componentPool.get(i);
                EndpointPair endpoints = EndpointPair.ordered(component.s(), component.t());
                groups.computeIfAbsent(endpoints, ignored -> new ArrayList<>())
                        .add(new IndexedComponent(i, component));
            }

            for (Map.Entry<EndpointPair, List<IndexedComponent>> entry : groups.entrySet()) {
                List<IndexedComponent> group = entry.getValue();
                if (group.size() < 2) {
                    continue;
                }
                VarCQ s = entry.getKey().s();
                VarCQ t = entry.getKey().t();
                for (int i = 0; i < group.size(); i++) {
                    IndexedComponent left = group.get(i);
                    for (int j = i + 1; j < group.size(); j++) {
                        IndexedComponent right = group.get(j);
                        CPQ merged = ensureUnary(CPQ.intersect(
                                oriented(left.component(), s, t),
                                oriented(right.component(), s, t)), s, t);
                        if (componentFilter == null || componentFilter.test(merged)) {
                            parallelPairs.add(unorderedPairKey(left.index(), right.index()));
                        }
                    }
                }
            }
        }

        private void precomputeSeriesPairs(List<Component> componentPool) {
            Map<VarCQ, List<IndexedComponent>> incident = new HashMap<>();
            for (int i = 0; i < componentPool.size(); i++) {
                Component component = componentPool.get(i);
                IndexedComponent indexed = new IndexedComponent(i, component);
                incident.computeIfAbsent(component.s(), ignored -> new ArrayList<>()).add(indexed);
                if (!component.s().equals(component.t())) {
                    incident.computeIfAbsent(component.t(), ignored -> new ArrayList<>()).add(indexed);
                }
            }

            List<VarCQ> vertices = new ArrayList<>(incident.keySet());
            vertices.sort(VAR_ORDER);
            for (VarCQ vertex : vertices) {
                if (terminals.contains(vertex)) {
                    continue;
                }
                List<IndexedComponent> neighbors = incident.get(vertex);
                if (neighbors.size() < 2) {
                    continue;
                }
                Set<Long> mergeablePairs = null;
                for (int i = 0; i < neighbors.size(); i++) {
                    IndexedComponent left = neighbors.get(i);
                    for (int j = i + 1; j < neighbors.size(); j++) {
                        IndexedComponent right = neighbors.get(j);
                        if (left.component() == right.component()
                                || isLoop(left.component())
                                || isLoop(right.component())) {
                            continue;
                        }

                        VarCQ a = other(left.component(), vertex);
                        VarCQ b = other(right.component(), vertex);
                        if (a == null || b == null) {
                            throw new IllegalStateException(
                                    "Series-reduction component not incident to shared variable");
                        }
                        if (a.equals(b)) {
                            continue;
                        }

                        CPQ merged = ensureUnary(CPQ.concat(
                                oriented(left.component(), a, vertex),
                                oriented(right.component(), vertex, b)), a, b);
                        if (componentFilter == null || componentFilter.test(merged)) {
                            if (mergeablePairs == null) {
                                mergeablePairs = new HashSet<>();
                                seriesPairsByVertex.put(vertex, mergeablePairs);
                            }
                            mergeablePairs.add(orderedPairKey(left.index(), right.index()));
                        }
                    }
                }
            }
        }

        private static long orderedPairKey(int left, int right) {
            return ((long) left << 32) | (right & 0xffffffffL);
        }

        private static long unorderedPairKey(int left, int right) {
            return left <= right ? orderedPairKey(left, right) : orderedPairKey(right, left);
        }
    }

    private record IndexedComponent(int index, Component component) {
    }

    private record EndpointPair(VarCQ s, VarCQ t) {
        private static EndpointPair ordered(VarCQ a, VarCQ b) {
            return a.getName().compareTo(b.getName()) <= 0
                    ? new EndpointPair(a, b)
                    : new EndpointPair(b, a);
        }
    }
}
