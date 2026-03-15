package evaluator.decomposition;

import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
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

    private record EndpointPair(VarCQ s, VarCQ t) {
        private static EndpointPair ordered(VarCQ a, VarCQ b) {
            return a.getName().compareTo(b.getName()) <= 0
                    ? new EndpointPair(a, b)
                    : new EndpointPair(b, a);
        }
    }
}
