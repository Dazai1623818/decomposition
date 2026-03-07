package evaluator.decomposition;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.decomposition.Decomposer.DecompositionTimeoutException;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.ToLongFunction;

/**
 * Enumerates CPQ components by expanding atomic edges through concatenation and intersection
 * up to a maximum core diameter.
 */
final class ExhaustiveComponentEnumerator {
    private final int maxCoreDiam;
    private final ToLongFunction<CPQ> costFn;
    private final long deadlineNanos;

    ExhaustiveComponentEnumerator(int maxCoreDiam) {
        this(maxCoreDiam, null, Long.MAX_VALUE);
    }

    ExhaustiveComponentEnumerator(int maxCoreDiam, long deadlineNanos) {
        this(maxCoreDiam, null, deadlineNanos);
    }

    ExhaustiveComponentEnumerator(int maxCoreDiam, ToLongFunction<CPQ> costFn) {
        this(maxCoreDiam, costFn, Long.MAX_VALUE);
    }

    ExhaustiveComponentEnumerator(int maxCoreDiam, ToLongFunction<CPQ> costFn, long deadlineNanos) {
        if (maxCoreDiam < 0) {
            throw new IllegalArgumentException("k must be >= 0");
        }
        this.maxCoreDiam = maxCoreDiam;
        this.costFn = costFn;
        this.deadlineNanos = deadlineNanos;
    }

    public List<Component> enumerate(ConjunctiveQuery query) {
        Objects.requireNonNull(query, "query");
        return new Enumerator(query, maxCoreDiam, costFn, deadlineNanos).enumerate();
    }

    /**
     * State holder for the enumeration worklist and best-known components.
     */
    private static final class Enumerator {
        private final int maxCoreDiam;
        private final List<AtomCQ> edges;
        private final int atomCount;
        private int nextId;
        private final Map<ComponentKey, Component> bestByKey = new HashMap<>();
        private final ToLongFunction<CPQ> costFn;

        private final Map<VarCQ, List<Component>> bySource = new HashMap<>();
        private final Map<VarCQ, List<Component>> byTarget = new HashMap<>();
        private final Map<EndpointPair, List<Component>> byEndpoints = new HashMap<>();
        private final long deadlineNanos;

        private Enumerator(ConjunctiveQuery query, int maxCoreDiam, ToLongFunction<CPQ> costFn, long deadlineNanos) {
            this.maxCoreDiam = maxCoreDiam;
            this.edges = query.edges();
            this.atomCount = edges.size();
            this.nextId = 0;
            this.costFn = costFn;
            this.deadlineNanos = deadlineNanos;
        }

        private List<Component> enumerate() {
            checkDeadline();
            Queue<WorkItem> worklist = costFn == null
                    ? new ArrayDeque<>()
                    : new PriorityQueue<>(workItemComparator());

            // Phase 1: seed worklist with atomic edge components.
            for (int i = 0; i < edges.size(); i++) {
                AtomCQ edge = edges.get(i);
                BitSet owned = new BitSet(atomCount);
                owned.set(i);
                BitSet inverseEmpty = new BitSet(atomCount);
                long canonicalCost = canonicalEdgeCost(edge);

                addAtomicComponent(
                        worklist,
                        edge.getSource(),
                        edge.getTarget(),
                        edge.getLabel(),
                        owned,
                        inverseEmpty,
                        canonicalCost);
                if (!edge.getSource().equals(edge.getTarget())) {
                    BitSet inverseSingle = new BitSet(atomCount);
                    inverseSingle.set(i);
                    addAtomicComponent(
                            worklist,
                            edge.getTarget(),
                            edge.getSource(),
                            edge.getLabel().getInverse(),
                            owned,
                            inverseSingle,
                            canonicalCost);
                }
            }

            // Phase 2: expand components by concatenation and intersection.
            while (!worklist.isEmpty()) {
                checkDeadline();
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException("Enumeration interrupted");
                }

                WorkItem item = worklist.poll();
                if (item == null) {
                    continue;
                }
                Component left = item.component;
                if (!isCurrent(left)) {
                    continue;
                }

                // Concatenation (left concat right, parent concat left).
                List<Component> rightCandidates = bySource.getOrDefault(left.t(), List.of());
                List<Component> parentCandidates = byTarget.getOrDefault(left.s(), List.of());
                int leftId = left.id();
                for (int pass = 0; pass < 2; pass++) {
                    checkDeadline();
                    boolean parentFirst = pass == 1;
                    List<Component> candidates = parentFirst ? parentCandidates : rightCandidates;
                    for (int i = 0, size = candidates.size(); i < size; i++) {
                        checkDeadline();
                        Component candidate = candidates.get(i);
                        if (!isCurrent(candidate)) {
                            continue;
                        }
                        if (parentFirst ? candidate.id() > leftId : candidate.id() >= leftId) {
                            continue;
                        }

                        Component first = parentFirst ? candidate : left;
                        Component second = parentFirst ? left : candidate;
                        if (first.maskUnsafe().intersects(second.maskUnsafe())) {
                            continue;
                        }
                        int newCore = first.diameter() + second.diameter();
                        if (newCore > maxCoreDiam) {
                            continue;
                        }
                        BitSet owned = union(first.maskUnsafe(), second.maskUnsafe());
                        BitSet inverse = union(first.inverseAtoms(), second.inverseAtoms());
                        CPQ cpq = CPQ.concat(List.of(first.cpq(), second.cpq()));
                        CPQ effective = first.s().equals(second.t()) ? CPQ.intersect(cpq, CPQ.id()) : cpq;
                        CPQ normalized = normalizeTree(Objects.requireNonNull(effective, "cpq").toAbstractSyntaxTree());
                        Component out = newComponent(
                                first.s(),
                                second.t(),
                                owned,
                                inverse,
                                newCore,
                                normalized,
                                normalized.toString(),
                                cpqSize(normalized));
                        registerIfBetter(out, costOf(normalized), worklist);
                    }
                }

                // Intersection (left intersect other).
                EndpointPair endpoints = new EndpointPair(left.s(), left.t());
                List<Component> parallelCandidates = byEndpoints.getOrDefault(endpoints, List.of());
                for (int i = 0, size = parallelCandidates.size(); i < size; i++) {
                    checkDeadline();
                    Component other = parallelCandidates.get(i);
                    if (other.id() >= left.id() || !isCurrent(other)) {
                        continue;
                    }
                    if (left.maskUnsafe().intersects(other.maskUnsafe())) {
                        continue;
                    }
                    int newCore = Math.max(left.diameter(), other.diameter());
                    if (newCore > maxCoreDiam) {
                        continue;
                    }
                    BitSet owned = union(left.maskUnsafe(), other.maskUnsafe());
                    BitSet inverse = union(left.inverseAtoms(), other.inverseAtoms());
                    CPQ cpq = CPQ.intersect(List.of(left.cpq(), other.cpq()));
                    CPQ effective = left.s().equals(left.t()) ? CPQ.intersect(cpq, CPQ.id()) : cpq;
                    CPQ normalized = normalizeTree(Objects.requireNonNull(effective, "cpq").toAbstractSyntaxTree());
                    Component out = newComponent(
                            left.s(),
                            left.t(),
                            owned,
                            inverse,
                            newCore,
                            normalized,
                            normalized.toString(),
                            cpqSize(normalized));
                    registerIfBetter(out, costOf(normalized), worklist);
                }
            }

            // Phase 3: finalize and sort results.
            List<Component> result = new ArrayList<>(bestByKey.values());
            result.sort(Comparator
                    .comparingInt((Component component) -> component.maskUnsafe().cardinality()).reversed()
                    .thenComparing(Comparator.comparingInt(Component::diameter).reversed())
                    .thenComparing(Component::normalized));

            if (Boolean.getBoolean("cpq.enumeration.dump")) {
                for (Component component : result) {
                    System.out.println(formatComponent(component));
                }
            }

            return result;
        }

        private void checkDeadline() {
            if (deadlineNanos != Long.MAX_VALUE && System.nanoTime() >= deadlineNanos) {
                throw new DecompositionTimeoutException("Component enumeration timed out");
            }
        }

        private static int cpqSize(CPQ cpq) {
            return countNodes(cpq.toAbstractSyntaxTree());
        }

        private static int countNodes(QueryTree node) {
            int total = 1;
            for (int i = 0; i < node.getArity(); i++) {
                total += countNodes(node.getOperand(i));
            }
            return total;
        }

        private static String formatComponent(Component component) {
            return "component s=" + component.s().getName()
                    + " t=" + component.t().getName()
                    + " diam=" + component.diameter()
                    + " mask=" + component.maskUnsafe()
                    + " normalized=" + component.normalized();
        }

        private static CPQ normalizeTree(QueryTree node) {
            return switch (node.getOperation()) {
                case IDENTITY -> CPQ.id();
                case EDGE -> {
                    Predicate label = node.getEdgeAtom().getLabel();
                    yield CPQ.label(label);
                }
                case CONCATENATION -> {
                    List<CPQ> parts = collectParts(node, OperationType.CONCATENATION);
                    parts.removeIf(part -> part.getOperationType() == OperationType.IDENTITY);
                    if (parts.isEmpty()) {
                        yield CPQ.id();
                    }
                    if (parts.size() == 1) {
                        yield parts.get(0);
                    }

                    CPQ normalized = parts.get(0);
                    for (int i = 1; i < parts.size(); i++) {
                        normalized = CPQ.concat(normalized, parts.get(i));
                    }
                    yield normalized;
                }
                case INTERSECTION -> {
                    List<CPQ> parts = collectParts(node, OperationType.INTERSECTION);
                    Map<String, CPQ> unique = new HashMap<>();
                    for (CPQ part : parts) {
                        unique.putIfAbsent(part.toString(), part);
                    }

                    List<Map.Entry<String, CPQ>> ordered = new ArrayList<>(unique.entrySet());
                    ordered.sort(Comparator.comparing(Map.Entry::getKey));
                    if (ordered.size() == 1) {
                        yield ordered.get(0).getValue();
                    }

                    CPQ normalized = ordered.get(0).getValue();
                    for (int i = 1; i < ordered.size(); i++) {
                        normalized = CPQ.intersect(normalized, ordered.get(i).getValue());
                    }
                    yield normalized;
                }
                default -> throw new IllegalArgumentException("Unsupported CPQ operation: " + node.getOperation());
            };
        }

        /**
         * Flattens associative operations into left-to-right, normalized operands.
         */
        private static List<CPQ> collectParts(QueryTree node, OperationType op) {
            List<CPQ> parts = new ArrayList<>();
            ArrayDeque<QueryTree> stack = new ArrayDeque<>();
            stack.push(node);
            while (!stack.isEmpty()) {
                QueryTree current = stack.pop();
                if (current.getOperation() == op) {
                    stack.push(current.getOperand(1));
                    stack.push(current.getOperand(0));
                } else {
                    parts.add(normalizeTree(current));
                }
            }
            return parts;
        }

        /**
         * Builds and registers a component consisting of a single labeled edge.
         */
        private void addAtomicComponent(
                Queue<WorkItem> worklist,
                VarCQ s,
                VarCQ t,
                Predicate label,
                BitSet ownedAtoms,
                BitSet inverseAtoms,
                long cost) {
            CPQ cpq = CPQ.label(label);
            if (s.equals(t)) {
                cpq = CPQ.intersect(cpq, CPQ.id());
            }
            String normalized = cpq.toString();
            Component component = newComponent(
                    s,
                    t,
                    (BitSet) ownedAtoms.clone(),
                    (BitSet) inverseAtoms.clone(),
                    1,
                    cpq,
                    normalized,
                    cpqSize(cpq));
            registerIfBetter(component, cost, worklist);
        }

        private void registerIfBetter(Component c, long cost, Queue<WorkItem> worklist) {
            ComponentKey key = key(c);
            Component existing = bestByKey.get(key);
            if (existing != null) {
                if (c.diameter() > existing.diameter()) {
                    return;
                }
                if (c.diameter() == existing.diameter() && c.size() >= existing.size()) {
                    return;
                }
            }

            bestByKey.put(key, c);
            worklist.add(new WorkItem(c, cost));

            bySource.computeIfAbsent(c.s(), v -> new ArrayList<>()).add(c);
            byTarget.computeIfAbsent(c.t(), v -> new ArrayList<>()).add(c);
            byEndpoints.computeIfAbsent(new EndpointPair(c.s(), c.t()), v -> new ArrayList<>()).add(c);
        }

        private Component newComponent(
                VarCQ s,
                VarCQ t,
                BitSet ownedAtoms,
                BitSet inverseAtoms,
                int coreDiam,
                CPQ cpq,
                String normalized,
                int size) {
            return new Component(s, t, coreDiam, ownedAtoms, inverseAtoms, cpq, normalized, nextId++, size);
        }

        /**
         * Computes a direction-agnostic cost for a single-edge component.
         * Uses the forward label from the original CQ edge, regardless of component direction.
         */
        private long canonicalEdgeCost(AtomCQ edge) {
            if (costFn == null) {
                return 0L;
            }
            CPQ cpq = CPQ.label(edge.getLabel());
            if (edge.getSource().equals(edge.getTarget())) {
                cpq = CPQ.intersect(cpq, CPQ.id());
            }
            return costFn.applyAsLong(cpq);
        }

        private long costOf(CPQ cpq) {
            return costFn == null ? 0L : costFn.applyAsLong(cpq);
        }

        private boolean isCurrent(Component component) {
            return bestByKey.get(key(component)) == component;
        }

        private static BitSet union(BitSet left, BitSet right) {
            BitSet out = (BitSet) left.clone();
            out.or(right);
            return out;
        }

        private static ComponentKey key(Component component) {
            return new ComponentKey(
                    component.s(),
                    component.t(),
                    component.maskUnsafe(),
                    component.inverseAtoms(),
                    component.normalized());
        }

        private record ComponentKey(VarCQ s, VarCQ t, BitSet ownedAtoms, BitSet inverseAtoms, String normalized) {
            private ComponentKey {
                ownedAtoms = (BitSet) ownedAtoms.clone();
                inverseAtoms = (BitSet) inverseAtoms.clone();
            }
        }

        private record EndpointPair(VarCQ s, VarCQ t) {
        }

        private static Comparator<WorkItem> workItemComparator() {
            return Comparator
                    .comparingLong((WorkItem item) -> item.cost)
                    .thenComparing(Comparator
                            .comparingInt((WorkItem item) -> item.component.maskUnsafe().cardinality())
                            .reversed())
                    .thenComparing(Comparator
                            .comparingInt((WorkItem item) -> item.component.diameter())
                            .reversed())
                    .thenComparingInt(item -> item.component.id());
        }

        private static final class WorkItem {
            private final Component component;
            private final long cost;

            private WorkItem(Component component, long cost) {
                this.component = component;
                this.cost = cost;
            }
        }
    }
}
