package evaluator.decomposition;

import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import evaluator.decomposition.Decomposer.DecompositionTimeoutException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

/**
 * Treewidth-2 aware component cover search with a System-R style DP objective.
 * Uses exact edge-disjoint coverage plus endpoint exposure constraints.
 */
final class Tw2CostDpDecomposer {
    private static final int UNBOUND = -1;
    private static final int MIN_WIDEN_POOL = 32;
    private static final int WIDEN_MULTIPLIER = 2;

    private Tw2CostDpDecomposer() {
    }

    static Stream<Plan> decompose(
            CQ cq,
            int k,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(costFn, "costFn");
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }

        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        if (query.edges().isEmpty()) {
            return Stream.of(new Plan(query, List.of()));
        }

        int[] eliminationOrder = treewidthTwoEliminationOrder(query, deadlineNanos);
        if (eliminationOrder == null) {
            return Stream.of(query.decomposeSingleEdge());
        }

        ExhaustiveComponentEnumerator enumerator = new ExhaustiveComponentEnumerator(k, costFn, deadlineNanos);
        List<Component> components = enumerator.enumerate(query);
        if (componentFilter != null) {
            components = filterComponents(components, componentFilter);
        }
        if (components.isEmpty()) {
            return Stream.of(query.decomposeSingleEdge());
        }

        Plan best = bestPlanWithWidening(query, components, costFn, eliminationOrder, deadlineNanos);
        if (best != null) {
            return Stream.of(best);
        }
        return Stream.of(query.decomposeSingleEdge());
    }

    /**
     * Any-time widening over ranked component options. Starts with a small
     * candidate pool and grows geometrically until a feasible DP solution
     * is found or the full pool is exhausted.
     */
    private static Plan bestPlanWithWidening(
            ConjunctiveQuery query,
            List<Component> components,
            ToLongFunction<CPQ> costFn,
            int[] eliminationOrder,
            long deadlineNanos) {
        checkDeadline(deadlineNanos);
        int total = components.size();
        int poolSize = Math.min(total, Math.max(MIN_WIDEN_POOL, query.edges().size() * 4));
        while (true) {
            checkDeadline(deadlineNanos);
            List<Component> pool = poolSize >= total
                    ? components
                    : components.subList(0, poolSize);
            Plan best = bestPlan(query, pool, costFn, eliminationOrder, deadlineNanos);
            if (best != null) {
                return best;
            }
            if (poolSize >= total) {
                return null;
            }
            long grown = (long) poolSize * WIDEN_MULTIPLIER;
            poolSize = grown >= Integer.MAX_VALUE
                    ? total
                    : Math.min(total, (int) grown);
        }
    }

    private static List<Component> filterComponents(
            List<Component> components,
            java.util.function.Predicate<CPQ> componentFilter) {
        List<Component> filtered = new ArrayList<>(components.size());
        for (Component component : components) {
            if (componentFilter.test(component.cpq())) {
                filtered.add(component);
            }
        }
        return filtered;
    }

    private static Plan bestPlan(
            ConjunctiveQuery query,
            List<Component> components,
            ToLongFunction<CPQ> costFn,
            int[] eliminationOrder,
            long deadlineNanos) {
        checkDeadline(deadlineNanos);
        int edgeCount = query.edges().size();
        int vertexCount = query.vertices().size();

        Map<VarCQ, Integer> varIndex = new HashMap<>(vertexCount);
        for (int i = 0; i < vertexCount; i++) {
            varIndex.put(query.vertices().get(i), i);
        }

        BitSet requiredFree = new BitSet(vertexCount);
        for (VarCQ free : query.freeVariables()) {
            Integer idx = varIndex.get(free);
            if (idx != null) {
                requiredFree.set(idx);
            }
        }

        List<Option> options = buildOptions(query, components, varIndex, requiredFree, costFn);
        if (options.isEmpty()) {
            return null;
        }
        List<List<Option>> byEdge = indexByEdge(options, edgeCount);
        EdgeLayout edgeLayout = buildEdgeLayout(query, varIndex);
        FrontierSchedule frontier = buildFrontierSchedule(edgeLayout, eliminationOrder, edgeCount);

        StateKey initKey = new StateKey(
                new BitSet(edgeCount),
                new BitSet(vertexCount),
                new BitSet(vertexCount),
                new BitSet(vertexCount),
                new BitSet(vertexCount));
        StateValue initValue = StateValue.initial(vertexCount);

        StageState initStage = new StageState(0, initKey);
        Map<StageState, StateValue> best = new HashMap<>();
        best.put(initStage, initValue);
        ArrayDeque<StageState> queue = new ArrayDeque<>();
        queue.add(initStage);

        StateValue bestFinal = null;
        while (!queue.isEmpty()) {
            checkDeadline(deadlineNanos);
            StageState stageState = queue.pollFirst();
            StateValue stateValue = best.get(stageState);
            if (stateValue == null) {
                continue;
            }

            int stage = stageState.stage();
            StateKey stateKey = stageState.key();
            if (stage >= vertexCount) {
                if (!stateKey.covered().isEmpty()) {
                    continue;
                }
                if (!containsAll(stateKey.freeCovered(), requiredFree)) {
                    continue;
                }
                if (isBetter(stateValue, bestFinal)) {
                    bestFinal = stateValue;
                }
                continue;
            }

            BitSet needed = (BitSet) frontier.stageRequiredEdges()[stage].clone();
            needed.andNot(stateKey.covered());
            if (needed.isEmpty()) {
                StateKey advanced = advanceStage(
                        stateKey,
                        eliminationOrder[stage],
                        requiredFree,
                        frontier.processedEdges()[stage + 1]);
                if (advanced == null) {
                    continue;
                }
                StageState nextStage = new StageState(stage + 1, advanced);
                StateValue existing = best.get(nextStage);
                if (isBetter(stateValue, existing)) {
                    best.put(nextStage, stateValue);
                    queue.addLast(nextStage);
                }
                continue;
            }

            int nextEdge = needed.nextSetBit(0);
            BitSet processedEdges = frontier.processedEdges()[stage];
            for (Option option : byEdge.get(nextEdge)) {
                checkDeadline(deadlineNanos);
                if (option.mask().intersects(processedEdges)) {
                    continue;
                }
                if (option.mask().intersects(stateKey.covered())) {
                    continue;
                }
                StateKey nextKey = transition(stateKey, option);
                if (nextKey == null) {
                    continue;
                }
                StateValue candidate = transitionValue(stateValue, option);
                StageState nextState = new StageState(stage, nextKey);
                StateValue existing = best.get(nextState);
                if (isBetter(candidate, existing)) {
                    best.put(nextState, candidate);
                    queue.addLast(nextState);
                }
            }
        }

        if (bestFinal == null) {
            return null;
        }
        List<Component> chosen = new ArrayList<>(bestFinal.components());
        chosen.sort(Comparator
                .comparingInt((Component component) -> firstMaskBit(component.maskUnsafe()))
                .thenComparingInt(component -> component.maskUnsafe().cardinality())
                .thenComparing(Component::sourceVarName)
                .thenComparing(Component::targetVarName)
                .thenComparing(Component::normalized));
        return new Plan(query, chosen);
    }

    private static List<Option> buildOptions(
            ConjunctiveQuery query,
            List<Component> components,
            Map<VarCQ, Integer> varIndex,
            BitSet requiredFree,
            ToLongFunction<CPQ> costFn) {
        List<Option> options = new ArrayList<>(components.size());
        List<AtomCQ> edges = query.edges();
        int vertexCount = query.vertices().size();
        for (Component component : components) {
            BitSet mask = (BitSet) component.maskUnsafe().clone();
            if (mask.isEmpty()) {
                continue;
            }
            BitSet vars = new BitSet(vertexCount);
            for (int edge = mask.nextSetBit(0); edge >= 0; edge = mask.nextSetBit(edge + 1)) {
                AtomCQ atom = edges.get(edge);
                Integer source = varIndex.get(atom.getSource());
                Integer target = varIndex.get(atom.getTarget());
                if (source != null) {
                    vars.set(source);
                }
                if (target != null) {
                    vars.set(target);
                }
            }

            BitSet endpoints = new BitSet(vertexCount);
            Integer source = varIndex.get(component.s());
            Integer target = varIndex.get(component.t());
            if (source != null) {
                endpoints.set(source);
            }
            if (target != null) {
                endpoints.set(target);
            }

            BitSet internals = (BitSet) vars.clone();
            internals.andNot(endpoints);
            BitSet freeEndpoints = (BitSet) endpoints.clone();
            freeEndpoints.and(requiredFree);

            double cardinality = normalizeCardinality(costFn.applyAsLong(component.cpq()));
            double[] ndv = endpointNdv(component, varIndex, cardinality, vertexCount);
            options.add(new Option(component, mask, vars, endpoints, internals, freeEndpoints, cardinality, ndv));
        }

        options.sort(Comparator
                .comparingInt((Option option) -> option.mask().cardinality()).reversed()
                .thenComparingDouble(Option::cardinality)
                .thenComparing(option -> option.component().normalized()));
        return options;
    }

    private static List<List<Option>> indexByEdge(List<Option> options, int edgeCount) {
        List<List<Option>> byEdge = new ArrayList<>(edgeCount);
        for (int i = 0; i < edgeCount; i++) {
            byEdge.add(new ArrayList<>());
        }
        for (Option option : options) {
            BitSet mask = option.mask();
            for (int edge = mask.nextSetBit(0); edge >= 0; edge = mask.nextSetBit(edge + 1)) {
                byEdge.get(edge).add(option);
            }
        }
        return byEdge;
    }

    private static StateKey transition(StateKey state, Option option) {
        BitSet covered = (BitSet) state.covered().clone();
        covered.or(option.mask());

        BitSet once = (BitSet) state.once().clone();
        BitSet many = (BitSet) state.many().clone();
        for (int var = option.vars().nextSetBit(0); var >= 0; var = option.vars().nextSetBit(var + 1)) {
            if (many.get(var)) {
                continue;
            }
            if (once.get(var)) {
                once.clear(var);
                many.set(var);
            } else {
                once.set(var);
            }
        }

        BitSet internal = (BitSet) state.internal().clone();
        internal.or(option.internals());
        if (internal.intersects(many)) {
            return null;
        }

        BitSet freeCovered = (BitSet) state.freeCovered().clone();
        freeCovered.or(option.freeEndpoints());
        return new StateKey(covered, once, many, internal, freeCovered);
    }

    private static StateKey advanceStage(
            StateKey state,
            int forgottenVar,
            BitSet requiredFree,
            BitSet processedEdgesAfterStage) {
        if (requiredFree.get(forgottenVar) && !state.freeCovered().get(forgottenVar)) {
            return null;
        }

        BitSet covered = (BitSet) state.covered().clone();
        covered.andNot(processedEdgesAfterStage);

        BitSet once = (BitSet) state.once().clone();
        once.clear(forgottenVar);
        BitSet many = (BitSet) state.many().clone();
        many.clear(forgottenVar);
        BitSet internal = (BitSet) state.internal().clone();
        internal.clear(forgottenVar);
        BitSet freeCovered = (BitSet) state.freeCovered().clone();
        return new StateKey(covered, once, many, internal, freeCovered);
    }

    private static StateValue transitionValue(StateValue state, Option option) {
        double[] nextNdv = new double[state.ndv().length];
        double outputCardinality;
        double nextCost;

        if (state.components().isEmpty()) {
            outputCardinality = option.cardinality();
            System.arraycopy(option.ndv(), 0, nextNdv, 0, nextNdv.length);
            nextCost = option.cardinality();
        } else {
            double selectivity = 1.0D;
            boolean joined = false;
            for (int i = 0; i < nextNdv.length; i++) {
                double left = state.ndv()[i];
                double right = option.ndv()[i];
                if (left > 0.0D && right > 0.0D) {
                    joined = true;
                    selectivity /= Math.max(left, right);
                }
            }
            outputCardinality = state.outputCardinality() * option.cardinality();
            if (joined) {
                outputCardinality *= selectivity;
            }
            outputCardinality = Math.max(1.0D, outputCardinality);

            for (int i = 0; i < nextNdv.length; i++) {
                double left = state.ndv()[i];
                double right = option.ndv()[i];
                if (left > 0.0D && right > 0.0D) {
                    nextNdv[i] = Math.min(left, right);
                } else if (left > 0.0D) {
                    nextNdv[i] = Math.min(left, outputCardinality);
                } else if (right > 0.0D) {
                    nextNdv[i] = Math.min(right, outputCardinality);
                }
            }

            // System-R style dynamic programming objective.
            nextCost = state.totalCost() + option.cardinality() + outputCardinality;
        }

        List<Component> components = new ArrayList<>(state.components().size() + 1);
        components.addAll(state.components());
        components.add(option.component());
        return new StateValue(components, nextCost, outputCardinality, nextNdv);
    }

    private static boolean isBetter(StateValue candidate, StateValue existing) {
        if (existing == null) {
            return true;
        }
        int cmp = Double.compare(candidate.totalCost(), existing.totalCost());
        if (cmp != 0) {
            return cmp < 0;
        }
        cmp = Integer.compare(candidate.components().size(), existing.components().size());
        if (cmp != 0) {
            return cmp < 0;
        }
        return Double.compare(candidate.outputCardinality(), existing.outputCardinality()) < 0;
    }

    private static boolean containsAll(BitSet superset, BitSet subset) {
        if (subset.isEmpty()) {
            return true;
        }
        BitSet missing = (BitSet) subset.clone();
        missing.andNot(superset);
        return missing.isEmpty();
    }

    private static double[] endpointNdv(
            Component component,
            Map<VarCQ, Integer> varIndex,
            double cardinality,
            int vertexCount) {
        double[] ndv = new double[vertexCount];
        Integer source = varIndex.get(component.s());
        Integer target = varIndex.get(component.t());
        if (source == null || target == null) {
            return ndv;
        }
        if (component.s().equals(component.t())) {
            ndv[source] = Math.max(1.0D, cardinality);
            return ndv;
        }
        double endpointNdv = Math.max(1.0D, Math.sqrt(cardinality));
        ndv[source] = endpointNdv;
        ndv[target] = endpointNdv;
        return ndv;
    }

    private static double normalizeCardinality(long raw) {
        if (raw <= 0L) {
            return 1.0D;
        }
        if (raw == Long.MAX_VALUE) {
            return 1.0E12D;
        }
        return (double) raw;
    }

    private static int firstMaskBit(BitSet mask) {
        int first = mask.nextSetBit(0);
        return first < 0 ? Integer.MAX_VALUE : first;
    }

    private static int[] treewidthTwoEliminationOrder(ConjunctiveQuery query, long deadlineNanos) {
        checkDeadline(deadlineNanos);
        int n = query.vertices().size();
        int[] order = new int[n];
        if (n == 0) {
            return order;
        }

        Map<VarCQ, Integer> index = new HashMap<>(n);
        for (int i = 0; i < n; i++) {
            index.put(query.vertices().get(i), i);
        }

        BitSet[] adjacency = new BitSet[n];
        for (int i = 0; i < n; i++) {
            adjacency[i] = new BitSet(n);
        }
        for (AtomCQ atom : query.edges()) {
            Integer source = index.get(atom.getSource());
            Integer target = index.get(atom.getTarget());
            if (source == null || target == null || source.equals(target)) {
                continue;
            }
            adjacency[source].set(target);
            adjacency[target].set(source);
        }

        BitSet alive = new BitSet(n);
        alive.set(0, n);
        if (!eliminateWidthTwo(adjacency, alive, order, 0, deadlineNanos)) {
            return null;
        }
        return order;
    }

    private static boolean eliminateWidthTwo(
            BitSet[] adjacency,
            BitSet alive,
            int[] order,
            int depth,
            long deadlineNanos) {
        checkDeadline(deadlineNanos);
        if (alive.isEmpty()) {
            return true;
        }

        ArrayList<Integer> candidates = new ArrayList<>();
        for (int v = alive.nextSetBit(0); v >= 0; v = alive.nextSetBit(v + 1)) {
            int degree = aliveDegree(adjacency[v], alive);
            if (degree <= 2) {
                candidates.add(v);
            }
        }
        if (candidates.isEmpty()) {
            return false;
        }
        candidates.sort(Comparator.comparingInt(v -> aliveDegree(adjacency[v], alive)));

        for (int candidate : candidates) {
            checkDeadline(deadlineNanos);
            BitSet[] nextAdjacency = cloneAdjacency(adjacency);
            BitSet nextAlive = (BitSet) alive.clone();
            eliminate(nextAdjacency, nextAlive, candidate);
            order[depth] = candidate;
            if (eliminateWidthTwo(nextAdjacency, nextAlive, order, depth + 1, deadlineNanos)) {
                return true;
            }
        }
        return false;
    }

    private static int aliveDegree(BitSet neighborhood, BitSet alive) {
        BitSet degreeSet = (BitSet) neighborhood.clone();
        degreeSet.and(alive);
        return degreeSet.cardinality();
    }

    private static BitSet[] cloneAdjacency(BitSet[] adjacency) {
        BitSet[] out = new BitSet[adjacency.length];
        for (int i = 0; i < adjacency.length; i++) {
            out[i] = (BitSet) adjacency[i].clone();
        }
        return out;
    }

    private static void eliminate(BitSet[] adjacency, BitSet alive, int vertex) {
        BitSet neighbors = (BitSet) adjacency[vertex].clone();
        neighbors.and(alive);

        int first = neighbors.nextSetBit(0);
        if (first >= 0) {
            int second = neighbors.nextSetBit(first + 1);
            if (second >= 0) {
                adjacency[first].set(second);
                adjacency[second].set(first);
            }
        }

        for (int neighbor = neighbors.nextSetBit(0); neighbor >= 0; neighbor = neighbors.nextSetBit(neighbor + 1)) {
            adjacency[neighbor].clear(vertex);
        }
        adjacency[vertex].clear();
        alive.clear(vertex);
    }

    private static void checkDeadline(long deadlineNanos) {
        if (Thread.currentThread().isInterrupted()) {
            throw new DecompositionTimeoutException("Treewidth-2 DP decomposition interrupted");
        }
        if (deadlineNanos != Long.MAX_VALUE && System.nanoTime() >= deadlineNanos) {
            throw new DecompositionTimeoutException("Treewidth-2 DP decomposition timed out");
        }
    }

    private static EdgeLayout buildEdgeLayout(ConjunctiveQuery query, Map<VarCQ, Integer> varIndex) {
        int edgeCount = query.edges().size();
        int vertexCount = query.vertices().size();
        int[] sourceByEdge = new int[edgeCount];
        int[] targetByEdge = new int[edgeCount];
        Arrays.fill(sourceByEdge, UNBOUND);
        Arrays.fill(targetByEdge, UNBOUND);
        BitSet[] incidentByVertex = new BitSet[vertexCount];
        for (int i = 0; i < vertexCount; i++) {
            incidentByVertex[i] = new BitSet(edgeCount);
        }

        List<AtomCQ> edges = query.edges();
        for (int edge = 0; edge < edgeCount; edge++) {
            AtomCQ atom = edges.get(edge);
            Integer source = varIndex.get(atom.getSource());
            Integer target = varIndex.get(atom.getTarget());
            if (source == null || target == null) {
                continue;
            }
            sourceByEdge[edge] = source;
            targetByEdge[edge] = target;
            incidentByVertex[source].set(edge);
            incidentByVertex[target].set(edge);
        }
        return new EdgeLayout(sourceByEdge, targetByEdge, incidentByVertex);
    }

    private static FrontierSchedule buildFrontierSchedule(
            EdgeLayout edgeLayout,
            int[] eliminationOrder,
            int edgeCount) {
        int vertexCount = eliminationOrder.length;
        BitSet[] stageRequiredEdges = new BitSet[vertexCount];
        BitSet[] processedEdges = new BitSet[vertexCount + 1];
        processedEdges[0] = new BitSet(edgeCount);
        BitSet removed = new BitSet(vertexCount);

        for (int stage = 0; stage < vertexCount; stage++) {
            int forgottenVar = eliminationOrder[stage];
            BitSet required = new BitSet(edgeCount);
            BitSet incident = edgeLayout.incidentByVertex()[forgottenVar];
            for (int edge = incident.nextSetBit(0); edge >= 0; edge = incident.nextSetBit(edge + 1)) {
                int source = edgeLayout.sourceByEdge()[edge];
                int target = edgeLayout.targetByEdge()[edge];
                if (source == UNBOUND || target == UNBOUND) {
                    continue;
                }
                int other = source == forgottenVar ? target : source;
                if (!removed.get(other)) {
                    required.set(edge);
                }
            }
            stageRequiredEdges[stage] = required;

            BitSet nextProcessed = (BitSet) processedEdges[stage].clone();
            nextProcessed.or(required);
            processedEdges[stage + 1] = nextProcessed;
            removed.set(forgottenVar);
        }

        return new FrontierSchedule(stageRequiredEdges, processedEdges);
    }

    private record Option(
            Component component,
            BitSet mask,
            BitSet vars,
            BitSet endpoints,
            BitSet internals,
            BitSet freeEndpoints,
            double cardinality,
            double[] ndv) {
        private Option {
            mask = (BitSet) mask.clone();
            vars = (BitSet) vars.clone();
            endpoints = (BitSet) endpoints.clone();
            internals = (BitSet) internals.clone();
            freeEndpoints = (BitSet) freeEndpoints.clone();
            ndv = ndv.clone();
        }
    }

    private record StateKey(
            BitSet covered,
            BitSet once,
            BitSet many,
            BitSet internal,
            BitSet freeCovered) {
        private StateKey {
            covered = (BitSet) covered.clone();
            once = (BitSet) once.clone();
            many = (BitSet) many.clone();
            internal = (BitSet) internal.clone();
            freeCovered = (BitSet) freeCovered.clone();
        }
    }

    private record StateValue(
            List<Component> components,
            double totalCost,
            double outputCardinality,
            double[] ndv) {
        private StateValue {
            components = List.copyOf(components);
            ndv = ndv.clone();
        }

        private static StateValue initial(int vertexCount) {
            return new StateValue(List.of(), 0.0D, 1.0D, new double[vertexCount]);
        }
    }

    private record StageState(int stage, StateKey key) {
    }

    private record EdgeLayout(
            int[] sourceByEdge,
            int[] targetByEdge,
            BitSet[] incidentByVertex) {
    }

    private record FrontierSchedule(
            BitSet[] stageRequiredEdges,
            BitSet[] processedEdges) {
    }
}
