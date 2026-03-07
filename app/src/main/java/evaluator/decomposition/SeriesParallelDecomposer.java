package evaluator.decomposition;

import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

final class SeriesParallelDecomposer {
    private SeriesParallelDecomposer() {
    }

    private static final long NO_DEADLINE = Long.MAX_VALUE;
    private static final double GUIDED_SERIES_HARD_BLOWUP = 64.0D;

    static Stream<Plan> decomposeGreedy(CQ cq) {
        return decomposeGreedy(cq, null);
    }

    static Stream<Plan> decomposeGreedy(CQ cq, java.util.function.Predicate<CPQ> componentFilter) {
        Objects.requireNonNull(cq, "cq");
        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        if (query.edges().isEmpty()) {
            return Stream.of(new Plan(query, List.of()));
        }

        Set<VarCQ> terminals = new HashSet<>(query.freeVariables());
        SeriesParallelReducer reducer = new SeriesParallelReducer(query, componentFilter, NO_DEADLINE);
        List<Component> components = reducer.reduce(allEdgeIds(query), terminals);
        return Stream.of(new Plan(query, components));
    }

    static Stream<Plan> decomposeCandidates(
            CQ cq,
            java.util.function.Predicate<CPQ> componentFilter,
            int restarts,
            int maxPlans,
            long seed) {
        return decomposeCandidates(cq, componentFilter, restarts, maxPlans, seed, NO_DEADLINE);
    }

    static Stream<Plan> decomposeCandidates(
            CQ cq,
            java.util.function.Predicate<CPQ> componentFilter,
            int restarts,
            int maxPlans,
            long seed,
            long deadlineNanos) {
        Objects.requireNonNull(cq, "cq");
        if (restarts < 1) {
            throw new IllegalArgumentException("restarts must be >= 1");
        }
        if (maxPlans < 1) {
            throw new IllegalArgumentException("maxPlans must be >= 1");
        }

        checkDeadline(deadlineNanos);
        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        if (query.edges().isEmpty()) {
            return Stream.of(new Plan(query, List.of()));
        }

        List<Integer> edgeIds = allEdgeIds(query);
        Set<VarCQ> terminals = new HashSet<>(query.freeVariables());
        SeriesParallelReducer reducer = new SeriesParallelReducer(query, componentFilter, deadlineNanos);

        checkDeadline(deadlineNanos);
        Map<String, Plan> unique = new HashMap<>();
        Plan deterministic = new Plan(query, reducer.reduce(edgeIds, terminals));
        unique.put(planSignature(deterministic), deterministic);

        for (int i = 0; i < restarts; i++) {
            checkDeadline(deadlineNanos);
            Random random = new Random(mixSeed(seed, i, query.edges().size(), query.vertices().size()));
            Plan candidate = new Plan(query, reducer.reduceRandomized(edgeIds, terminals, random));
            String signature = planSignature(candidate);
            Plan existing = unique.get(signature);
            if (existing == null || comparePlans(candidate, existing) < 0) {
                unique.put(signature, candidate);
            }
        }

        List<Plan> ranked = new ArrayList<>(unique.values());
        ranked.sort(SeriesParallelDecomposer::comparePlans);
        if (ranked.size() > maxPlans) {
            ranked = ranked.subList(0, maxPlans);
        }
        return ranked.stream();
    }

    static Stream<Plan> decomposeGuided(
            CQ cq,
            java.util.function.Predicate<CPQ> componentFilter,
            ToLongFunction<CPQ> costFn,
            ToDoubleFunction<Plan> joinScoreFn,
            long deadlineNanos) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(costFn, "costFn");
        Objects.requireNonNull(joinScoreFn, "joinScoreFn");

        checkDeadline(deadlineNanos);
        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        if (query.edges().isEmpty()) {
            return Stream.of(new Plan(query, List.of()));
        }

        List<Integer> edgeIds = allEdgeIds(query);
        Set<VarCQ> terminals = new HashSet<>(query.freeVariables());
        SeriesParallelReducer reducer = new SeriesParallelReducer(query, componentFilter, deadlineNanos);
        List<Component> components = reducer.reduceGuided(edgeIds, terminals, costFn, joinScoreFn);
        return Stream.of(new Plan(query, components));
    }

    private static List<Integer> allEdgeIds(ConjunctiveQuery query) {
        List<Integer> edgeIds = new ArrayList<>(query.edges().size());
        for (int i = 0; i < query.edges().size(); i++) {
            edgeIds.add(i);
        }
        return edgeIds;
    }

    private static String planSignature(Plan plan) {
        List<String> parts = new ArrayList<>(plan.components().size());
        for (Component component : plan.components()) {
            parts.add(component.sourceVarName() + "->"
                    + component.targetVarName() + "|"
                    + component.maskUnsafe() + "|"
                    + component.cpq());
        }
        parts.sort(String::compareTo);
        return String.join(";", parts);
    }

    private static int comparePlans(Plan left, Plan right) {
        int cmp = Integer.compare(left.components().size(), right.components().size());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(left.maxDiameter(), right.maxDiameter());
        if (cmp != 0) {
            return cmp;
        }
        return planSignature(left).compareTo(planSignature(right));
    }

    private static long mixSeed(long seed, int restart, int edgeCount, int vertexCount) {
        long mixed = seed;
        mixed ^= 0x9E3779B97F4A7C15L * (restart + 1L);
        mixed ^= ((long) edgeCount << 21);
        mixed ^= ((long) vertexCount << 9);
        return mixed;
    }

    private static void checkDeadline(long deadlineNanos) {
        if (deadlineNanos != NO_DEADLINE && System.nanoTime() >= deadlineNanos) {
            throw new Decomposer.DecompositionTimeoutException("Series-parallel decomposition timed out");
        }
    }

    /**
     * Greedy series/parallel reducer that preserves terminal variables and
     * maintains disjoint edge masks for reduced components.
     */
    private static final class SeriesParallelReducer {
        private static final Comparator<VarCQ> VAR_ORDER = Comparator.comparing(VarCQ::getName);
        private final ConjunctiveQuery query;
        private final List<AtomCQ> atoms;
        private final java.util.function.Predicate<CPQ> componentFilter;
        private final long deadlineNanos;

        SeriesParallelReducer(
                ConjunctiveQuery query,
                java.util.function.Predicate<CPQ> componentFilter,
                long deadlineNanos) {
            this.query = query;
            this.atoms = query.edges();
            this.componentFilter = componentFilter;
            this.deadlineNanos = deadlineNanos;
        }

        List<Component> reduce(List<Integer> edgeIds, Set<VarCQ> terminals) {
            return reduceInternal(edgeIds, terminals, null);
        }

        List<Component> reduceRandomized(List<Integer> edgeIds, Set<VarCQ> terminals, Random random) {
            Objects.requireNonNull(random, "random");
            return reduceInternal(edgeIds, terminals, random);
        }

        List<Component> reduceGuided(
                List<Integer> edgeIds,
                Set<VarCQ> terminals,
                ToLongFunction<CPQ> costFn,
                ToDoubleFunction<Plan> joinScoreFn) {
            Objects.requireNonNull(costFn, "costFn");
            Objects.requireNonNull(joinScoreFn, "joinScoreFn");
            checkDeadline(deadlineNanos);

            List<ReducedEdge> edges = initialReducedEdges(edgeIds, costFn);
            while (true) {
                checkDeadline(deadlineNanos);
                List<MergeCandidate> candidates = enumerateMergeCandidates(edges, terminals, costFn);
                if (candidates.isEmpty()) {
                    break;
                }

                MergeChoice best = null;
                for (MergeCandidate candidate : candidates) {
                    checkDeadline(deadlineNanos);
                    MergeChoice choice = scoreMergeCandidate(edges, candidate, joinScoreFn);
                    if (choice == null) {
                        continue;
                    }
                    if (best == null || compareMergeChoice(choice, best) < 0) {
                        best = choice;
                    }
                }

                if (best == null) {
                    break;
                }
                applyMerge(edges, best.candidate());
            }

            return collectActiveComponents(edges);
        }

        private List<Component> reduceInternal(List<Integer> edgeIds, Set<VarCQ> terminals, Random random) {
            checkDeadline(deadlineNanos);
            List<ReducedEdge> edges = initialReducedEdges(edgeIds, cpq -> 0L);

            while (true) {
                checkDeadline(deadlineNanos);
                boolean changedParallel = random == null ? reduceParallel(edges) : reduceParallelRandom(edges, random);
                if (changedParallel) {
                    continue;
                }
                boolean changedSeries = random == null
                        ? reduceSeries(edges, terminals)
                        : reduceSeriesRandom(edges, terminals, random);
                if (changedSeries) {
                    continue;
                }
                break;
            }

            return collectActiveComponents(edges);
        }

        private List<ReducedEdge> initialReducedEdges(List<Integer> edgeIds, ToLongFunction<CPQ> costFn) {
            List<ReducedEdge> edges = new ArrayList<>(edgeIds.size());
            for (int edgeId : edgeIds) {
                checkDeadline(deadlineNanos);
                AtomCQ atom = atoms.get(edgeId);
                VarCQ s = atom.getSource();
                VarCQ t = atom.getTarget();
                CPQ cpq = CPQ.label(atom.getLabel());
                if (s.equals(t)) {
                    cpq = CPQ.intersect(cpq, CPQ.id());
                }
                BitSet mask = new BitSet();
                mask.set(edgeId);
                long cardinality = safeCost(costFn.applyAsLong(cpq));
                edges.add(new ReducedEdge(s, t, cpq, mask, 1, cardinality));
            }
            return edges;
        }

        private List<Component> collectActiveComponents(List<ReducedEdge> edges) {
            List<Component> out = new ArrayList<>();
            for (ReducedEdge edge : edges) {
                checkDeadline(deadlineNanos);
                if (!edge.active) {
                    continue;
                }
                out.add(new Component(edge.s, edge.t, edge.diameter, edge.mask, edge.cpq, edge.cpq.toString()));
            }
            return out;
        }

        private boolean reduceParallel(List<ReducedEdge> edges) {
            Map<EndpointPair, List<ReducedEdge>> groups = new HashMap<>();
            for (ReducedEdge edge : edges) {
                checkDeadline(deadlineNanos);
                if (!edge.active) {
                    continue;
                }
                EndpointPair key = EndpointPair.ordered(edge.s, edge.t);
                groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(edge);
            }

            List<EndpointPair> orderedPairs = new ArrayList<>(groups.keySet());
            orderedPairs.sort(Comparator.comparing((EndpointPair p) -> p.s.getName())
                    .thenComparing(p -> p.t.getName()));

            for (EndpointPair pair : orderedPairs) {
                checkDeadline(deadlineNanos);
                List<ReducedEdge> group = groups.get(pair);
                if (group.size() < 2) {
                    continue;
                }
                VarCQ s = pair.s;
                VarCQ t = pair.t;
                CPQ merged = null;
                int diameter = 0;
                BitSet mask = new BitSet();
                for (ReducedEdge edge : group) {
                    checkDeadline(deadlineNanos);
                    CPQ part = oriented(edge, s, t);
                    merged = merged == null ? part : CPQ.intersect(merged, part);
                    diameter = Math.max(diameter, edge.diameter);
                    mask.or(edge.mask);
                }
                merged = ensureUnary(merged, s, t);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }
                for (ReducedEdge edge : group) {
                    edge.active = false;
                }
                edges.add(new ReducedEdge(s, t, merged, mask, diameter));
                return true;
            }
            return false;
        }

        private boolean reduceParallelRandom(List<ReducedEdge> edges, Random random) {
            Map<EndpointPair, List<ReducedEdge>> groups = new HashMap<>();
            for (ReducedEdge edge : edges) {
                checkDeadline(deadlineNanos);
                if (!edge.active) {
                    continue;
                }
                EndpointPair key = EndpointPair.ordered(edge.s, edge.t);
                groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(edge);
            }

            List<EndpointPair> orderedPairs = new ArrayList<>(groups.keySet());
            orderedPairs.sort(Comparator.comparing((EndpointPair p) -> p.s.getName())
                    .thenComparing(p -> p.t.getName()));

            List<ParallelMerge> candidates = new ArrayList<>();
            for (EndpointPair pair : orderedPairs) {
                checkDeadline(deadlineNanos);
                List<ReducedEdge> group = groups.get(pair);
                if (group.size() < 2) {
                    continue;
                }
                VarCQ s = pair.s;
                VarCQ t = pair.t;
                CPQ merged = null;
                int diameter = 0;
                BitSet mask = new BitSet();
                for (ReducedEdge edge : group) {
                    checkDeadline(deadlineNanos);
                    CPQ part = oriented(edge, s, t);
                    merged = merged == null ? part : CPQ.intersect(merged, part);
                    diameter = Math.max(diameter, edge.diameter);
                    mask.or(edge.mask);
                }
                merged = ensureUnary(merged, s, t);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }
                candidates.add(new ParallelMerge(group, s, t, merged, mask, diameter, 0L));
            }

            if (candidates.isEmpty()) {
                return false;
            }
            ParallelMerge chosen = candidates.get(random.nextInt(candidates.size()));
            for (ReducedEdge edge : chosen.group()) {
                edge.active = false;
            }
            edges.add(new ReducedEdge(
                    chosen.s(),
                    chosen.t(),
                    chosen.cpq(),
                    chosen.mask(),
                    chosen.diameter(),
                    chosen.cardinality()));
            return true;
        }

        private boolean reduceSeries(List<ReducedEdge> edges, Set<VarCQ> terminals) {
            Map<VarCQ, List<ReducedEdge>> incident = new HashMap<>();
            for (ReducedEdge edge : edges) {
                checkDeadline(deadlineNanos);
                if (!edge.active) {
                    continue;
                }
                incident.computeIfAbsent(edge.s, ignored -> new ArrayList<>()).add(edge);
                if (!edge.s.equals(edge.t)) {
                    incident.computeIfAbsent(edge.t, ignored -> new ArrayList<>()).add(edge);
                }
            }

            List<VarCQ> vertices = new ArrayList<>(incident.keySet());
            vertices.sort(VAR_ORDER);
            for (VarCQ v : vertices) {
                checkDeadline(deadlineNanos);
                if (terminals.contains(v)) {
                    continue;
                }
                List<ReducedEdge> list = incident.get(v);
                if (list.size() != 2) {
                    continue;
                }
                ReducedEdge left = list.get(0);
                ReducedEdge right = list.get(1);
                if (left == right || left.isLoop() || right.isLoop()) {
                    continue;
                }
                VarCQ a = left.other(v);
                VarCQ b = right.other(v);
                if (a == null || b == null) {
                    throw new IllegalStateException("Series reduction edge not incident to vertex");
                }
                if (a.equals(b)) {
                    continue;
                }

                CPQ first = oriented(left, a, v);
                CPQ second = oriented(right, v, b);
                CPQ merged = CPQ.concat(first, second);
                merged = ensureUnary(merged, a, b);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }
                BitSet mask = union(left.mask, right.mask);
                int diameter = left.diameter + right.diameter;

                left.active = false;
                right.active = false;
                edges.add(new ReducedEdge(a, b, merged, mask, diameter));
                return true;
            }
            return false;
        }

        private boolean reduceSeriesRandom(List<ReducedEdge> edges, Set<VarCQ> terminals, Random random) {
            Map<VarCQ, List<ReducedEdge>> incident = new HashMap<>();
            for (ReducedEdge edge : edges) {
                checkDeadline(deadlineNanos);
                if (!edge.active) {
                    continue;
                }
                incident.computeIfAbsent(edge.s, ignored -> new ArrayList<>()).add(edge);
                if (!edge.s.equals(edge.t)) {
                    incident.computeIfAbsent(edge.t, ignored -> new ArrayList<>()).add(edge);
                }
            }

            List<VarCQ> vertices = new ArrayList<>(incident.keySet());
            vertices.sort(VAR_ORDER);
            List<SeriesMerge> candidates = new ArrayList<>();
            for (VarCQ v : vertices) {
                checkDeadline(deadlineNanos);
                if (terminals.contains(v)) {
                    continue;
                }
                List<ReducedEdge> list = incident.get(v);
                if (list.size() != 2) {
                    continue;
                }
                ReducedEdge left = list.get(0);
                ReducedEdge right = list.get(1);
                if (left == right || left.isLoop() || right.isLoop()) {
                    continue;
                }
                VarCQ a = left.other(v);
                VarCQ b = right.other(v);
                if (a == null || b == null) {
                    throw new IllegalStateException("Series reduction edge not incident to vertex");
                }
                if (a.equals(b)) {
                    continue;
                }

                CPQ first = oriented(left, a, v);
                CPQ second = oriented(right, v, b);
                CPQ merged = CPQ.concat(first, second);
                merged = ensureUnary(merged, a, b);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }
                BitSet mask = union(left.mask, right.mask);
                int diameter = left.diameter + right.diameter;
                candidates.add(new SeriesMerge(left, right, a, b, merged, mask, diameter, 0L));
            }

            if (candidates.isEmpty()) {
                return false;
            }
            SeriesMerge chosen = candidates.get(random.nextInt(candidates.size()));
            chosen.left().active = false;
            chosen.right().active = false;
            edges.add(new ReducedEdge(
                    chosen.a(),
                    chosen.b(),
                    chosen.cpq(),
                    chosen.mask(),
                    chosen.diameter(),
                    chosen.cardinality()));
            return true;
        }

        private List<MergeCandidate> enumerateMergeCandidates(
                List<ReducedEdge> edges,
                Set<VarCQ> terminals,
                ToLongFunction<CPQ> costFn) {
            List<MergeCandidate> out = new ArrayList<>();
            for (ParallelMerge merge : guidedParallelMerges(edges, costFn)) {
                ReducedEdge mergedEdge = new ReducedEdge(
                        merge.s(),
                        merge.t(),
                        merge.cpq(),
                        merge.mask(),
                        merge.diameter(),
                        merge.cardinality());
                out.add(new MergeCandidate(
                        MergeKind.PARALLEL,
                        List.copyOf(merge.group()),
                        mergedEdge,
                        mergeSignature(MergeKind.PARALLEL, mergedEdge)));
            }
            for (SeriesMerge merge : guidedSeriesMerges(edges, terminals, costFn)) {
                ReducedEdge mergedEdge = new ReducedEdge(
                        merge.a(),
                        merge.b(),
                        merge.cpq(),
                        merge.mask(),
                        merge.diameter(),
                        merge.cardinality());
                out.add(new MergeCandidate(
                        MergeKind.SERIES,
                        List.of(merge.left(), merge.right()),
                        mergedEdge,
                        mergeSignature(MergeKind.SERIES, mergedEdge)));
            }
            out.sort(Comparator.comparing(MergeCandidate::signature));
            return out;
        }

        private List<ParallelMerge> guidedParallelMerges(List<ReducedEdge> edges, ToLongFunction<CPQ> costFn) {
            Map<EndpointPair, List<ReducedEdge>> groups = new HashMap<>();
            for (ReducedEdge edge : edges) {
                checkDeadline(deadlineNanos);
                if (!edge.active) {
                    continue;
                }
                EndpointPair key = EndpointPair.ordered(edge.s, edge.t);
                groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(edge);
            }

            List<EndpointPair> orderedPairs = new ArrayList<>(groups.keySet());
            orderedPairs.sort(Comparator.comparing((EndpointPair p) -> p.s.getName())
                    .thenComparing(p -> p.t.getName()));

            List<ParallelMerge> candidates = new ArrayList<>();
            for (EndpointPair pair : orderedPairs) {
                checkDeadline(deadlineNanos);
                List<ReducedEdge> group = groups.get(pair);
                if (group.size() < 2) {
                    continue;
                }
                VarCQ s = pair.s;
                VarCQ t = pair.t;
                CPQ merged = null;
                int diameter = 0;
                BitSet mask = new BitSet();
                for (ReducedEdge edge : group) {
                    checkDeadline(deadlineNanos);
                    CPQ part = oriented(edge, s, t);
                    merged = merged == null ? part : CPQ.intersect(merged, part);
                    diameter = Math.max(diameter, edge.diameter);
                    mask.or(edge.mask);
                }
                merged = ensureUnary(merged, s, t);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }
                long cardinality = safeCost(costFn.applyAsLong(merged));
                candidates.add(new ParallelMerge(group, s, t, merged, mask, diameter, cardinality));
            }
            return candidates;
        }

        private List<SeriesMerge> guidedSeriesMerges(
                List<ReducedEdge> edges,
                Set<VarCQ> terminals,
                ToLongFunction<CPQ> costFn) {
            Map<VarCQ, List<ReducedEdge>> incident = new HashMap<>();
            for (ReducedEdge edge : edges) {
                checkDeadline(deadlineNanos);
                if (!edge.active) {
                    continue;
                }
                incident.computeIfAbsent(edge.s, ignored -> new ArrayList<>()).add(edge);
                if (!edge.s.equals(edge.t)) {
                    incident.computeIfAbsent(edge.t, ignored -> new ArrayList<>()).add(edge);
                }
            }

            List<VarCQ> vertices = new ArrayList<>(incident.keySet());
            vertices.sort(VAR_ORDER);
            List<SeriesMerge> candidates = new ArrayList<>();
            for (VarCQ v : vertices) {
                checkDeadline(deadlineNanos);
                if (terminals.contains(v)) {
                    continue;
                }
                List<ReducedEdge> list = incident.get(v);
                if (list.size() != 2) {
                    continue;
                }
                ReducedEdge left = list.get(0);
                ReducedEdge right = list.get(1);
                if (left == right || left.isLoop() || right.isLoop()) {
                    continue;
                }
                VarCQ a = left.other(v);
                VarCQ b = right.other(v);
                if (a == null || b == null) {
                    throw new IllegalStateException("Series reduction edge not incident to vertex");
                }
                if (a.equals(b)) {
                    continue;
                }

                CPQ first = oriented(left, a, v);
                CPQ second = oriented(right, v, b);
                CPQ merged = CPQ.concat(first, second);
                merged = ensureUnary(merged, a, b);
                if (componentFilter != null && !componentFilter.test(merged)) {
                    continue;
                }

                BitSet mask = union(left.mask, right.mask);
                int diameter = left.diameter + right.diameter;
                long cardinality = safeCost(costFn.applyAsLong(merged));
                candidates.add(new SeriesMerge(left, right, a, b, merged, mask, diameter, cardinality));
            }
            return candidates;
        }

        private MergeChoice scoreMergeCandidate(
                List<ReducedEdge> edges,
                MergeCandidate candidate,
                ToDoubleFunction<Plan> joinScoreFn) {
            double mergedCardinality = Math.max(1.0D, candidate.merged().cardinality);
            double inputMin = Double.POSITIVE_INFINITY;
            for (ReducedEdge edge : candidate.consumed()) {
                inputMin = Math.min(inputMin, Math.max(1.0D, edge.cardinality));
            }
            double blowupRatio = mergedCardinality / Math.max(1.0D, inputMin);
            if (candidate.kind() == MergeKind.SERIES && blowupRatio > GUIDED_SERIES_HARD_BLOWUP) {
                return null;
            }

            List<Component> components = componentsAfterMerge(edges, candidate);
            Plan plan = new Plan(query, components);
            double score = joinScoreFn.applyAsDouble(plan);

            return new MergeChoice(
                    candidate,
                    score,
                    (long) Math.ceil(mergedCardinality),
                    candidate.kind() == MergeKind.PARALLEL ? 0 : 1,
                    candidate.signature());
        }

        private List<Component> componentsAfterMerge(List<ReducedEdge> edges, MergeCandidate candidate) {
            List<Component> components = new ArrayList<>();
            for (ReducedEdge edge : edges) {
                if (!edge.active || candidate.consumed().contains(edge)) {
                    continue;
                }
                components.add(new Component(edge.s, edge.t, edge.diameter, edge.mask, edge.cpq, edge.cpq.toString()));
            }
            ReducedEdge merged = candidate.merged();
            components.add(new Component(
                    merged.s,
                    merged.t,
                    merged.diameter,
                    merged.mask,
                    merged.cpq,
                    merged.cpq.toString()));
            return components;
        }

        private static int compareMergeChoice(MergeChoice left, MergeChoice right) {
            int cmp = Double.compare(left.score(), right.score());
            if (cmp != 0) {
                return cmp;
            }
            cmp = Long.compare(left.mergedCardinality(), right.mergedCardinality());
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(left.kindOrder(), right.kindOrder());
            if (cmp != 0) {
                return cmp;
            }
            return left.signature().compareTo(right.signature());
        }

        private static String mergeSignature(MergeKind kind, ReducedEdge merged) {
            return kind.name()
                    + "|"
                    + merged.s.getName()
                    + "->"
                    + merged.t.getName()
                    + "|d="
                    + merged.diameter
                    + "|m="
                    + merged.mask
                    + "|c="
                    + merged.cpq;
        }

        private static void applyMerge(List<ReducedEdge> edges, MergeCandidate candidate) {
            for (ReducedEdge consumed : candidate.consumed()) {
                consumed.active = false;
            }
            edges.add(candidate.merged());
        }

        private static long safeCost(long raw) {
            return Math.max(0L, raw);
        }

        private static CPQ oriented(ReducedEdge edge, VarCQ s, VarCQ t) {
            if (edge.s.equals(s) && edge.t.equals(t)) {
                return edge.cpq;
            }
            if (edge.s.equals(t) && edge.t.equals(s)) {
                return invert(edge.cpq);
            }
            throw new IllegalStateException("Edge endpoints do not match requested orientation");
        }

        private static CPQ ensureUnary(CPQ cpq, VarCQ s, VarCQ t) {
            if (s.equals(t)) {
                return CPQ.intersect(cpq, CPQ.id());
            }
            return cpq;
        }

        private static BitSet union(BitSet left, BitSet right) {
            BitSet out = (BitSet) left.clone();
            out.or(right);
            return out;
        }

        private static CPQ invert(CPQ cpq) {
            return invertTree(cpq.toAbstractSyntaxTree());
        }

        private static CPQ invertTree(QueryTree node) {
            return switch (node.getOperation()) {
                case IDENTITY -> CPQ.id();
                case EDGE -> {
                    Predicate label = node.getEdgeAtom().getLabel();
                    yield CPQ.label(label.getInverse());
                }
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

        private record ParallelMerge(
                List<ReducedEdge> group,
                VarCQ s,
                VarCQ t,
                CPQ cpq,
                BitSet mask,
                int diameter,
                long cardinality) {
        }

        private record SeriesMerge(
                ReducedEdge left,
                ReducedEdge right,
                VarCQ a,
                VarCQ b,
                CPQ cpq,
                BitSet mask,
                int diameter,
                long cardinality) {
        }

        private enum MergeKind {
            PARALLEL,
            SERIES
        }

        private record MergeCandidate(
                MergeKind kind,
                List<ReducedEdge> consumed,
                ReducedEdge merged,
                String signature) {
        }

        private record MergeChoice(
                MergeCandidate candidate,
                double score,
                long mergedCardinality,
                int kindOrder,
                String signature) {
        }
    }

    private static final class ReducedEdge {
        private final VarCQ s;
        private final VarCQ t;
        private final CPQ cpq;
        private final BitSet mask;
        private final int diameter;
        private final long cardinality;
        private boolean active;

        private ReducedEdge(VarCQ s, VarCQ t, CPQ cpq, BitSet mask, int diameter) {
            this(s, t, cpq, mask, diameter, 0L);
        }

        private ReducedEdge(VarCQ s, VarCQ t, CPQ cpq, BitSet mask, int diameter, long cardinality) {
            this.s = Objects.requireNonNull(s, "s");
            this.t = Objects.requireNonNull(t, "t");
            this.cpq = Objects.requireNonNull(cpq, "cpq");
            this.mask = (BitSet) Objects.requireNonNull(mask, "mask").clone();
            this.diameter = diameter;
            this.cardinality = Math.max(0L, cardinality);
            this.active = true;
        }

        private boolean isLoop() {
            return s.equals(t);
        }

        private VarCQ other(VarCQ v) {
            if (s.equals(v)) {
                return t;
            }
            if (t.equals(v)) {
                return s;
            }
            return null;
        }
    }

    private record EndpointPair(VarCQ s, VarCQ t) {
        private static EndpointPair ordered(VarCQ a, VarCQ b) {
            return a.getName().compareTo(b.getName()) <= 0
                    ? new EndpointPair(a, b)
                    : new EndpointPair(b, a);
        }
    }
}
