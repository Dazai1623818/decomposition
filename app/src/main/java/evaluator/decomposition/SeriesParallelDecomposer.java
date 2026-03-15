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
import java.util.IdentityHashMap;
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
            ToLongFunction<CPQ> costFn,
            int restarts,
            long seed) {
        return decomposeCandidates(cq, componentFilter, costFn, restarts, seed, NO_DEADLINE);
    }

    static Stream<Plan> decomposeCandidates(
            CQ cq,
            java.util.function.Predicate<CPQ> componentFilter,
            ToLongFunction<CPQ> costFn,
            int restarts,
            long seed,
            long deadlineNanos) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(costFn, "costFn");
        if (restarts < 0) {
            throw new IllegalArgumentException("restarts must be >= 0");
        }

        checkDeadline(deadlineNanos);
        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        if (query.edges().isEmpty()) {
            return Stream.of(new Plan(query, List.of()));
        }
        if (restarts == 0) {
            return Stream.of(query.decomposeSingleEdge());
        }

        List<Integer> edgeIds = allEdgeIds(query);
        Set<VarCQ> terminals = new HashSet<>(query.freeVariables());
        SeriesParallelReducer reducer = new SeriesParallelReducer(query, componentFilter, deadlineNanos);

        checkDeadline(deadlineNanos);
        Map<String, Plan> unique = new HashMap<>();
        for (int i = 0; i < restarts; i++) {
            checkDeadline(deadlineNanos);
            Random random = new Random(mixSeed(seed, i, query.edges().size(), query.vertices().size()));
            Plan candidate = new Plan(query, reducer.reduceRandomized(edgeIds, terminals, random));
            String signature = planSignature(candidate);
            unique.putIfAbsent(signature, candidate);
        }

        List<Plan> ranked = new ArrayList<>(unique.values());
        Map<Plan, RankedPlan> rankedPlans = new IdentityHashMap<>(ranked.size());
        for (Map.Entry<String, Plan> entry : unique.entrySet()) {
            rankedPlans.put(entry.getValue(), rankPlan(entry.getValue(), costFn, entry.getKey()));
        }
        ranked.sort((left, right) -> compareRankedPlans(rankedPlans.get(left), rankedPlans.get(right)));
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
            parts.add(component.signature());
        }
        parts.sort(String::compareTo);
        return String.join(";", parts);
    }

    private static String stateSignature(Plan plan) {
        List<String> parts = new ArrayList<>(plan.components().size());
        for (Component component : plan.components()) {
            parts.add(stateSignature(component));
        }
        parts.sort(String::compareTo);
        return String.join(";", parts);
    }

    private static String stateSignature(Component component) {
        return component.s().getName()
                + "->" + component.t().getName()
                + "#d=" + component.diameter()
                + "#m=" + component.maskUnsafe();
    }

    /**
     * Keeps randomized series/parallel candidates ordered by total collapse first,
     * with cumulative component cost breaking ties.
     */
    private static int compareRankedPlans(RankedPlan left, RankedPlan right) {
        int cmp = Integer.compare(right.collapsedEdges(), left.collapsedEdges());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Long.compare(left.totalCost(), right.totalCost());
        if (cmp != 0) {
            return cmp;
        }
        return left.signature().compareTo(right.signature());
    }

    private static RankedPlan rankPlan(Plan plan, ToLongFunction<CPQ> costFn, String signature) {
        int collapsedEdges = Math.max(0, plan.cq().edges().size() - plan.components().size());
        long totalCost = 0L;
        for (Component component : plan.components()) {
            totalCost = safeAddCost(totalCost, costFn.applyAsLong(component.cpq()));
        }
        return new RankedPlan(plan, collapsedEdges, totalCost, signature);
    }

    private static long safeAddCost(long left, long right) {
        long normalizedRight = Math.max(0L, right);
        if (left >= Long.MAX_VALUE || normalizedRight >= Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }
        if (left > Long.MAX_VALUE - normalizedRight) {
            return Long.MAX_VALUE;
        }
        return left + normalizedRight;
    }

    private static Component toComponent(ReducedEdge edge) {
        CpqDeduplication.NormalizedCpq normalized = CpqDeduplication.normalizeCpq(edge.cpq);
        return new Component(edge.s, edge.t, edge.diameter, edge.mask, normalized.cpq(), normalized.normalized());
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
                out.add(toComponent(edge));
            }
            return out;
        }

        private static List<ReducedEdge> afterMerge(List<ReducedEdge> edges, MergeCandidate candidate) {
            Set<ReducedEdge> consumed = Set.copyOf(candidate.consumed());
            List<ReducedEdge> next = new ArrayList<>(edges.size() - consumed.size() + 1);
            for (ReducedEdge edge : edges) {
                if (!consumed.contains(edge)) {
                    next.add(copy(edge));
                }
            }
            next.add(copy(candidate.merged()));
            return next;
        }

        private boolean reduceParallel(List<ReducedEdge> edges) {
            List<ParallelMerge> candidates = parallelMerges(edges, cpq -> 0L);
            if (candidates.isEmpty()) {
                return false;
            }
            applyParallelMerge(edges, candidates.get(0));
            return true;
        }

        private boolean reduceParallelRandom(List<ReducedEdge> edges, Random random) {
            List<ParallelMerge> candidates = parallelMerges(edges, cpq -> 0L);
            if (candidates.isEmpty()) {
                return false;
            }
            ParallelMerge chosen = candidates.get(random.nextInt(candidates.size()));
            applyParallelMerge(edges, chosen);
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
                        List.of(merge.left(), merge.right()),
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
            return parallelMerges(edges, costFn);
        }

        private List<ParallelMerge> parallelMerges(List<ReducedEdge> edges, ToLongFunction<CPQ> costFn) {
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
                for (int i = 0; i < group.size(); i++) {
                    ReducedEdge left = group.get(i);
                    for (int j = i + 1; j < group.size(); j++) {
                        checkDeadline(deadlineNanos);
                        ReducedEdge right = group.get(j);
                        CPQ merged = ensureUnary(CPQ.intersect(
                                oriented(left, s, t),
                                oriented(right, s, t)), s, t);
                        if (componentFilter != null && !componentFilter.test(merged)) {
                            continue;
                        }
                        BitSet mask = union(left.mask, right.mask);
                        int diameter = Math.max(left.diameter, right.diameter);
                        long cardinality = safeCost(costFn.applyAsLong(merged));
                        ReducedEdge mergedEdge = new ReducedEdge(s, t, merged, mask, diameter, cardinality);
                        candidates.add(new ParallelMerge(
                                left,
                                right,
                                s,
                                t,
                                merged,
                                mask,
                                diameter,
                                cardinality,
                                mergeSignature(MergeKind.PARALLEL, mergedEdge)));
                    }
                }
            }
            candidates.sort(Comparator.comparing(ParallelMerge::signature));
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
            Set<ReducedEdge> consumed = Set.copyOf(candidate.consumed());
            for (ReducedEdge edge : edges) {
                if (!edge.active || consumed.contains(edge)) {
                    continue;
                }
                components.add(toComponent(edge));
            }
            ReducedEdge merged = candidate.merged();
            components.add(toComponent(merged));
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
                    + toComponent(merged).signature();
        }

        private static void applyParallelMerge(List<ReducedEdge> edges, ParallelMerge merge) {
            merge.left().active = false;
            merge.right().active = false;
            edges.add(new ReducedEdge(
                    merge.s(),
                    merge.t(),
                    merge.cpq(),
                    merge.mask(),
                    merge.diameter(),
                    merge.cardinality()));
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

        private static ReducedEdge copy(ReducedEdge edge) {
            return new ReducedEdge(edge.s, edge.t, edge.cpq, edge.mask, edge.diameter, edge.cardinality);
        }

        private record ParallelMerge(
                ReducedEdge left,
                ReducedEdge right,
                VarCQ s,
                VarCQ t,
                CPQ cpq,
                BitSet mask,
                int diameter,
                long cardinality,
                String signature) {
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

    private record RankedPlan(
            Plan plan,
            int collapsedEdges,
            long totalCost,
            String signature) {
    }
}
