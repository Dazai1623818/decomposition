package evaluator.evaluation;

import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.decomposition.Decomposer;
import evaluator.decomposition.Decomposer.DecompositionTimeoutException;
import evaluator.index.CpqIndex;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Planner that enumerates decomposition candidates from supported methods.
 */
public final class Planner {
    private static final long EXPERIMENTAL_SERIES_PARALLEL_DEFAULT_SEED = 0x9E3779B97F4A7C15L;
    private static final int EXPERIMENTAL_SERIES_PARALLEL_RESTARTS_PER_PLAN = 2;
    private static final String METHODS_PROPERTY = "cpq.decompose.methods";

    private final CpqIndex index;
    private final Set<DecompositionMethod> methodAllowlist;
    private final long seriesParallelSeed;

    public Planner(CpqIndex index) {
        this(index, EXPERIMENTAL_SERIES_PARALLEL_DEFAULT_SEED);
    }

    public Planner(CpqIndex index, long seriesParallelSeed) {
        this.index = Objects.requireNonNull(index, "index");
        this.methodAllowlist = parseMethodAllowlistProperty(METHODS_PROPERTY);
        this.seriesParallelSeed = seriesParallelSeed;
    }

    public Plan decompose(ConjunctiveQuery cq) {
        Objects.requireNonNull(cq, "cq");
        return ensureIndexable(cq.decomposeSingleEdge());
    }

    public Set<DecompositionMethod> supportedMethods() {
        EnumSet<DecompositionMethod> methods = EnumSet.of(
                DecompositionMethod.SINGLE_EDGE,
                DecompositionMethod.COST,
                DecompositionMethod.COST_OVERLAP,
                DecompositionMethod.DIAMETER,
                DecompositionMethod.SERIES_PARALLEL,
                DecompositionMethod.SERIES_PARALLEL_OVERLAP);
        if (!methodAllowlist.isEmpty()) {
            methods.retainAll(methodAllowlist);
            if (methods.isEmpty()) {
                throw new IllegalStateException(
                        "No enabled decomposition methods remain after applying "
                                + METHODS_PROPERTY
                                + "="
                                + String.join(",", methodNames(methodAllowlist)));
            }
        }
        return Set.copyOf(methods);
    }

    public List<Candidate> planAll(ConjunctiveQuery cq, int coverLimit) {
        return planAll(cq, coverLimit, index.k(), 0).candidates();
    }

    public List<Candidate> planAll(ConjunctiveQuery cq, int coverLimit, int k) {
        return planAll(cq, coverLimit, k, 0).candidates();
    }

    public Selection planAll(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        Objects.requireNonNull(cq, "cq");
        validatePlanArguments(coverLimit, k, decompositionTimeoutMs);

        List<Candidate> results = new ArrayList<>();
        EnumSet<DecompositionMethod> timedOutMethods = EnumSet.noneOf(DecompositionMethod.class);
        EnumMap<DecompositionMethod, Long> decompositionNanosByMethod = new EnumMap<>(DecompositionMethod.class);
        Set<DecompositionMethod> enabledMethods = supportedMethods();
        for (DecompositionMethod method : DecompositionMethod.values()) {
            if (!enabledMethods.contains(method)) {
                continue;
            }
            MethodSelection planned = planMethod(cq, method, coverLimit, k, decompositionTimeoutMs);
            decompositionNanosByMethod.put(method, planned.decomposeNanos());
            if (planned.timedOut()) {
                timedOutMethods.add(method);
            }
            results.addAll(planned.candidates());
        }

        return new Selection(
                List.copyOf(results),
                Set.copyOf(timedOutMethods),
                Map.copyOf(decompositionNanosByMethod));
    }

    /**
     * Plans candidates for exactly one decomposition method. This keeps method
     * accounting isolated for compare-style benchmarks without changing the
     * shared multi-method planning path used elsewhere.
     */
    public MethodSelection planMethod(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(method, "method");
        validatePlanArguments(coverLimit, k, decompositionTimeoutMs);
        if (!supportedMethods().contains(method)) {
            throw new IllegalArgumentException("Unsupported decomposition method: " + method.id());
        }

        return switch (method) {
            case SINGLE_EDGE -> planSingleEdge(cq);
            case COST -> planCost(cq, coverLimit, k, decompositionTimeoutMs);
            case COST_OVERLAP -> planCostOverlap(cq, coverLimit, k, decompositionTimeoutMs);
            case DIAMETER -> planDiameter(cq, coverLimit, k, decompositionTimeoutMs);
            case SERIES_PARALLEL -> planSeriesParallel(cq, coverLimit, decompositionTimeoutMs);
            case SERIES_PARALLEL_OVERLAP -> planSeriesParallelOverlap(cq, decompositionTimeoutMs);
        };
    }

    public PreparedPlan prepare(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        Selection selection = planAll(cq, coverLimit, k, decompositionTimeoutMs);
        if (selection.candidates().isEmpty()) {
            return null;
        }
        return prepare(selection.candidates().get(0));
    }

    public PreparedPlan prepare(Candidate candidate) {
        Objects.requireNonNull(candidate, "candidate");
        return new PreparedPlan(
                candidate,
                ExecutablePlan.compile(candidate.plan(), index));
    }

    private void validatePlanArguments(int coverLimit, int k, int decompositionTimeoutMs) {
        if (coverLimit < 0) {
            throw new IllegalArgumentException("coverLimit must be >= 0");
        }
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }
        if (k > index.k()) {
            throw new IllegalArgumentException("k must be <= index k");
        }
        if (decompositionTimeoutMs < 0) {
            throw new IllegalArgumentException("decompositionTimeoutMs must be >= 0");
        }
    }

    private MethodSelection planSingleEdge(ConjunctiveQuery cq) {
        long start = System.nanoTime();
        Plan single = cq.decomposeSingleEdge();
        long decomposeNanos = System.nanoTime() - start;
        return methodSelectionSingle(DecompositionMethod.SINGLE_EDGE, single, decomposeNanos);
    }

    private MethodSelection planCost(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        long deadline = computeDeadlineNanos(decompositionTimeoutMs);
        Decomposer cost = Decomposer.cpqkCoverCost(k, coverLimit, index::cost, index::supports, deadline);
        TimedDecompositions decompositions = collectWithTimeoutGuard(() -> cost.decompose(cq.syntax()));
        return methodSelectionMany(
                DecompositionMethod.COST,
                decompositions.decompositions(),
                decompositions.nanos(),
                decompositions.timedOut());
    }

    private MethodSelection planCostOverlap(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        long deadline = computeDeadlineNanos(decompositionTimeoutMs);
        Decomposer cost = Decomposer.cpqkCoverCost(k, coverLimit, index::cost, index::supports, deadline);
        TimedDecompositions decompositions = collectWithTimeoutGuard(() -> cost.decompose(cq.syntax()));
        long overlapStart = System.nanoTime();
        List<Plan> ranked = rankPlansByOverlapScore(decompositions.decompositions(), coverLimit);
        long decomposeNanos = decompositions.nanos() + (System.nanoTime() - overlapStart);
        return methodSelectionMany(
                DecompositionMethod.COST_OVERLAP,
                ranked,
                decomposeNanos,
                decompositions.timedOut());
    }

    private MethodSelection planDiameter(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs) {
        long deadline = computeDeadlineNanos(decompositionTimeoutMs);
        Decomposer diameter = Decomposer.cpqkCoverDiameter(k, coverLimit, index::supports, deadline);
        TimedDecompositions decompositions = collectWithTimeoutGuard(() -> diameter.decompose(cq.syntax()));
        return methodSelectionMany(
                DecompositionMethod.DIAMETER,
                decompositions.decompositions(),
                decompositions.nanos(),
                decompositions.timedOut());
    }

    private MethodSelection planSeriesParallel(
            ConjunctiveQuery cq,
            int coverLimit,
            int decompositionTimeoutMs) {
        int maxPlans = coverLimit == 0 ? Integer.MAX_VALUE : Math.max(1, coverLimit);
        int restarts = computeSeriesParallelRestarts(maxPlans);
        long deadline = computeDeadlineNanos(decompositionTimeoutMs);
        TimedDecompositions decompositions = collectWithTimeoutGuard(
                () -> Decomposer.seriesParallelCandidates(
                        restarts,
                        maxPlans,
                        seriesParallelSeed,
                        index::supports,
                        deadline)
                        .decompose(cq.syntax()));
        return methodSelectionMany(
                DecompositionMethod.SERIES_PARALLEL,
                decompositions.decompositions(),
                decompositions.nanos(),
                decompositions.timedOut());
    }

    private MethodSelection planSeriesParallelOverlap(
            ConjunctiveQuery cq,
            int decompositionTimeoutMs) {
        long deadline = computeDeadlineNanos(decompositionTimeoutMs);
        TimedDecompositions decompositions = collectWithTimeoutGuard(
                () -> Decomposer.seriesParallelOverlapGuided(
                        index::cost,
                        index::overlapJoinScore,
                        index::supports,
                        deadline)
                        .decompose(cq.syntax()));
        return methodSelectionMany(
                DecompositionMethod.SERIES_PARALLEL_OVERLAP,
                decompositions.decompositions(),
                decompositions.nanos(),
                decompositions.timedOut());
    }

    private MethodSelection methodSelectionSingle(
            DecompositionMethod method,
            Plan plan,
            long decomposeNanos) {
        List<Candidate> results = new ArrayList<>(1);
        addCandidate(results, method, plan, decomposeNanos);
        return new MethodSelection(List.copyOf(results), false, decomposeNanos);
    }

    private MethodSelection methodSelectionMany(
            DecompositionMethod method,
            List<Plan> plans,
            long decomposeNanos,
            boolean timedOut) {
        List<Candidate> results = new ArrayList<>(plans.size());
        addAllCandidates(results, method, plans, decomposeNanos);
        return new MethodSelection(List.copyOf(results), timedOut, decomposeNanos);
    }

    private Plan ensureIndexable(Plan plan) {
        if (isIndexable(plan)) {
            return plan;
        }
        throw new IllegalArgumentException("No index covers labels for the query components");
    }

    private boolean isIndexable(Plan plan) {
        for (Component component : plan.components()) {
            if (!index.supports(component.cpq())) {
                return false;
            }
        }
        return true;
    }

    private void addCandidate(
            List<Candidate> results,
            DecompositionMethod method,
            Plan plan,
            long decomposeNanos) {
        if (isIndexable(plan)) {
            results.add(new Candidate(method, 0, plan, decomposeNanos));
        }
    }

    private void addAllCandidates(
            List<Candidate> results,
            DecompositionMethod method,
            List<Plan> plans,
            long decomposeNanos) {
        int ordinal = 0;
        for (Plan plan : plans) {
            if (isIndexable(plan)) {
                results.add(new Candidate(method, ordinal++, plan, decomposeNanos));
            }
        }
    }

    private List<Plan> rankPlansByOverlapScore(List<Plan> plans, int limit) {
        if (plans.isEmpty()) {
            return List.of();
        }
        int max = limit == 0 ? Integer.MAX_VALUE : Math.max(1, limit);
        List<ScoredPlan> scored = new ArrayList<>(plans.size());
        for (Plan plan : plans) {
            scored.add(new ScoredPlan(
                    plan,
                    index.overlapScore(plan),
                    plan.components().size(),
                    plan.maxDiameter(),
                    planSignature(plan)));
        }
        scored.sort((left, right) -> {
            int cmp = Double.compare(left.score(), right.score());
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(left.components(), right.components());
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(left.maxDiameter(), right.maxDiameter());
            if (cmp != 0) {
                return cmp;
            }
            return left.signature().compareTo(right.signature());
        });

        int resultSize = Math.min(max, scored.size());
        List<Plan> ranked = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            ranked.add(scored.get(i).plan());
        }
        return ranked;
    }

    private static String planSignature(Plan plan) {
        List<String> signatures = new ArrayList<>(plan.components().size());
        for (Component component : plan.components()) {
            signatures.add(component.signature());
        }
        signatures.sort(String::compareTo);
        return String.join("|", signatures);
    }

    private TimedDecompositions collect(java.util.stream.Stream<Plan> stream) {
        long start = System.nanoTime();
        List<Plan> list = stream.toList();
        return new TimedDecompositions(list, System.nanoTime() - start, false);
    }

    private TimedDecompositions collectWithTimeoutGuard(
            Supplier<java.util.stream.Stream<Plan>> streamSupplier) {
        long start = System.nanoTime();
        try {
            return collect(streamSupplier.get());
        } catch (DecompositionTimeoutException ex) {
            return TimedDecompositions.timeoutResult(System.nanoTime() - start);
        }
    }

    private static long computeDeadlineNanos(int timeoutMs) {
        if (timeoutMs <= 0) {
            return Long.MAX_VALUE;
        }
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        long now = System.nanoTime();
        long deadline = now + timeoutNanos;
        if (deadline < 0L) {
            return Long.MAX_VALUE;
        }
        return deadline;
    }

    private static int computeSeriesParallelRestarts(int maxPlans) {
        if (maxPlans >= Integer.MAX_VALUE / EXPERIMENTAL_SERIES_PARALLEL_RESTARTS_PER_PLAN) {
            return Integer.MAX_VALUE;
        }
        return Math.max(1, maxPlans) * EXPERIMENTAL_SERIES_PARALLEL_RESTARTS_PER_PLAN;
    }

    private static int positiveIntProperty(String name, int fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            return parsed > 0 ? parsed : fallback;
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static Set<DecompositionMethod> parseMethodAllowlistProperty(String propertyName) {
        String raw = System.getProperty(propertyName);
        if (raw == null || raw.isBlank()) {
            return Set.of();
        }
        Set<DecompositionMethod> methods = new LinkedHashSet<>();
        for (String part : raw.split(",")) {
            String token = part.trim();
            if (token.isEmpty()) {
                continue;
            }
            methods.add(parseMethodToken(propertyName, token));
        }
        return Set.copyOf(methods);
    }

    private static DecompositionMethod parseMethodToken(String propertyName, String token) {
        String normalized = token.trim();
        for (DecompositionMethod method : DecompositionMethod.values()) {
            if (method.id().equalsIgnoreCase(normalized) || method.name().equalsIgnoreCase(normalized)) {
                return method;
            }
        }
        throw new IllegalArgumentException(
                "Unknown decomposition method '" + token + "' in system property " + propertyName);
    }

    private static List<String> methodNames(Set<DecompositionMethod> methods) {
        List<String> names = new ArrayList<>(methods.size());
        for (DecompositionMethod method : methods) {
            names.add(method.id());
        }
        return names;
    }

    public record Candidate(DecompositionMethod method, int ordinal, Plan plan, long decomposeNanos) {
    }

    public record Selection(
            List<Candidate> candidates,
            Set<DecompositionMethod> timedOutMethods,
            Map<DecompositionMethod, Long> decompositionNanosByMethod) {
    }

    public record MethodSelection(
            List<Candidate> candidates,
            boolean timedOut,
            long decomposeNanos) {
    }

    public record PreparedPlan(
            Candidate candidate,
            ExecutablePlan executable) {
        public Plan plan() {
            return candidate.plan();
        }
    }

    private record ScoredPlan(
            Plan plan,
            double score,
            int components,
            int maxDiameter,
            String signature) {
    }

    private record TimedDecompositions(List<Plan> decompositions, long nanos, boolean timedOut) {
        private static TimedDecompositions timeoutResult(long nanos) {
            return new TimedDecompositions(List.of(), nanos, true);
        }
    }
}
