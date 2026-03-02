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
    private final boolean enableTw2CostDp;
    private final Set<DecompositionMethod> methodAllowlist;
    private final long seriesParallelSeed;

    public Planner(CpqIndex index) {
        this(index, EXPERIMENTAL_SERIES_PARALLEL_DEFAULT_SEED);
    }

    public Planner(CpqIndex index, long seriesParallelSeed) {
        this.index = Objects.requireNonNull(index, "index");
        this.enableTw2CostDp = booleanProperty("cpq.decompose.experimental.tw2CostDp", false);
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
                DecompositionMethod.DIAMETER,
                DecompositionMethod.SERIES_PARALLEL);
        if (enableTw2CostDp) {
            methods.add(DecompositionMethod.TW2_COST_DP);
        }
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

        List<Candidate> results = new ArrayList<>();
        EnumSet<DecompositionMethod> timedOutMethods = EnumSet.noneOf(DecompositionMethod.class);
        EnumMap<DecompositionMethod, Long> decompositionNanosByMethod = new EnumMap<>(DecompositionMethod.class);

        long singleStart = System.nanoTime();
        Plan single = cq.decomposeSingleEdge();
        long singleNanos = System.nanoTime() - singleStart;
        decompositionNanosByMethod.put(DecompositionMethod.SINGLE_EDGE, singleNanos);
        addCandidate(results, DecompositionMethod.SINGLE_EDGE, single, singleNanos);

        long costDeadline = computeDeadlineNanos(decompositionTimeoutMs);
        Decomposer cost = Decomposer.cpqkCoverCost(k, coverLimit, index::cost, index::supports, costDeadline);
        TimedDecompositions costDecomps = collectWithTimeoutGuard(() -> cost.decompose(cq.syntax()));
        decompositionNanosByMethod.put(DecompositionMethod.COST, costDecomps.nanos());
        if (costDecomps.timedOut()) {
            timedOutMethods.add(DecompositionMethod.COST);
        }
        addAllCandidates(results, DecompositionMethod.COST, costDecomps.decompositions(), costDecomps.nanos());

        long diameterDeadline = computeDeadlineNanos(decompositionTimeoutMs);
        Decomposer diameter = Decomposer.cpqkCoverDiameter(k, coverLimit, index::supports, diameterDeadline);
        TimedDecompositions diameterDecomps = collectWithTimeoutGuard(() -> diameter.decompose(cq.syntax()));
        decompositionNanosByMethod.put(DecompositionMethod.DIAMETER, diameterDecomps.nanos());
        if (diameterDecomps.timedOut()) {
            timedOutMethods.add(DecompositionMethod.DIAMETER);
        }
        addAllCandidates(results, DecompositionMethod.DIAMETER, diameterDecomps.decompositions(), diameterDecomps.nanos());

        if (enableTw2CostDp) {
            long tw2Deadline = computeDeadlineNanos(decompositionTimeoutMs);
            Decomposer tw2CostDp = Decomposer.tw2CostDp(k, index::cost, index::supports, tw2Deadline);
            TimedDecompositions tw2Dp = collectWithTimeoutGuard(() -> tw2CostDp.decompose(cq.syntax()));
            decompositionNanosByMethod.put(DecompositionMethod.TW2_COST_DP, tw2Dp.nanos());
            if (tw2Dp.timedOut()) {
                timedOutMethods.add(DecompositionMethod.TW2_COST_DP);
            }
            addAllCandidates(results, DecompositionMethod.TW2_COST_DP, tw2Dp.decompositions(), tw2Dp.nanos());
        }

        int maxPlans = Math.max(1, coverLimit);
        int restarts = computeSeriesParallelRestarts(maxPlans);
        long seriesParallelDeadline = computeDeadlineNanos(decompositionTimeoutMs);
        TimedDecompositions seriesParallel = collectWithTimeoutGuard(
                () -> Decomposer.seriesParallelCandidates(
                                restarts,
                                maxPlans,
                                seriesParallelSeed,
                                index::supports,
                                seriesParallelDeadline)
                        .decompose(cq.syntax()));
        decompositionNanosByMethod.put(DecompositionMethod.SERIES_PARALLEL, seriesParallel.nanos());
        if (seriesParallel.timedOut()) {
            timedOutMethods.add(DecompositionMethod.SERIES_PARALLEL);
        }
        addAllCandidates(results, DecompositionMethod.SERIES_PARALLEL, seriesParallel.decompositions(), seriesParallel.nanos());

        return new Selection(
                List.copyOf(results),
                Set.copyOf(timedOutMethods),
                Map.copyOf(decompositionNanosByMethod));
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

    private static boolean booleanProperty(String name, boolean fallback) {
        String raw = System.getProperty(name);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String normalized = raw.trim();
        if ("true".equalsIgnoreCase(normalized) || "1".equals(normalized)) {
            return true;
        }
        if ("false".equalsIgnoreCase(normalized) || "0".equals(normalized)) {
            return false;
        }
        return fallback;
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

    public record PreparedPlan(
            Candidate candidate,
            ExecutablePlan executable) {
        public Plan plan() {
            return candidate.plan();
        }
    }

    private record TimedDecompositions(List<Plan> decompositions, long nanos, boolean timedOut) {
        private static TimedDecompositions timeoutResult(long nanos) {
            return new TimedDecompositions(List.of(), nanos, true);
        }
    }
}
