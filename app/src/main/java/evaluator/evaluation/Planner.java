package evaluator.evaluation;

import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.decomposition.Decomposer;
import evaluator.decomposition.Decomposer.DecompositionTimeoutException;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
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
    private static final String METHODS_PROPERTY = "cpq.decompose.methods";
    private static final EnumSet<DecompositionMethod> DEFAULT_METHODS = EnumSet.allOf(DecompositionMethod.class);

    private final CpqIndex index;
    private final Set<DecompositionMethod> methodAllowlist;
    private final long seriesParallelSeed;
    private final SystemRScorer systemRScorer;

    public enum CandidateSelectionPolicy {
        METHOD_NATIVE,
        SYSTEM_R_SCORE
    }

    public Planner(CpqIndex index) {
        this(index, EXPERIMENTAL_SERIES_PARALLEL_DEFAULT_SEED, 0);
    }

    public Planner(CpqIndex index, long seriesParallelSeed) {
        this(index, seriesParallelSeed, 0);
    }

    public Planner(CpqIndex index, long seriesParallelSeed, int systemRMaxCandidateOrders) {
        this.index = Objects.requireNonNull(index, "index");
        this.methodAllowlist = parseMethodAllowlistProperty(METHODS_PROPERTY);
        this.seriesParallelSeed = seriesParallelSeed;
        this.systemRScorer = new SystemRScorer(index, systemRMaxCandidateOrders);
    }

    public Plan decompose(ConjunctiveQuery cq) {
        Objects.requireNonNull(cq, "cq");
        return ensureIndexable(cq.decomposeSingleEdge());
    }

    public Set<DecompositionMethod> supportedMethods() {
        EnumSet<DecompositionMethod> methods = methodAllowlist.isEmpty()
                ? EnumSet.copyOf(DEFAULT_METHODS)
                : EnumSet.copyOf(methodAllowlist);
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
        return planAll(cq, coverLimit, k, decompositionTimeoutMs, decompositionTimeoutMs);
    }

    /**
     * Plans all enabled methods using a decomposition budget for cover generation
     * and a separate budget for System R scoring.
     */
    public Selection planAll(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int systemRTimeoutMs) {
        Objects.requireNonNull(cq, "cq");
        validatePlanArguments(coverLimit, k);
        validateTimeoutMs(decompositionTimeoutMs, "decompositionTimeoutMs");
        validateTimeoutMs(systemRTimeoutMs, "systemRTimeoutMs");

        List<Candidate> results = new ArrayList<>();
        EnumSet<DecompositionMethod> timedOutMethods = EnumSet.noneOf(DecompositionMethod.class);
        EnumMap<DecompositionMethod, Long> decompositionNanosByMethod = new EnumMap<>(DecompositionMethod.class);
        Set<DecompositionMethod> enabledMethods = supportedMethods();
        long decompositionDeadlineNanos = computeDeadlineNanos(decompositionTimeoutMs);
        long systemRDeadlineNanos = computeDeadlineNanos(systemRTimeoutMs);
        for (DecompositionMethod method : DecompositionMethod.values()) {
            if (!enabledMethods.contains(method)) {
                continue;
            }
            MethodSelection planned = planMethod(
                    cq,
                    method,
                    coverLimit,
                    k,
                    decompositionDeadlineNanos,
                    systemRDeadlineNanos);
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
        return planMethod(cq, method, coverLimit, k, decompositionTimeoutMs, decompositionTimeoutMs);
    }

    /**
     * Plans one method using the decomposition budget for decomposition search
     * and a separate budget for System R scoring.
     */
    public MethodSelection planMethod(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int systemRTimeoutMs) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(method, "method");
        validatePlanArguments(coverLimit, k);
        validateTimeoutMs(decompositionTimeoutMs, "decompositionTimeoutMs");
        validateTimeoutMs(systemRTimeoutMs, "systemRTimeoutMs");
        return planMethod(
                cq,
                method,
                coverLimit,
                k,
                computeDeadlineNanos(decompositionTimeoutMs),
                computeDeadlineNanos(systemRTimeoutMs));
    }

    /**
     * Plans one method using absolute deadlines for decomposition search and
     * planner-side System R scoring.
     */
    public MethodSelection planMethod(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            int coverLimit,
            int k,
            long decompositionDeadlineNanos,
            long systemRDeadlineNanos) {
        Objects.requireNonNull(cq, "cq");
        Objects.requireNonNull(method, "method");
        validatePlanArguments(coverLimit, k);
        if (!supportedMethods().contains(method)) {
            throw new IllegalArgumentException("Unsupported decomposition method: " + method.id());
        }

        return switch (method) {
            case SINGLE_EDGE -> planSingleEdge(cq);
            case COST -> planCost(cq, coverLimit, k, decompositionDeadlineNanos);
            case MAX_COLLAPSE -> planMaxCollapse(cq, coverLimit, k, decompositionDeadlineNanos);
            case SERIES_PARALLEL -> planSeriesParallel(cq, coverLimit, decompositionDeadlineNanos);
            case SINGLE_EDGE_SYSTEM_R -> planSingleEdgeSystemR(cq);
            case EXHAUSTIVE_LEAF_COST -> planExhaustiveLeafCost(
                    cq,
                    coverLimit,
                    k,
                    decompositionDeadlineNanos);
            case EXHAUSTIVE_LEAF_SYSTEM_R -> planExhaustiveLeafSystemR(
                    cq,
                    coverLimit,
                    k,
                    decompositionDeadlineNanos,
                    systemRDeadlineNanos);
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

    /**
     * Selects one candidate per enabled method using either native method order
     * or a shared System R reranking step.
     */
    public SelectedSelection selectBestCandidates(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int systemRTimeoutMs,
            CandidateSelectionPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        Selection planned = planAll(cq, coverLimit, k, decompositionTimeoutMs, systemRTimeoutMs);
        return selectBestCandidates(planned, policy, computeDeadlineNanos(systemRTimeoutMs));
    }

    /**
     * Selects one candidate per enabled method from an already generated plan
     * family using either native method order or a shared System R reranking step.
     */
    public SelectedSelection selectBestCandidates(
            Selection planned,
            CandidateSelectionPolicy policy,
            long deadlineNanos) {
        Objects.requireNonNull(planned, "planned");
        Objects.requireNonNull(policy, "policy");
        List<SelectedCandidate> selected = selectBestPerMethod(
                planned.candidates(),
                policy,
                deadlineNanos);
        return new SelectedSelection(
                List.copyOf(selected),
                planned.timedOutMethods(),
                planned.decompositionNanosByMethod());
    }

    /**
     * Selects one candidate for the given method using either native method
     * order or a shared System R reranking step.
     */
    public SelectedMethodSelection selectBestCandidate(
            ConjunctiveQuery cq,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int systemRTimeoutMs,
            CandidateSelectionPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        MethodSelection planned = planMethod(cq, method, coverLimit, k, decompositionTimeoutMs, systemRTimeoutMs);
        return selectBestCandidate(planned, policy, computeDeadlineNanos(systemRTimeoutMs));
    }

    /**
     * Selects one candidate for an already generated method family using either
     * native method order or a shared System R reranking step.
     */
    public SelectedMethodSelection selectBestCandidate(
            MethodSelection planned,
            CandidateSelectionPolicy policy,
            long deadlineNanos) {
        Objects.requireNonNull(planned, "planned");
        Objects.requireNonNull(policy, "policy");
        SelectedCandidate selected = selectBestCandidate(
                planned.candidates(),
                policy,
                deadlineNanos);
        return new SelectedMethodSelection(selected, planned.timedOut(), planned.decomposeNanos());
    }

    /**
     * Chooses the variable order for an executable plan. System R plans always
     * use the scorer; heuristic plans can optionally enable the same scorer for
     * execution-time order estimation.
     */
    public JoinOrderPlan selectJoinOrder(
            ExecutablePlan executable,
            boolean estimateHeuristicPlans,
            long deadlineNanos) {
        Objects.requireNonNull(executable, "executable");
        if (executable.plan().orderPolicy() == Plan.OrderPolicy.SYSTEM_R || estimateHeuristicPlans) {
            long estimateStart = System.nanoTime();
            SystemRScorer.OrderSelection selection = systemRScorer.selectBestOrder(executable.plan(), deadlineNanos);
            return new JoinOrderPlan(
                    selection.order(),
                    selection.estimatedCount(),
                    0.0D,
                    System.nanoTime() - estimateStart);
        }
        return new JoinOrderPlan(
                heuristicOrder(executable.plan().components(), executable.componentCounts()),
                Double.NaN,
                Double.NaN,
                0L);
    }

    /**
     * Exposes the shared planner-side projected-count estimate used by BenchEngine.
     */
    public ProjectedCountEstimate estimateProjectedCount(
            Plan plan,
            List<String> variableOrder,
            List<String> projected,
            long deadlineNanos) {
        return systemRScorer.estimateProjectedCount(plan, variableOrder, projected, deadlineNanos);
    }

    private void validatePlanArguments(int coverLimit, int k) {
        if (coverLimit < 0) {
            throw new IllegalArgumentException("coverLimit must be >= 0");
        }
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }
        if (k > index.k()) {
            throw new IllegalArgumentException("k must be <= index k");
        }
    }

    private static void validateTimeoutMs(int timeoutMs, String name) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private MethodSelection planSingleEdge(ConjunctiveQuery cq) {
        long start = System.nanoTime();
        Plan single = cq.decomposeSingleEdge();
        long decomposeNanos = System.nanoTime() - start;
        return methodSelectionSingle(DecompositionMethod.SINGLE_EDGE, single, decomposeNanos);
    }

    private MethodSelection planSingleEdgeSystemR(ConjunctiveQuery cq) {
        long start = System.nanoTime();
        Plan single = cq.decomposeSingleEdge().withOrderPolicy(Plan.OrderPolicy.SYSTEM_R);
        long decomposeNanos = System.nanoTime() - start;
        return methodSelectionSingle(DecompositionMethod.SINGLE_EDGE_SYSTEM_R, single, decomposeNanos);
    }

    private MethodSelection planCost(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            long decompositionDeadlineNanos) {
        long deadline = decompositionDeadlineNanos;
        Decomposer cost = Decomposer.cpqkCoverCost(k, coverLimit, index::cost, index::supports, deadline);
        TimedDecompositions decompositions = collectWithTimeoutGuard(() -> cost.decompose(cq.syntax()));
        return methodSelectionMany(
                DecompositionMethod.COST,
                decompositions.decompositions(),
                decompositions.nanos(),
                decompositions.timedOut());
    }

    private MethodSelection planExhaustiveLeafCost(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            long decompositionDeadline) {
        Decomposer exhaustive = Decomposer.cpqkCoverTerminalLeaves(
                k,
                coverLimit,
                index::cost,
                index::supports,
                decompositionDeadline);
        TimedDecompositions decompositions = collectWithTimeoutGuard(() -> exhaustive.decompose(cq.syntax()));
        return selectSingleCostMethod(
                DecompositionMethod.EXHAUSTIVE_LEAF_COST,
                decompositions);
    }

    private MethodSelection planExhaustiveLeafSystemR(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            long decompositionDeadline,
            long systemRDeadlineNanos) {
        Decomposer exhaustive = Decomposer.cpqkCoverTerminalLeaves(
                k,
                coverLimit,
                cpq -> 0L,
                index::supports,
                decompositionDeadline);
        TimedDecompositions decompositions = collectWithTimeoutGuard(() -> exhaustive.decompose(cq.syntax()));
        return selectSingleSystemRMethod(
                DecompositionMethod.EXHAUSTIVE_LEAF_SYSTEM_R,
                decompositions,
                systemRDeadlineNanos);
    }

    private MethodSelection planMaxCollapse(
            ConjunctiveQuery cq,
            int coverLimit,
            int k,
            long decompositionDeadlineNanos) {
        long deadline = decompositionDeadlineNanos;
        Decomposer maxCollapse = Decomposer.cpqkCoverMaxCollapse(
                k,
                coverLimit,
                index::cost,
                index::supports,
                deadline);
        TimedDecompositions decompositions = collectWithTimeoutGuard(() -> maxCollapse.decompose(cq.syntax()));
        return methodSelectionMany(
                DecompositionMethod.MAX_COLLAPSE,
                decompositions.decompositions(),
                decompositions.nanos(),
                decompositions.timedOut());
    }

    private MethodSelection planSeriesParallel(
            ConjunctiveQuery cq,
            int coverLimit,
            long decompositionDeadlineNanos) {
        long deadline = decompositionDeadlineNanos;
        TimedDecompositions decompositions = collectWithTimeoutGuard(
                () -> Decomposer.seriesParallelCandidates(
                        coverLimit,
                        seriesParallelSeed,
                        index::cost,
                        index::supports,
                        deadline)
                        .decompose(cq.syntax()));
        return methodSelectionMany(
                DecompositionMethod.SERIES_PARALLEL,
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

    private MethodSelection selectSingleSystemRMethod(
            DecompositionMethod method,
            TimedDecompositions decompositions,
            long deadlineNanos) {
        if (decompositions.decompositions().isEmpty()) {
            return methodSelectionMany(method, List.of(), decompositions.nanos(), decompositions.timedOut());
        }
        long scoreStart = System.nanoTime();
        SystemRScorer.PlanSelection selected = systemRScorer.selectBestPlan(
                decompositions.decompositions(),
                deadlineNanos);
        long decomposeNanos = decompositions.nanos() + (System.nanoTime() - scoreStart);
        return methodSelectionSingle(method, selected.plan(), decomposeNanos);
    }

    private List<SelectedCandidate> selectBestPerMethod(
            List<Candidate> candidates,
            CandidateSelectionPolicy policy,
            long deadlineNanos) {
        EnumMap<DecompositionMethod, List<Candidate>> byMethod = new EnumMap<>(DecompositionMethod.class);
        for (Candidate candidate : candidates) {
            byMethod.computeIfAbsent(candidate.method(), ignored -> new ArrayList<>()).add(candidate);
        }
        List<SelectedCandidate> selected = new ArrayList<>(byMethod.size());
        for (List<Candidate> methodCandidates : byMethod.values()) {
            SelectedCandidate candidate = selectBestCandidate(methodCandidates, policy, deadlineNanos);
            if (candidate != null) {
                selected.add(candidate);
            }
        }
        return selected;
    }

    private SelectedCandidate selectBestCandidate(
            List<Candidate> candidates,
            CandidateSelectionPolicy policy,
            long deadlineNanos) {
        if (candidates.isEmpty()) {
            return null;
        }
        if (policy == CandidateSelectionPolicy.METHOD_NATIVE || candidates.size() == 1) {
            return new SelectedCandidate(candidates.get(0), 0L);
        }

        long estimateStart = System.nanoTime();
        Candidate best = candidates.get(0);
        double bestScore = estimateCandidate(best, deadlineNanos);
        for (int i = 1; i < candidates.size(); i++) {
            Candidate candidate = candidates.get(i);
            double score = estimateCandidate(candidate, deadlineNanos);
            if (score < bestScore || (score == bestScore && candidate.ordinal() < best.ordinal())) {
                best = candidate;
                bestScore = score;
            }
        }
        return new SelectedCandidate(best, System.nanoTime() - estimateStart);
    }

    private double estimateCandidate(Candidate candidate, long deadlineNanos) {
        Deadline.check(deadlineNanos);
        return systemRScorer.selectBestOrder(candidate.plan(), deadlineNanos).score();
    }

    private MethodSelection selectSingleCostMethod(
            DecompositionMethod method,
            TimedDecompositions decompositions) {
        if (decompositions.decompositions().isEmpty()) {
            return methodSelectionMany(method, List.of(), decompositions.nanos(), decompositions.timedOut());
        }
        long scoreStart = System.nanoTime();
        Plan selected = selectLowestCostPlan(decompositions.decompositions());
        long decomposeNanos = decompositions.nanos() + (System.nanoTime() - scoreStart);
        return methodSelectionSingle(method, selected, decomposeNanos);
    }

    private Plan selectLowestCostPlan(List<Plan> candidates) {
        Plan best = null;
        long bestCost = Long.MAX_VALUE;
        for (Plan candidate : candidates) {
            long cost = planCost(candidate);
            if (best == null || compareCostSelection(candidate, cost, best, bestCost) < 0) {
                best = candidate;
                bestCost = cost;
            }
        }
        return best;
    }

    private long planCost(Plan plan) {
        long total = 0L;
        for (Component component : plan.components()) {
            long cost = Math.max(0L, index.cost(component.cpq()));
            if (total >= Long.MAX_VALUE - cost) {
                return Long.MAX_VALUE;
            }
            total += cost;
        }
        return total;
    }

    private static int compareCostSelection(Plan left, long leftCost, Plan right, long rightCost) {
        int cmp = Long.compare(leftCost, rightCost);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(left.components().size(), right.components().size());
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(left.maxDiameter(), right.maxDiameter());
        if (cmp != 0) {
            return cmp;
        }
        return planSignature(left).compareTo(planSignature(right));
    }

    private static String planSignature(Plan plan) {
        List<String> signatures = new ArrayList<>(plan.components().size());
        for (Component component : plan.components()) {
            signatures.add(component.signature());
        }
        signatures.sort(String::compareTo);
        return String.join("|", signatures);
    }

    private static List<String> heuristicOrder(List<Component> components, List<Long> componentCounts) {
        Map<String, Long> minCountByVar = new java.util.HashMap<>();
        Map<String, Integer> participationByVar = new java.util.HashMap<>();
        for (int i = 0; i < components.size(); i++) {
            long count = componentCounts.get(i);
            Component component = components.get(i);
            String left = component.sourceVarName();
            String right = component.targetVarName();
            minCountByVar.merge(left, count, Math::min);
            participationByVar.merge(left, 1, Integer::sum);
            if (!left.equals(right)) {
                minCountByVar.merge(right, count, Math::min);
                participationByVar.merge(right, 1, Integer::sum);
            }
        }
        return minCountByVar.keySet().stream()
                .sorted(java.util.Comparator
                        .comparingInt((String variable) -> participationByVar.getOrDefault(variable, 0))
                        .reversed()
                        .thenComparingLong(variable -> minCountByVar.getOrDefault(variable, Long.MAX_VALUE))
                        .thenComparing(java.util.Comparator.naturalOrder()))
                .toList();
    }

    private TimedDecompositions collect(java.util.stream.Stream<Plan> stream, long startedNanos) {
        List<Plan> list = stream.toList();
        return new TimedDecompositions(list, System.nanoTime() - startedNanos, false);
    }

    private TimedDecompositions collectWithTimeoutGuard(
            Supplier<java.util.stream.Stream<Plan>> streamSupplier) {
        long start = System.nanoTime();
        try {
            return collect(streamSupplier.get(), start);
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
            if (method.matchesToken(normalized)) {
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

    public record SelectedCandidate(
            DecompositionMethod method,
            int ordinal,
            Plan plan,
            long decomposeNanos,
            long selectionEstimateNanos) {
        private SelectedCandidate(Candidate candidate, long selectionEstimateNanos) {
            this(
                    candidate.method(),
                    candidate.ordinal(),
                    candidate.plan(),
                    candidate.decomposeNanos(),
                    selectionEstimateNanos);
        }
    }

    public record Selection(
            List<Candidate> candidates,
            Set<DecompositionMethod> timedOutMethods,
            Map<DecompositionMethod, Long> decompositionNanosByMethod) {
    }

    public record SelectedSelection(
            List<SelectedCandidate> candidates,
            Set<DecompositionMethod> timedOutMethods,
            Map<DecompositionMethod, Long> decompositionNanosByMethod) {
    }

    public record MethodSelection(
            List<Candidate> candidates,
            boolean timedOut,
            long decomposeNanos) {
    }

    public record SelectedMethodSelection(
            SelectedCandidate candidate,
            boolean timedOut,
            long decomposeNanos) {
    }

    public record JoinOrderPlan(
            List<String> order,
            double estimatedCount,
            double estimateStdError,
            long estimateNanos) {
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
