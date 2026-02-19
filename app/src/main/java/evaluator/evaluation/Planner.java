package evaluator.evaluation;

import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.decomposition.Decomposer;
import evaluator.decomposition.Decomposer.DecompositionTimeoutException;
import evaluator.index.CpqIndex;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Planner that enumerates decomposition candidates from supported methods.
 */
public final class Planner {
    private final CpqIndex index;

    public Planner(CpqIndex index) {
        this.index = Objects.requireNonNull(index, "index");
    }

    public Plan decompose(ConjunctiveQuery cq) {
        Objects.requireNonNull(cq, "cq");
        return ensureIndexable(cq.decomposeSingleEdge());
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
        EnumSet<Method> timedOutMethods = EnumSet.noneOf(Method.class);

        long singleStart = System.nanoTime();
        Plan single = cq.decomposeSingleEdge();
        addCandidate(results, Method.SINGLE_EDGE, single, System.nanoTime() - singleStart);

        long costDeadline = computeDeadlineNanos(decompositionTimeoutMs);
        Decomposer cost = Decomposer.cpqkCoverCost(k, coverLimit, index::cost, index::supports, costDeadline);
        TimedDecompositions costDecomps = collectWithTimeoutGuard(() -> cost.decompose(cq.syntax()));
        if (costDecomps.timedOut()) {
            timedOutMethods.add(Method.COST);
        }
        addAllCandidates(results, Method.COST, costDecomps.decompositions(), costDecomps.nanos());

        long diameterDeadline = computeDeadlineNanos(decompositionTimeoutMs);
        Decomposer diameter = Decomposer.cpqkCoverDiameter(k, coverLimit, index::supports, diameterDeadline);
        TimedDecompositions diameterDecomps = collectWithTimeoutGuard(() -> diameter.decompose(cq.syntax()));
        if (diameterDecomps.timedOut()) {
            timedOutMethods.add(Method.DIAMETER);
        }
        addAllCandidates(results, Method.DIAMETER, diameterDecomps.decompositions(), diameterDecomps.nanos());

        TimedDecompositions spqr = collect(Decomposer.spqrGreedy(index::supports).decompose(cq.syntax()));
        addAllCandidates(results, Method.SPQR, spqr.decompositions(), spqr.nanos());

        TimedDecompositions seriesParallel = collect(Decomposer.seriesParallelGreedy(index::supports).decompose(cq.syntax()));
        addAllCandidates(results, Method.SERIES_PARALLEL, seriesParallel.decompositions(), seriesParallel.nanos());

        return new Selection(List.copyOf(results), Set.copyOf(timedOutMethods));
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
            Method method,
            Plan plan,
            long decomposeNanos) {
        if (isIndexable(plan)) {
            results.add(new Candidate(method, 0, plan, decomposeNanos));
        }
    }

    private void addAllCandidates(
            List<Candidate> results,
            Method method,
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
        try {
            return collect(streamSupplier.get());
        } catch (DecompositionTimeoutException ex) {
            return TimedDecompositions.timeoutResult();
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

    public enum Method {
        SINGLE_EDGE("single_edge"),
        COST("cost"),
        DIAMETER("diameter"),
        SPQR("spqr"),
        SERIES_PARALLEL("series_parallel");

        private final String id;

        Method(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }
    }

    public record Candidate(Method method, int ordinal, Plan plan, long decomposeNanos) {
    }

    public record Selection(List<Candidate> candidates, Set<Method> timedOutMethods) {
    }

    public record PreparedPlan(
            Candidate candidate,
            ExecutablePlan executable) {
        public Plan plan() {
            return candidate.plan();
        }
    }

    private record TimedDecompositions(List<Plan> decompositions, long nanos, boolean timedOut) {
        private static TimedDecompositions timeoutResult() {
            return new TimedDecompositions(List.of(), 0L, true);
        }
    }
}
