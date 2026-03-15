package evaluator.decomposition;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import java.util.List;
import java.util.Objects;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

public interface Decomposer {
    Stream<Plan> decompose(CQ cq);

    static Decomposer cpqkCoverExhaustive(int k, int limit) {
        return cpqkCoverExhaustive(k, limit, (java.util.function.Predicate<CPQ>) null);
    }

    static Decomposer cpqkCoverExhaustive(int k, int limit, java.util.function.Predicate<CPQ> componentFilter) {
        return cpqkCoverExhaustive(k, limit, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer cpqkCoverExhaustive(int k, int limit, java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        return cpqkCoverCost(k, limit, cpq -> 0L, componentFilter, deadlineNanos);
    }

    static Decomposer cpqkCoverExhaustive(int k, int limit, ToLongFunction<CPQ> costFn) {
        return cpqkCoverCost(k, limit, costFn);
    }

    static Decomposer cpqkCoverExhaustive(int k, int limit, ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter) {
        return cpqkCoverCost(k, limit, costFn, componentFilter);
    }

    static Decomposer cpqkCoverCost(int k, int limit, ToLongFunction<CPQ> costFn) {
        return cpqkCoverCost(k, limit, costFn, null);
    }

    static Decomposer cpqkCoverCost(int k, int limit, ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter) {
        return cpqkCoverCost(k, limit, costFn, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer cpqkCoverCost(
            int k,
            int limit,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        if (k < 0) {
            throw new IllegalArgumentException("k must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        Objects.requireNonNull(costFn, "costFn");
        ExhaustiveComponentEnumerator enumerator = new ExhaustiveComponentEnumerator(k, componentFilter, deadlineNanos);
        CoverSelector selector = new CoverSelector(limit, CoverSelector.Order.COST, costFn, deadlineNanos);
        return cq -> {
            Objects.requireNonNull(cq, "cq");
            ConjunctiveQuery query = ConjunctiveQuery.from(cq);
            List<Component> components = enumerator.enumerate(query);
            return selector.select(query, components).sequential();
        };
    }

    /**
     * Exhaustive bounded-component cover search that only emits terminal
     * series/parallel leaf covers. Terminality is checked on completed exact
     * covers using the same free-variable-preserving reduction rules as the
     * legacy leaf search.
     */
    static Decomposer cpqkCoverTerminalLeaves(
            int k,
            int limit,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter) {
        return cpqkCoverTerminalLeaves(k, limit, costFn, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer cpqkCoverTerminalLeaves(
            int k,
            int limit,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        if (k < 0) {
            throw new IllegalArgumentException("k must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        Objects.requireNonNull(costFn, "costFn");
        java.util.function.Predicate<CPQ> mergedFilter = cpq -> cpq.getDiameter() <= k
                && (componentFilter == null || componentFilter.test(cpq));
        ExhaustiveComponentEnumerator enumerator = new ExhaustiveComponentEnumerator(k, mergedFilter, deadlineNanos);
        CoverSelector selector = new CoverSelector(
                limit,
                CoverSelector.Order.COST,
                costFn,
                plan -> TerminalLeafFilter.isTerminalLeaf(plan, mergedFilter),
                deadlineNanos);
        return cq -> {
            Objects.requireNonNull(cq, "cq");
            ConjunctiveQuery query = ConjunctiveQuery.from(cq);
            List<Component> components = enumerator.enumerate(query);
            return selector.select(query, components).sequential();
        };
    }

    /**
     * Exhaustive bounded-component cover search ranked by collapse-first structure:
     * fewer components first, with the provided count signal used only to break
     * ties among covers with the same number of components.
     */
    static Decomposer cpqkCoverMaxCollapse(int k, int limit, ToLongFunction<CPQ> costFn) {
        return cpqkCoverMaxCollapse(k, limit, costFn, null);
    }

    static Decomposer cpqkCoverMaxCollapse(
            int k,
            int limit,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter) {
        return cpqkCoverMaxCollapse(k, limit, costFn, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer cpqkCoverMaxCollapse(
            int k,
            int limit,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        if (k < 0) {
            throw new IllegalArgumentException("k must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        Objects.requireNonNull(costFn, "costFn");
        ExhaustiveComponentEnumerator enumerator = new ExhaustiveComponentEnumerator(k, componentFilter, deadlineNanos);
        CoverSelector selector = new CoverSelector(limit, CoverSelector.Order.MAX_COLLAPSE, costFn, deadlineNanos);
        return cq -> {
            Objects.requireNonNull(cq, "cq");
            ConjunctiveQuery query = ConjunctiveQuery.from(cq);
            List<Component> components = enumerator.enumerate(query);
            return selector.select(query, components).sequential();
        };
    }

    @Deprecated
    static Decomposer cpqkCoverDiameter(int k, int limit) {
        return cpqkCoverDiameter(k, limit, null);
    }

    @Deprecated
    static Decomposer cpqkCoverDiameter(int k, int limit, java.util.function.Predicate<CPQ> componentFilter) {
        return cpqkCoverDiameter(k, limit, componentFilter, Long.MAX_VALUE);
    }

    @Deprecated
    static Decomposer cpqkCoverDiameter(int k, int limit, java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        return cpqkCoverMaxCollapse(k, limit, cpq -> 0L, componentFilter, deadlineNanos);
    }

    @Deprecated
    static Decomposer cpqkCover(int k, int limit) {
        return cpqkCoverExhaustive(k, limit);
    }

    /**
     * Greedily applies series/parallel reductions over the entire CQ graph until
     * no more reductions apply. Only free variables are preserved as terminals.
     */
    static Decomposer seriesParallelGreedy() {
        return SeriesParallelDecomposer::decomposeGreedy;
    }

    static Decomposer seriesParallelGreedy(java.util.function.Predicate<CPQ> componentFilter) {
        Objects.requireNonNull(componentFilter, "componentFilter");
        return cq -> SeriesParallelDecomposer.decomposeGreedy(cq, componentFilter);
    }

    /**
     * Generates multiple randomized series/parallel greedy decompositions over
     * the full CQ graph and returns deduplicated plans ranked by total collapsed
     * edges, with cumulative component cost used as a tie-break. A zero restart
     * budget falls back to the single-edge decomposition.
     */
    static Decomposer seriesParallelCandidates(int restarts, long seed) {
        return seriesParallelCandidates(restarts, seed, cpq -> 0L, null);
    }

    static Decomposer seriesParallelCandidates(
            int restarts,
            long seed,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter) {
        return seriesParallelCandidates(restarts, seed, costFn, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer seriesParallelCandidates(
            int restarts,
            long seed,
            java.util.function.Predicate<CPQ> componentFilter) {
        return seriesParallelCandidates(restarts, seed, cpq -> 0L, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer seriesParallelCandidates(
            int restarts,
            long seed,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        if (restarts < 0) {
            throw new IllegalArgumentException("restarts must be >= 0");
        }
        Objects.requireNonNull(costFn, "costFn");
        return cq -> SeriesParallelDecomposer.decomposeCandidates(
                cq,
                componentFilter,
                costFn,
                restarts,
                seed,
                deadlineNanos);
    }

    /**
     * Signals that decomposition generation exceeded its configured time budget.
     */
    final class DecompositionTimeoutException extends RuntimeException {
        public DecompositionTimeoutException(String message) {
            super(message);
        }
    }
}
