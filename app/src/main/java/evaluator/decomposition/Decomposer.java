package evaluator.decomposition;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import java.util.ArrayList;
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
        ExhaustiveComponentEnumerator enumerator = new ExhaustiveComponentEnumerator(k, deadlineNanos);
        CoverSelector selector = new CoverSelector(limit, CoverSelector.Order.COST, costFn, deadlineNanos);
        return cq -> {
            Objects.requireNonNull(cq, "cq");
            ConjunctiveQuery query = ConjunctiveQuery.from(cq);
            List<Component> components = enumerator.enumerate(query);
            if (componentFilter != null) {
                components = filterComponents(components, componentFilter);
            }
            return selector.select(query, components).sequential();
        };
    }

    static Decomposer cpqkCoverDiameter(int k, int limit) {
        return cpqkCoverDiameter(k, limit, null);
    }

    static Decomposer cpqkCoverDiameter(int k, int limit, java.util.function.Predicate<CPQ> componentFilter) {
        return cpqkCoverDiameter(k, limit, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer cpqkCoverDiameter(int k, int limit, java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        if (k < 0) {
            throw new IllegalArgumentException("k must be >= 0");
        }
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        ExhaustiveComponentEnumerator enumerator = new ExhaustiveComponentEnumerator(k, deadlineNanos);
        CoverSelector selector = new CoverSelector(limit, CoverSelector.Order.DIAMETER, null, deadlineNanos);
        return cq -> {
            Objects.requireNonNull(cq, "cq");
            ConjunctiveQuery query = ConjunctiveQuery.from(cq);
            List<Component> components = enumerator.enumerate(query);
            if (componentFilter != null) {
                components = filterComponents(components, componentFilter);
            }
            return selector.select(query, components).sequential();
        };
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
     * Treewidth-2 aware dynamic programming decomposer.
     * Enumerates CPQk components, then minimizes a System-R style join-cost
     * estimate over exact edge-disjoint covers.
     */
    static Decomposer tw2CostDp(
            int k,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter) {
        return tw2CostDp(k, costFn, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer tw2CostDp(
            int k,
            ToLongFunction<CPQ> costFn,
            java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        if (k < 1) {
            throw new IllegalArgumentException("k must be >= 1");
        }
        Objects.requireNonNull(costFn, "costFn");
        return cq -> Tw2CostDpDecomposer.decompose(cq, k, costFn, componentFilter, deadlineNanos);
    }

    /**
     * Generates multiple randomized series/parallel greedy decompositions over
     * the full CQ graph and returns the best unique plans by structural score.
     */
    static Decomposer seriesParallelCandidates(int restarts, int maxPlans, long seed) {
        return seriesParallelCandidates(restarts, maxPlans, seed, null);
    }

    static Decomposer seriesParallelCandidates(
            int restarts,
            int maxPlans,
            long seed,
            java.util.function.Predicate<CPQ> componentFilter) {
        return seriesParallelCandidates(restarts, maxPlans, seed, componentFilter, Long.MAX_VALUE);
    }

    static Decomposer seriesParallelCandidates(
            int restarts,
            int maxPlans,
            long seed,
            java.util.function.Predicate<CPQ> componentFilter,
            long deadlineNanos) {
        if (restarts < 1) {
            throw new IllegalArgumentException("restarts must be >= 1");
        }
        if (maxPlans < 1) {
            throw new IllegalArgumentException("maxPlans must be >= 1");
        }
        return cq -> SeriesParallelDecomposer.decomposeCandidates(
                cq,
                componentFilter,
                restarts,
                maxPlans,
                seed,
                deadlineNanos);
    }

    private static List<Component> filterComponents(List<Component> components,
            java.util.function.Predicate<CPQ> componentFilter) {
        List<Component> filtered = new ArrayList<>(components.size());
        for (Component component : components) {
            if (componentFilter.test(component.cpq())) {
                filtered.add(component);
            }
        }
        return filtered;
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
