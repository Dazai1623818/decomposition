package evaluator.bench;

import static evaluator.bench.BenchTypes.*;

import evaluator.cpq.Plan;
import evaluator.evaluation.ExecutablePlan;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.evaluation.Planner;
import evaluator.evaluation.ProjectedCountEstimate;
import evaluator.evaluation.Relation;
import evaluator.util.Deadline;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Deterministic estimation and profiling helpers used by explicit
 * estimator-facing workloads.
 */
final class EstimatorDiagnostics {
    private final Planner planner;
    private final EngineConfig config;

    EstimatorDiagnostics(Planner planner, EngineConfig config) {
        this.planner = Objects.requireNonNull(planner, "planner");
        this.config = Objects.requireNonNull(config, "config");
    }

    CardinalityEstimate estimateCount(ExecutablePlan executable, long deadlineNanos) {
        EvaluationStats stats = fromCompilationStats(executable.compilationStats());
        if (executable.isEmpty()) {
            return new CardinalityEstimate(0.0, stats.queryNanos(), stats.mappingNanos(), 0L);
        }
        Deadline.check(deadlineNanos);
        long estimateStart = System.nanoTime();
        Planner.JoinOrderPlan orderPlan = planner.selectJoinOrder(executable, false, deadlineNanos);
        ProjectedCountEstimate estimate = planner.estimateProjectedCount(
                executable.plan(),
                orderPlan.order(),
                executable.plan().projectedVariableNames(),
                deadlineNanos);
        long estimateNanos = System.nanoTime() - estimateStart;
        return new CardinalityEstimate(
                estimate.estimatedCount(),
                stats.queryNanos(),
                stats.mappingNanos(),
                estimateNanos);
    }

    OrderProfileSummary profileOrders(
            ExecutablePlan executable,
            int randomOrders,
            long seed,
            long deadlineNanos) {
        if (randomOrders < 0) {
            throw new IllegalArgumentException("randomOrders must be >= 0");
        }
        EvaluationStats stats = fromCompilationStats(executable.compilationStats());
        if (executable.isEmpty()) {
            return new OrderProfileSummary(stats.queryNanos(), stats.mappingNanos(), List.of());
        }
        Planner.JoinOrderPlan defaultOrder = planner.selectJoinOrder(executable, false, deadlineNanos);
        List<List<String>> orders = sampleOrders(defaultOrder.order(), randomOrders, seed);
        List<OrderProfile> profiles = new ArrayList<>(orders.size());
        for (List<String> order : orders) {
            Deadline.check(deadlineNanos);
            long start = System.nanoTime();
            LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                    order,
                    LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                    config.joinSafeDistinctFastPath(),
                    deadlineNanos);
            long joinNanos = System.nanoTime() - start;
            profiles.add(new OrderProfile(order, joinNanos, count.count()));
        }
        return new OrderProfileSummary(stats.queryNanos(), stats.mappingNanos(), profiles);
    }

    List<PrefixEstimationStep> tracePrefixEstimationSteps(
            ExecutablePlan executable,
            List<String> variableOrder,
            long deadlineNanos) {
        if (variableOrder.isEmpty() || executable.isEmpty()) {
            return List.of();
        }
        List<Relation> relations = executable.relations();
        List<ProjectedCountEstimate.PrefixEstimate> prefixEstimates = estimatePrefixProjectedCounts(
                executable.plan(),
                variableOrder,
                deadlineNanos);
        List<PrefixEstimationStep> steps = new ArrayList<>(variableOrder.size());
        long cumulativeEstimateNanos = 0L;
        long cumulativeEvalNanos = 0L;
        long cumulativePrefixNanos = 0L;
        double cumulativeQ = 0.0D;
        double cumulativeRelative = 0.0D;
        for (int i = 0; i < variableOrder.size(); i++) {
            Deadline.check(deadlineNanos);
            List<String> prefix = List.copyOf(variableOrder.subList(0, i + 1));
            ProjectedCountEstimate.PrefixEstimate prefixEstimate = prefixEstimates.get(i);
            long estimateNanos = prefixEstimate.estimateNanos();
            long actualStart = System.nanoTime();
            long actual = evalProjectedCount(relations, variableOrder, prefix, deadlineNanos);
            long evalNanos = System.nanoTime() - actualStart;
            long prefixNanos = estimateNanos + evalNanos;
            cumulativeEstimateNanos += estimateNanos;
            cumulativeEvalNanos += evalNanos;
            cumulativePrefixNanos += prefixNanos;
            double qError = qError(prefixEstimate.estimatedCount(), actual);
            double relativeError = relativeError(prefixEstimate.estimatedCount(), actual);
            cumulativeQ += qError;
            cumulativeRelative += relativeError;
            double stepCount = i + 1.0D;
            steps.add(new PrefixEstimationStep(
                    i + 1,
                    variableOrder.get(i),
                    prefix,
                    prefixEstimate.estimatedCount(),
                    actual,
                    estimateNanos,
                    evalNanos,
                    prefixNanos,
                    cumulativeEstimateNanos,
                    cumulativeEvalNanos,
                    cumulativePrefixNanos,
                    qError,
                    relativeError,
                    cumulativeQ / stepCount,
                    cumulativeRelative / stepCount));
        }
        return List.copyOf(steps);
    }

    private List<ProjectedCountEstimate.PrefixEstimate> estimatePrefixProjectedCounts(
            Plan decomposition,
            List<String> variableOrder,
            long deadlineNanos) {
        List<ProjectedCountEstimate.PrefixEstimate> estimates = new ArrayList<>(variableOrder.size());
        for (int i = 0; i < variableOrder.size(); i++) {
            Deadline.check(deadlineNanos);
            List<String> prefix = List.copyOf(variableOrder.subList(0, i + 1));
            long estimateStart = System.nanoTime();
            ProjectedCountEstimate estimate = planner.estimateProjectedCount(
                    decomposition,
                    variableOrder,
                    prefix,
                    deadlineNanos);
            long estimateNanos = System.nanoTime() - estimateStart;
            estimates.add(new ProjectedCountEstimate.PrefixEstimate(
                    estimate.estimatedCount(),
                    estimateNanos));
        }
        return List.copyOf(estimates);
    }

    private long evalProjectedCount(
            List<Relation> relations,
            List<String> variableOrder,
            List<String> projected,
            long deadlineNanos) {
        LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) LeapfrogJoin.join(
                relations,
                variableOrder,
                projected,
                LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                config.joinSafeDistinctFastPath(),
                deadlineNanos);
        return count.count();
    }

    private static double qError(double estimate, long actual) {
        double smoothedEstimate = Math.max(1.0D, estimate);
        double smoothedActual = Math.max(1.0D, (double) actual);
        return Math.max(smoothedEstimate / smoothedActual, smoothedActual / smoothedEstimate);
    }

    private static double relativeError(double estimate, long actual) {
        return Math.abs(Math.max(0.0D, estimate) - Math.max(0L, actual)) / Math.max(1.0D, actual);
    }

    private static EvaluationStats fromCompilationStats(ExecutablePlan.CompilationStats compilationStats) {
        EvaluationStats stats = new EvaluationStats();
        stats.addQueryNanos(compilationStats.queryNanos());
        stats.addMappingNanos(compilationStats.mappingNanos());
        return stats;
    }

    private static List<List<String>> sampleOrders(List<String> baseOrder, int randomOrders, long seed) {
        List<List<String>> orders = new ArrayList<>();
        List<String> frozenBase = List.copyOf(baseOrder);
        orders.add(frozenBase);
        if (randomOrders <= 0 || baseOrder.size() <= 1) {
            return orders;
        }
        Random random = new Random(seed);
        java.util.Set<List<String>> seen = new java.util.HashSet<>();
        seen.add(frozenBase);
        int attempts = 0;
        int targetSize = 1 + randomOrders;
        while (orders.size() < targetSize && attempts < randomOrders * 50) {
            List<String> shuffled = new ArrayList<>(baseOrder);
            java.util.Collections.shuffle(shuffled, random);
            if (seen.add(shuffled)) {
                orders.add(shuffled);
            }
            attempts++;
        }
        return orders;
    }

    record PrefixEstimationStep(
            int step,
            String variable,
            List<String> prefixOrder,
            double estimate,
            long actual,
            long estimateNanos,
            long evalNanos,
            long prefixNanos,
            long cumulativeEstimateNanos,
            long cumulativeEvalNanos,
            long cumulativePrefixNanos,
            double qError,
            double relativeError,
            double cumulativeQError,
            double cumulativeRelativeError) {
    }
}
