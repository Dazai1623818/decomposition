package evaluator.bench;

import static evaluator.bench.BenchTypes.*;

import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import evaluator.evaluation.DecompositionMethod;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * Formatting and emission helpers for compare-file and estimationbench logs.
 */
final class BenchLogEmitter {
    private BenchLogEmitter() {
    }

    static long answerCount(EvaluationResult result) {
        if (result instanceof CountResult count) {
            return count.count();
        }
        if (result instanceof RowResult rows) {
            return rows.rows().size();
        }
        throw new IllegalArgumentException("Unsupported evaluation result type: " + result.getClass().getName());
    }

    static int edgesCollapsed(Plan decomposition) {
        int collapsed = 0;
        for (Component component : decomposition.components()) {
            collapsed += Math.max(0, component.maskUnsafe().cardinality() - 1);
        }
        return collapsed;
    }

    static void printCompareFileHeader(
            PrintWriter out,
            CompareFileSpec spec,
            int totalQueries,
            int warmupQueryCount,
            String warmupSource,
            int warmupLimit,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int joinOrderBudget,
            int coverLimit,
            int k,
            String command,
            long estimationSeed) {
        out.println("started=" + Instant.now());
        out.println("index=" + spec.indexPath());
        out.println("queries=" + spec.queriesFile());
        out.println("total_queries=" + totalQueries);
        out.println("warmup_enabled=" + spec.warmup());
        out.println("warmup_source=" + (spec.warmup() ? warmupSource : "-"));
        out.println("warmup_limit=" + warmupLimit);
        out.println("warmup_query_count=" + warmupQueryCount);
        out.println("per_method_timeout_ms=" + methodTimeoutMs);
        out.println("planning_timeout_ms=" + decompositionTimeoutMs);
        out.println("join_order_budget=" + joinOrderBudget);
        out.println("cover_limit=" + coverLimit);
        out.println("k=" + k);
        out.println("seed=" + spec.seed());
        out.println("selection_seed=" + estimationSeed);
        out.println("series_parallel_seed=" + estimationSeed);
        out.println("estimation_seed=" + estimationSeed);
        out.println("command=" + command);
        out.flush();
    }

    static void printEstimationBenchHeader(
            PrintWriter out,
            EstimationBenchSpec spec,
            int totalQueries,
            int warmupQueries,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int joinOrderBudget,
            int coverLimit,
            int k,
            String command,
            long runSeed,
            long estimationSeed) {
        out.println("started=" + Instant.now());
        out.println("index=" + spec.indexPath());
        out.println("queries=" + spec.queriesFile());
        out.println("total_queries=" + totalQueries);
        out.println("warmup_queries=" + (spec.warmupQueriesFile() == null ? "-" : spec.warmupQueriesFile()));
        out.println("warmup_query_count=" + warmupQueries);
        out.println("per_method_timeout_ms=" + methodTimeoutMs);
        out.println("planning_timeout_ms=" + decompositionTimeoutMs);
        out.println("join_order_budget=" + joinOrderBudget);
        out.println("cover_limit=" + coverLimit);
        out.println("k=" + k);
        out.println("seed=" + runSeed);
        out.println("selection_seed=" + estimationSeed);
        out.println("series_parallel_seed=" + estimationSeed);
        out.println("estimation_seed=" + estimationSeed);
        out.println("command=" + command);
        out.println(
                "# columns: query method ord step variable prefix_order full_order estimate stderr actual prefix_estimate_ms prefix_eval_ms prefix_total_ms prefix_cum_estimate_ms prefix_cum_eval_ms prefix_cum_total_ms q_error rel_error cum_q_error cum_rel_error final_answers final_estimate final_stderr parse_ms planning_ms planning_selection_ms method_wall_ms end_to_end_ms execution_ms index_lookup_ms mapping_ms join_order_ms join_ms status [error]");
        out.flush();
    }

    static String formatDecompositionLine(
            int queryNumber,
            DecompositionCandidate candidate) {
        return String.format(
                Locale.ROOT,
                "query=%d method=%s ord=%d decomposition=\"%s\"",
                queryNumber,
                candidate.method().name(),
                candidate.ordinal(),
                sanitize(formatDecomposition(candidate.decomposition())));
    }

    static void emitCompareFileRow(
            PrintWriter out,
            CompareFileRow row) {
        String errorSegment = row.errorMessage() == null || row.errorMessage().isBlank()
                ? ""
                : String.format(Locale.ROOT, " error=\"%s\"", sanitize(row.errorMessage()));
        out.println(String.format(
                Locale.ROOT,
                "query=%d method=%s ord=%d var_order=%s comps=%d max_diam=%d edges_collapsed=%d answers=%d estimate=%.6f stderr=%.6f parse_ms=%.3f planning_ms=%.3f planning_selection_ms=%.3f method_wall_ms=%.3f end_to_end_ms=%.3f execution_ms=%.3f index_lookup_ms=%.3f mapping_ms=%.3f join_order_ms=%.3f join_ms=%.3f status=%s%s",
                row.queryNumber(),
                row.method().name(),
                row.ordinal(),
                formatVariableOrder(row.variableOrder()),
                row.components(),
                row.maxDiameter(),
                row.edgesCollapsed(),
                row.answers(),
                row.estimatedCount(),
                row.estimateStdError(),
                nanosToMillis(row.timings().parseNanos()),
                nanosToMillis(row.timings().planningNanos()),
                nanosToMillis(row.timings().planningSelectionNanos()),
                nanosToMillis(row.timings().methodWallNanos()),
                nanosToMillis(row.timings().endToEndNanos()),
                nanosToMillis(row.timings().executionNanos()),
                nanosToMillis(row.timings().indexLookupNanos()),
                nanosToMillis(row.timings().mappingNanos()),
                nanosToMillis(row.timings().joinOrderNanos()),
                nanosToMillis(row.timings().joinNanos()),
                row.status(),
                errorSegment));
    }

    static void emitEstimationBenchRow(
            PrintWriter out,
            EstimationBenchRow row) {
        String safeVariable = row.variable() == null ? "-" : row.variable();
        String errorSegment = row.errorMessage() == null || row.errorMessage().isBlank()
                ? ""
                : String.format(Locale.ROOT, " error=\"%s\"", sanitize(row.errorMessage()));
        out.println(String.format(
                Locale.ROOT,
                "query=%d method=%s ord=%d step=%d variable=%s prefix_order=%s full_order=%s estimate=%.6f stderr=%.6f actual=%d prefix_estimate_ms=%.3f prefix_eval_ms=%.3f prefix_total_ms=%.3f prefix_cum_estimate_ms=%.3f prefix_cum_eval_ms=%.3f prefix_cum_total_ms=%.3f q_error=%.6f rel_error=%.6f cum_q_error=%.6f cum_rel_error=%.6f final_answers=%d final_estimate=%.6f final_stderr=%.6f parse_ms=%.3f planning_ms=%.3f planning_selection_ms=%.3f method_wall_ms=%.3f end_to_end_ms=%.3f execution_ms=%.3f index_lookup_ms=%.3f mapping_ms=%.3f join_order_ms=%.3f join_ms=%.3f status=%s%s",
                row.queryNumber(),
                row.method().name(),
                row.ordinal(),
                row.step(),
                safeVariable,
                formatVariableOrder(row.prefixOrder()),
                formatVariableOrder(row.fullOrder()),
                row.estimate(),
                row.standardError(),
                row.actual(),
                nanosToMillis(row.prefixEstimateNanos()),
                nanosToMillis(row.prefixEvalNanos()),
                nanosToMillis(row.prefixTotalNanos()),
                nanosToMillis(row.prefixCumulativeEstimateNanos()),
                nanosToMillis(row.prefixCumulativeEvalNanos()),
                nanosToMillis(row.prefixCumulativeTotalNanos()),
                row.qError(),
                row.relativeError(),
                row.cumulativeQError(),
                row.cumulativeRelativeError(),
                row.finalAnswers(),
                row.finalEstimate(),
                row.finalStdError(),
                nanosToMillis(row.timings().parseNanos()),
                nanosToMillis(row.timings().planningNanos()),
                nanosToMillis(row.timings().planningSelectionNanos()),
                nanosToMillis(row.timings().methodWallNanos()),
                nanosToMillis(row.timings().endToEndNanos()),
                nanosToMillis(row.timings().executionNanos()),
                nanosToMillis(row.timings().indexLookupNanos()),
                nanosToMillis(row.timings().mappingNanos()),
                nanosToMillis(row.timings().joinOrderNanos()),
                nanosToMillis(row.timings().joinNanos()),
                row.status(),
                errorSegment));
    }

    static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0d;
    }

    private static String formatDecomposition(Plan decomposition) {
        StringBuilder builder = new StringBuilder();
        builder.append("projected=").append(decomposition.projectedVariableNames());
        builder.append(" components=[");
        List<Component> components = decomposition.components();
        for (int i = 0; i < components.size(); i++) {
            if (i > 0) {
                builder.append(" | ");
            }
            Component component = components.get(i);
            builder.append(component.sourceVarName())
                    .append("->")
                    .append(component.targetVarName())
                    .append(" d=")
                    .append(component.diameter())
                    .append(" cpq=")
                    .append(component.cpq())
                    .append(" mask=")
                    .append(component.maskUnsafe());
        }
        builder.append("]");
        return builder.toString();
    }

    private static String sanitize(String text) {
        return text.replace('"', '\'');
    }

    private static String formatVariableOrder(List<String> variableOrder) {
        if (variableOrder == null || variableOrder.isEmpty()) {
            return "[]";
        }
        return "[" + String.join(",", variableOrder) + "]";
    }

    record MethodPhaseBreakdown(
            long parseNanos,
            long planningNanos,
            long planningSelectionNanos,
            long methodWallNanos,
            long executionNanos,
            long indexLookupNanos,
            long mappingNanos,
            long joinOrderNanos,
            long joinNanos) {
        static MethodPhaseBreakdown parseFailure(long parseNanos, long methodWallNanos) {
            return new MethodPhaseBreakdown(parseNanos, 0L, 0L, methodWallNanos, 0L, 0L, 0L, 0L, 0L);
        }

        static MethodPhaseBreakdown planningOnly(
                long parseNanos,
                long planningNanos,
                long planningSelectionNanos,
                long methodWallNanos) {
            return new MethodPhaseBreakdown(
                    parseNanos,
                    planningNanos,
                    planningSelectionNanos,
                    methodWallNanos,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L);
        }

        static MethodPhaseBreakdown success(
                long parseNanos,
                long planningNanos,
                long planningSelectionNanos,
                long methodWallNanos,
                EvaluationWithStats evaluation) {
            long indexLookupNanos = evaluation.stats().queryNanos();
            long mappingNanos = evaluation.stats().mappingNanos();
            long joinOrderNanos = evaluation.stats().estimateNanos();
            long joinNanos = evaluation.stats().joinNanos();
            return new MethodPhaseBreakdown(
                    parseNanos,
                    planningNanos,
                    planningSelectionNanos,
                    methodWallNanos,
                    indexLookupNanos + mappingNanos + joinOrderNanos + joinNanos,
                    indexLookupNanos,
                    mappingNanos,
                    joinOrderNanos,
                    joinNanos);
        }

        long endToEndNanos() {
            return parseNanos + methodWallNanos;
        }
    }

    record CompareFileRow(
            int queryNumber,
            DecompositionMethod method,
            int ordinal,
            int components,
            int maxDiameter,
            int edgesCollapsed,
            long answers,
            double estimatedCount,
            double estimateStdError,
            MethodPhaseBreakdown timings,
            List<String> variableOrder,
            String status,
            String errorMessage) {
        private static final long UNKNOWN_ANSWER_COUNT = -1L;

        static CompareFileRow parseError(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long wallNanos,
                String errorMessage) {
            return new CompareFileRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    0,
                    0,
                    UNKNOWN_ANSWER_COUNT,
                    Double.NaN,
                    Double.NaN,
                    MethodPhaseBreakdown.parseFailure(parseNanos, wallNanos),
                    List.of(),
                    "ERROR",
                    errorMessage);
        }

        static CompareFileRow withoutCandidate(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long decomposeNanos,
                long wallNanos,
                String status) {
            return new CompareFileRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    0,
                    0,
                    UNKNOWN_ANSWER_COUNT,
                    Double.NaN,
                    Double.NaN,
                    MethodPhaseBreakdown.planningOnly(parseNanos, decomposeNanos, 0L, wallNanos),
                    List.of(),
                    status,
                    null);
        }

        static CompareFileRow timeout(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                long parseNanos,
                long decomposeNanos,
                long wallNanos) {
            return new CompareFileRow(
                    queryNumber,
                    method,
                    candidate == null ? -1 : candidate.ordinal(),
                    candidate == null ? 0 : candidate.decomposition().size(),
                    candidate == null ? 0 : candidate.decomposition().maxDiameter(),
                    candidate == null ? 0 : BenchLogEmitter.edgesCollapsed(candidate.decomposition()),
                    UNKNOWN_ANSWER_COUNT,
                    Double.NaN,
                    Double.NaN,
                    MethodPhaseBreakdown.planningOnly(
                            parseNanos,
                            decomposeNanos,
                            candidate == null ? 0L : candidate.selectionEstimateNanos(),
                            wallNanos),
                    List.of(),
                    "EXEC_TIMEOUT",
                    null);
        }

        static CompareFileRow ok(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                long parseNanos,
                long wallNanos,
                EvaluationWithStats evaluation) {
            return new CompareFileRow(
                    queryNumber,
                    method,
                    candidate.ordinal(),
                    candidate.decomposition().size(),
                    candidate.decomposition().maxDiameter(),
                    BenchLogEmitter.edgesCollapsed(candidate.decomposition()),
                    answerCount(evaluation.result()),
                    evaluation.estimatedCount(),
                    evaluation.estimateStdError(),
                    MethodPhaseBreakdown.success(
                            parseNanos,
                            candidate.decomposeNanos(),
                            candidate.selectionEstimateNanos(),
                            wallNanos,
                            evaluation),
                    evaluation.variableOrder(),
                    "OK",
                    null);
        }
    }

    record EstimationBenchRow(
            int queryNumber,
            DecompositionMethod method,
            int ordinal,
            int step,
            String variable,
            List<String> prefixOrder,
            List<String> fullOrder,
            double estimate,
            double standardError,
            long actual,
            long prefixEstimateNanos,
            long prefixEvalNanos,
            long prefixTotalNanos,
            long prefixCumulativeEstimateNanos,
            long prefixCumulativeEvalNanos,
            long prefixCumulativeTotalNanos,
            double qError,
            double relativeError,
            double cumulativeQError,
            double cumulativeRelativeError,
            long finalAnswers,
            double finalEstimate,
            double finalStdError,
            MethodPhaseBreakdown timings,
            String status,
            String errorMessage) {
        private static final long UNKNOWN_COUNT = -1L;

        static EstimationBenchRow parseError(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long wallNanos,
                String errorMessage) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    null,
                    List.of(),
                    List.of(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    Double.NaN,
                    Double.NaN,
                    MethodPhaseBreakdown.parseFailure(parseNanos, wallNanos),
                    "ERROR",
                    errorMessage);
        }

        static EstimationBenchRow withoutCandidate(
                int queryNumber,
                DecompositionMethod method,
                long parseNanos,
                long decomposeNanos,
                long wallNanos,
                String status) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    -1,
                    0,
                    null,
                    List.of(),
                    List.of(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    Double.NaN,
                    Double.NaN,
                    MethodPhaseBreakdown.planningOnly(parseNanos, decomposeNanos, 0L, wallNanos),
                    status,
                    null);
        }

        static EstimationBenchRow timeout(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                long parseNanos,
                long decomposeNanos,
                long wallNanos) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    candidate == null ? -1 : candidate.ordinal(),
                    0,
                    null,
                    List.of(),
                    List.of(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    Double.NaN,
                    Double.NaN,
                    MethodPhaseBreakdown.planningOnly(
                            parseNanos,
                            decomposeNanos,
                            candidate == null ? 0L : candidate.selectionEstimateNanos(),
                            wallNanos),
                    "EXEC_TIMEOUT",
                    null);
        }

        static EstimationBenchRow okWithoutSteps(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                EvaluationWithStats evaluation,
                long parseNanos,
                long wallNanos) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    candidate.ordinal(),
                    0,
                    null,
                    List.of(),
                    evaluation.variableOrder(),
                    Double.NaN,
                    Double.NaN,
                    UNKNOWN_COUNT,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    answerCount(evaluation.result()),
                    evaluation.estimatedCount(),
                    evaluation.estimateStdError(),
                    MethodPhaseBreakdown.success(
                            parseNanos,
                            candidate.decomposeNanos(),
                            candidate.selectionEstimateNanos(),
                            wallNanos,
                            evaluation),
                    "OK",
                    null);
        }

        static EstimationBenchRow okStep(
                int queryNumber,
                DecompositionMethod method,
                DecompositionCandidate candidate,
                EvaluationWithStats evaluation,
                EstimatorDiagnostics.PrefixEstimationStep step,
                long parseNanos,
                long wallNanos) {
            return new EstimationBenchRow(
                    queryNumber,
                    method,
                    candidate.ordinal(),
                    step.step(),
                    step.variable(),
                    step.prefixOrder(),
                    evaluation.variableOrder(),
                    step.estimate(),
                    step.standardError(),
                    step.actual(),
                    step.estimateNanos(),
                    step.evalNanos(),
                    step.prefixNanos(),
                    step.cumulativeEstimateNanos(),
                    step.cumulativeEvalNanos(),
                    step.cumulativePrefixNanos(),
                    step.qError(),
                    step.relativeError(),
                    step.cumulativeQError(),
                    step.cumulativeRelativeError(),
                    answerCount(evaluation.result()),
                    evaluation.estimatedCount(),
                    evaluation.estimateStdError(),
                    MethodPhaseBreakdown.success(
                            parseNanos,
                            candidate.decomposeNanos(),
                            candidate.selectionEstimateNanos(),
                            wallNanos,
                            evaluation),
                    "OK",
                    null);
        }
    }
}
