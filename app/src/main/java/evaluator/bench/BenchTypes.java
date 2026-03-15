package evaluator.bench;

import evaluator.cpq.Plan;
import evaluator.evaluation.DecompositionMethod;
import evaluator.util.Deadline;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Benchmark-facing enums and records shared by CLI and runner/evaluator code.
 */
public final class BenchTypes {
    private BenchTypes() {
    }

    public record DecompositionCandidate(
            DecompositionMethod method,
            int ordinal,
            Plan decomposition,
            long decomposeNanos,
            long selectionEstimateNanos) {
        public DecompositionCandidate(
                DecompositionMethod method,
                int ordinal,
                Plan decomposition,
                long decomposeNanos) {
            this(method, ordinal, decomposition, decomposeNanos, 0L);
        }

        public DecompositionCandidate withSelectionEstimateNanos(long nanos) {
            return new DecompositionCandidate(method, ordinal, decomposition, decomposeNanos, nanos);
        }
    }

    public record DecompositionEvaluation(
            DecompositionMethod method,
            int ordinal,
            Plan decomposition,
            long decomposeNanos,
            EvaluationWithStats evaluation) {
    }

    public record CandidateSelection(
            List<DecompositionCandidate> candidates,
            Set<DecompositionMethod> timedOutMethods,
            Map<DecompositionMethod, Long> decompositionNanosByMethod) {
    }

    public record ComponentFilteredCandidates(
            List<DecompositionCandidate> allCandidates,
            List<DecompositionCandidate> filteredCandidates) {
    }

    public enum ExplorePreparationStatus {
        READY,
        NO_DECOMPOSITIONS,
        NO_DECOMPOSITIONS_AFTER_COMPONENT_FILTER
    }

    public record ExploreCandidates(
            ExplorePreparationStatus status,
            List<DecompositionCandidate> allCandidates,
            List<DecompositionCandidate> filteredCandidates,
            List<DecompositionCandidate> selectedCandidates) {
    }

    public enum ComparisonPreparationStatus {
        READY,
        NO_DECOMPOSITIONS
    }

    public record ComparisonCandidates(
            ComparisonPreparationStatus status,
            List<DecompositionCandidate> candidates,
            Set<DecompositionMethod> timedOutWithoutCandidate,
            Map<DecompositionMethod, Long> decompositionNanosByMethod) {
    }

    public enum EvaluationMode {
        ROWS("rows"),
        COUNT("count");

        private final String id;

        EvaluationMode(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }
    }

    public enum EvaluationMethod {
        LEAPFROG_ROWS("leapfrog_rows", EvaluationMode.ROWS),
        LEAPFROG_COUNT("leapfrog_count", EvaluationMode.COUNT);

        private final String id;
        private final EvaluationMode mode;

        EvaluationMethod(String id, EvaluationMode mode) {
            this.id = id;
            this.mode = mode;
        }

        public String id() {
            return id;
        }

        public EvaluationMode mode() {
            return mode;
        }

        public static EvaluationMethod fromMode(EvaluationMode mode) {
            return mode == EvaluationMode.ROWS ? LEAPFROG_ROWS : LEAPFROG_COUNT;
        }
    }

    public sealed interface EvaluationResult permits RowResult, CountResult {
    }

    public record RowResult(Set<Map<String, Integer>> rows) implements EvaluationResult {
        public RowResult(List<Map<String, Integer>> rows) {
            this(distinctRows(rows));
        }

        /**
         * Builds an immutable distinct row set while enforcing the method deadline
         * during the final materialization phase.
         */
        public static RowResult fromRows(List<Map<String, Integer>> rows, long deadlineNanos) {
            Deadline.check(deadlineNanos);
            TrustedDistinctRows distinct = new TrustedDistinctRows(distinctCapacity(rows.size()));
            for (Map<String, Integer> row : rows) {
                Deadline.check(deadlineNanos);
                distinct.add(row);
            }
            Deadline.check(deadlineNanos);
            return new RowResult(distinct);
        }

        public RowResult {
            rows = rows instanceof TrustedDistinctRows trusted
                    ? Collections.unmodifiableSet(trusted)
                    : freezeRows(rows);
        }

        private static Set<Map<String, Integer>> freezeRows(Set<Map<String, Integer>> rows) {
            return Collections.unmodifiableSet(new LinkedHashSet<>(rows));
        }

        private static int distinctCapacity(int rowCount) {
            return Math.max(16, (int) (rowCount / 0.75f) + 1);
        }

        private static TrustedDistinctRows distinctRows(List<Map<String, Integer>> rows) {
            TrustedDistinctRows distinct = new TrustedDistinctRows(distinctCapacity(rows.size()));
            distinct.addAll(rows);
            return distinct;
        }

        private static final class TrustedDistinctRows extends LinkedHashSet<Map<String, Integer>> {
            private TrustedDistinctRows(int capacity) {
                super(capacity);
            }
        }
    }

    public record CountResult(long count) implements EvaluationResult {
    }

    public static final class EvaluationStats {
        private long queryNanos;
        private long mappingNanos;
        private long estimateNanos;
        private long joinNanos;

        public void addQueryNanos(long nanos) {
            queryNanos += nanos;
        }

        public void addMappingNanos(long nanos) {
            mappingNanos += nanos;
        }

        public void addEstimateNanos(long nanos) {
            estimateNanos += nanos;
        }

        public void addJoinNanos(long nanos) {
            joinNanos += nanos;
        }

        public long queryNanos() {
            return queryNanos;
        }

        public long mappingNanos() {
            return mappingNanos;
        }

        public long estimateNanos() {
            return estimateNanos;
        }

        public long joinNanos() {
            return joinNanos;
        }
    }

    public record EvaluationWithStats(
            EvaluationResult result,
            EvaluationStats stats,
            List<String> variableOrder,
            double estimatedCount,
            double estimateStdError) {
    }

    public record CardinalityEstimate(
            double estimatedCount,
            double standardError,
            long queryNanos,
            long mappingNanos,
            long estimateNanos) {
    }

    public record OrderProfile(List<String> order, long joinNanos, long count) {
    }

    public record OrderProfileSummary(long queryNanos, long mappingNanos, List<OrderProfile> profiles) {
    }

    public enum OrderPolicy {
        HEURISTIC("heuristic"),
        RANDOM_SAMPLE("random_sample");

        private final String id;

        OrderPolicy(String id) {
            this.id = id;
        }

        public String id() {
            return id;
        }
    }

    /**
     * Benchmark specs used by future bench CLI commands.
     */
    public enum EvalFileStatus {
        OK,
        NO_DECOMPOSITIONS,
        TIMEOUT,
        ERROR
    }

    public record EvalFileProgress(
            int queryNumber,
            String queryText,
            String decompositionMethodId,
            int components,
            int maxDiameter,
            long answers,
            EvalFileStatus status,
            long parseNanos,
            long decomposeNanos,
            long queryNanos,
            long mappingNanos,
            long estimateNanos,
            long joinNanos,
            long wallNanos,
            String errorMessage) {
    }

    @FunctionalInterface
    public interface EvalFileProgressSink {
        EvalFileProgressSink NOOP = progress -> {
        };

        void onQueryResult(EvalFileProgress progress);
    }

    public record EvalFileSpec(
            Path indexPath,
            Path queriesFile,
            EvaluationMode evaluationMode,
            DecompositionMethod method,
            int coverLimit,
            int kOverride,
            int timeoutMs,
            EvalFileProgressSink progressSink) {
    }

    public record ExploreSpec(
            Path indexPath,
            String queryText,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int candidateLimit,
            int minComponents,
            int maxComponents,
            int methodTimeoutMs,
            int profileTimeoutMs,
            int profileOrders,
            long seed) {
    }

    public record CompareSpec(
            Path indexPath,
            String queryText,
            EvaluationMode evaluationMode,
            int coverLimit,
            int k,
            int decompositionTimeoutMs,
            int methodTimeoutMs,
            long seed) {
    }

    public record ProfileSpec(
            Path indexPath,
            String queryText,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int profileOrders,
            long seed,
            int timeoutMs) {
    }

    public record EstimateSpec(
            Path indexPath,
            String queryText,
            DecompositionMethod method,
            int coverLimit,
            int k,
            int timeoutMs) {
    }

    public record CompareFileSpec(
            Path indexPath,
            Path queriesFile,
            Path warmupQueriesFile,
            int warmupQueryLimit,
            EvaluationMode mode,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int coverLimit,
            int kOverride,
            long seed,
            Path compareLogPath,
            Path decompositionLogPath,
            boolean warmup) {
        public CompareFileSpec(
                Path indexPath,
                Path queriesFile,
                EvaluationMode mode,
                int methodTimeoutMs,
                int decompositionTimeoutMs,
                int coverLimit,
                int kOverride,
                long seed,
                Path compareLogPath,
                Path decompositionLogPath,
                boolean warmup) {
            this(
                    indexPath,
                    queriesFile,
                    null,
                    0,
                    mode,
                    methodTimeoutMs,
                    decompositionTimeoutMs,
                    coverLimit,
                    kOverride,
                    seed,
                    compareLogPath,
                    decompositionLogPath,
                    warmup);
        }
    }

    public record EstimationBenchSpec(
            Path indexPath,
            Path queriesFile,
            Path warmupQueriesFile,
            int methodTimeoutMs,
            int decompositionTimeoutMs,
            int coverLimit,
            int kOverride,
            long seed,
            Path outputPath) {
    }

    public record EvalFileReport(
            int queryCount,
            int failureCount,
            int timeoutCount) {
    }

    public record ExploreReport(
            ExplorePreparationStatus status,
            int candidateCount) {
    }

    public record CompareReport(
            ComparisonPreparationStatus status,
            int comparedMethods,
            int timeoutCount) {
    }

    public record ProfileReport(
            int profiledOrders,
            boolean timedOut) {
    }

    public record EstimateReport(
            double estimate,
            double standardError,
            boolean timedOut) {
    }

    public record CompareFileReport(
            int queryCount,
            long methodRows,
            long okRows,
            long timeoutRows,
            long decompositionTimeoutRows,
            long noCandidateRows,
            long errorRows,
            long elapsedNanos) {
    }

    public record EstimationBenchReport(
            int queryCount,
            long methodRows,
            long stepRows,
            long okRows,
            long timeoutRows,
            long decompositionTimeoutRows,
            long noCandidateRows,
            long errorRows,
            long elapsedNanos) {
    }
}
