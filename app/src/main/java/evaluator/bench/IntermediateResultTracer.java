package evaluator.bench;

import evaluator.evaluation.ExecutablePlan;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.util.Deadline;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * Computes exact intermediate-result progression for a compiled executable plan
 * along one fixed variable order. When a deadline expires, the completed stages
 * are preserved and the timeout point is reported.
 */
final class IntermediateResultTracer {
    private static final long UNKNOWN_COUNT = -1L;

    private IntermediateResultTracer() {
    }

    static Trace trace(
            ExecutablePlan executable,
            List<String> fullOrder,
            boolean safeDistinctFastPath,
            long deadlineNanos) {
        long sumComponentTuples = sum(executable.componentCounts());
        long maxComponentTuples = max(executable.componentCounts());
        List<String> normalizedFullOrder = List.copyOf(fullOrder);
        List<String> projectedOrder = List.copyOf(executable.plan().projectedVariableNames());
        List<Stage> stages = new ArrayList<>(normalizedFullOrder.size() + 2);

        long previousCount = sumComponentTuples;
        stages.add(new Stage(
                0,
                "mat",
                null,
                List.of(),
                sumComponentTuples,
                previousCount,
                ratio(sumComponentTuples, previousCount),
                ratio(sumComponentTuples, sumComponentTuples)));

        for (int i = 0; i < normalizedFullOrder.size(); i++) {
            String stageLabel = Integer.toString(i + 1);
            String variable = normalizedFullOrder.get(i);
            List<String> prefixOrder = normalizedFullOrder.subList(0, i + 1);
            try {
                long prefixCount = exactProjectedCount(
                        executable,
                        normalizedFullOrder,
                        prefixOrder,
                        safeDistinctFastPath,
                        deadlineNanos);
                stages.add(new Stage(
                        i + 1,
                        stageLabel,
                        variable,
                        List.copyOf(prefixOrder),
                        prefixCount,
                        previousCount,
                        ratio(prefixCount, previousCount),
                        ratio(prefixCount, sumComponentTuples)));
                previousCount = prefixCount;
            } catch (Deadline.Exceeded | CancellationException ex) {
                return new Trace(
                        TraceStatus.EXEC_TIMEOUT,
                        stageLabel,
                        variable,
                        sumComponentTuples,
                        maxComponentTuples,
                        UNKNOWN_COUNT,
                        normalizedFullOrder,
                        projectedOrder,
                        List.copyOf(stages));
            }
        }

        try {
            long answerCount = exactProjectedCount(
                    executable,
                    normalizedFullOrder,
                    projectedOrder,
                    safeDistinctFastPath,
                    deadlineNanos);
            stages.add(new Stage(
                    normalizedFullOrder.size() + 1,
                    "ans",
                    null,
                    projectedOrder,
                    answerCount,
                    previousCount,
                    ratio(answerCount, previousCount),
                    ratio(answerCount, sumComponentTuples)));
            return new Trace(
                    TraceStatus.OK,
                    null,
                    null,
                    sumComponentTuples,
                    maxComponentTuples,
                    answerCount,
                    normalizedFullOrder,
                    projectedOrder,
                    List.copyOf(stages));
        } catch (Deadline.Exceeded | CancellationException ex) {
            return new Trace(
                    TraceStatus.EXEC_TIMEOUT,
                    "ans",
                    null,
                    sumComponentTuples,
                    maxComponentTuples,
                    UNKNOWN_COUNT,
                    normalizedFullOrder,
                    projectedOrder,
                    List.copyOf(stages));
        }
    }

    private static long exactProjectedCount(
            ExecutablePlan executable,
            List<String> variableOrder,
            List<String> projected,
            boolean safeDistinctFastPath,
            long deadlineNanos) {
        Deadline.check(deadlineNanos);
        if (projected.equals(executable.plan().projectedVariableNames())) {
            return ((LeapfrogJoin.JoinResult.Count) executable.join(
                    variableOrder,
                    LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                    safeDistinctFastPath,
                    deadlineNanos)).count();
        }
        return ((LeapfrogJoin.JoinResult.Count) LeapfrogJoin.join(
                executable.relations(),
                variableOrder,
                projected,
                LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                safeDistinctFastPath,
                deadlineNanos)).count();
    }

    private static double ratio(long numerator, long denominator) {
        return denominator <= 0L ? Double.NaN : (double) numerator / (double) denominator;
    }

    private static long sum(List<Long> values) {
        long sum = 0L;
        for (Long value : values) {
            if (value != null) {
                sum += value;
            }
        }
        return sum;
    }

    private static long max(List<Long> values) {
        long max = 0L;
        for (Long value : values) {
            if (value != null) {
                max = Math.max(max, value);
            }
        }
        return max;
    }

    enum TraceStatus {
        OK,
        EXEC_TIMEOUT
    }

    record Trace(
            TraceStatus status,
            String timeoutStageLabel,
            String timeoutVariable,
            long sumComponentTuples,
            long maxComponentTuples,
            long answerCount,
            List<String> fullOrder,
            List<String> projectedOrder,
            List<Stage> stages) {
        boolean timedOut() {
            return status == TraceStatus.EXEC_TIMEOUT;
        }
    }

    record Stage(
            int stageIndex,
            String stageLabel,
            String variable,
            List<String> prefixOrder,
            long count,
            long previousCount,
            double retainedVsPrevious,
            double retainedVsMaterialized) {
    }
}
