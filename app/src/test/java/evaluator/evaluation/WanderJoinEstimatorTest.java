package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import evaluator.evaluation.Relation;
import evaluator.evaluation.Relation.RelationProjection;
import evaluator.util.Deadline;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;

class WanderJoinEstimatorTest {
    @Test
    void estimatesProjectedCountExactlyForSingleRelationProjection() {
        Relation relation = relation("?x", "?y", new int[][] {
                { 1, 10 },
                { 2, 20 },
                { 2, 30 }
        });

        WanderJoinEstimator.Estimate estimate = WanderJoinEstimator.estimateProjectedCount(
                List.of(relation),
                List.of("?x", "?y"),
                List.of("?x"),
                64,
                7L);

        assertEquals(2.0, estimate.estimatedCount(), 0.0);
        assertEquals(0.0, estimate.standardError(), 0.0);
    }

    @Test
    void returnsZeroWhenNoExtensionExists() {
        Relation first = relation("?x", "?y", new int[][] {
                { 1, 2 },
                { 2, 3 }
        });
        Relation second = relation("?y", "?z", new int[][] {
                { 4, 5 }
        });

        WanderJoinEstimator.Estimate estimate = WanderJoinEstimator.estimateProjectedCount(
                List.of(first, second),
                List.of("?x", "?y", "?z"),
                List.of("?x"),
                64,
                11L);

        assertEquals(0.0, estimate.estimatedCount(), 0.0);
        assertEquals(0.0, estimate.standardError(), 0.0);
    }

    @Test
    void abortsPromptlyWhenInterrupted() {
        Relation relation = relation("?x", "?y", new int[][] {
                { 1, 10 },
                { 2, 20 },
                { 2, 30 }
        });

        Thread.currentThread().interrupt();
        try {
            assertThrows(CancellationException.class, () -> WanderJoinEstimator.estimateProjectedCount(
                    List.of(relation),
                    List.of("?x", "?y"),
                    List.of("?x"),
                    8,
                    7L));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void rejectsExpiredDeadline() {
        Relation relation = relation("?x", "?y", new int[][] {
                { 1, 10 },
                { 2, 20 },
                { 2, 30 }
        });

        assertThrows(Deadline.Exceeded.class, () -> WanderJoinEstimator.estimateProjectedCount(
                List.of(relation),
                List.of("?x", "?y"),
                List.of("?x"),
                8,
                7L,
                System.nanoTime() - 1));
    }

    private static Relation relation(String sourceVar, String targetVar, int[][] pairs) {
        Map<Integer, int[]> forward = new HashMap<>();
        Map<Integer, int[]> reverse = new HashMap<>();
        int[] allSources = new int[pairs.length];
        int[] allTargets = new int[pairs.length];

        for (int i = 0; i < pairs.length; i++) {
            int source = pairs[i][0];
            int target = pairs[i][1];
            allSources[i] = source;
            allTargets[i] = target;
            forward.compute(source, (key, existing) -> append(existing, target));
            reverse.compute(target, (key, existing) -> append(existing, source));
        }

        java.util.Arrays.sort(allSources);
        java.util.Arrays.sort(allTargets);
        allSources = deduplicate(allSources);
        allTargets = deduplicate(allTargets);

        for (Map.Entry<Integer, int[]> entry : forward.entrySet()) {
            int[] values = entry.getValue();
            java.util.Arrays.sort(values);
            entry.setValue(deduplicate(values));
        }
        for (Map.Entry<Integer, int[]> entry : reverse.entrySet()) {
            int[] values = entry.getValue();
            java.util.Arrays.sort(values);
            entry.setValue(deduplicate(values));
        }

        RelationProjection projection = new RelationProjection(allSources, allTargets, forward, reverse);
        return Relation.binary(sourceVar, targetVar, sourceVar + "->" + targetVar, projection);
    }

    private static int[] append(int[] values, int value) {
        if (values == null) {
            return new int[] { value };
        }
        int[] copy = java.util.Arrays.copyOf(values, values.length + 1);
        copy[values.length] = value;
        return copy;
    }

    private static int[] deduplicate(int[] values) {
        if (values.length == 0) {
            return values;
        }
        int unique = 1;
        for (int i = 1; i < values.length; i++) {
            if (values[i] != values[unique - 1]) {
                values[unique++] = values[i];
            }
        }
        return unique == values.length ? values : java.util.Arrays.copyOf(values, unique);
    }
}
