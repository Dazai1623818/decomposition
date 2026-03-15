package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.index.CpqIndex;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SystemRScorerTest {
    @Test
    void estimateProjectedCountUsesSummaryArithmeticAcrossRelations() {
        SystemRScorer scorer = new SystemRScorer(new DummyIndex());
        Relation left = binary(
                "?x",
                "?y",
                new int[] { 1, 2 },
                new int[] { 10 },
                Map.of(1, new int[] { 10 }, 2, new int[] { 10 }),
                Map.of(10, new int[] { 1, 2 }));
        Relation right = binary(
                "?x",
                "?z",
                new int[] { 2, 3 },
                new int[] { 20 },
                Map.of(2, new int[] { 20 }, 3, new int[] { 20 }),
                Map.of(20, new int[] { 2, 3 }));

        ProjectedCountEstimate estimate = scorer.estimateProjectedCount(
                List.of(left, right),
                List.of("?x", "?y", "?z"),
                List.of("?x"),
                Long.MAX_VALUE);

        assertEquals(2.0D, estimate.estimatedCount());
        assertEquals(0.0D, estimate.standardError());
    }

    @Test
    void estimateProjectedCountForPlanMatchesRelationPath() {
        SystemRScorer scorer = new SystemRScorer(new StatsIndex(Map.of(
                "0", new CpqIndex.ComponentStats(2L, new int[] { 1, 2 }, new int[] { 10, 11 }),
                "1", new CpqIndex.ComponentStats(2L, new int[] { 10 }, new int[] { 100, 200 }))));
        ConjunctiveQuery query = ConjunctiveQuery.parse("(x,z) \u2190 0(x,y), 1(y,z)");

        ProjectedCountEstimate planEstimate = scorer.estimateProjectedCount(
                query.decomposeSingleEdge(),
                List.of("?y", "?x", "?z"),
                List.of("?x", "?z"),
                Long.MAX_VALUE);
        ProjectedCountEstimate relationEstimate = scorer.estimateProjectedCount(
                List.of(
                        binary(
                                "?x",
                                "?y",
                                new int[] { 1, 2 },
                                new int[] { 10, 11 },
                                Map.of(1, new int[] { 10 }, 2, new int[] { 11 }),
                                Map.of(10, new int[] { 1 }, 11, new int[] { 2 })),
                        binary(
                                "?y",
                                "?z",
                                new int[] { 10 },
                                new int[] { 100, 200 },
                                Map.of(10, new int[] { 100, 200 }),
                                Map.of(100, new int[] { 10 }, 200, new int[] { 10 }))),
                List.of("?y", "?x", "?z"),
                List.of("?x", "?z"),
                Long.MAX_VALUE);

        assertEquals(relationEstimate.estimatedCount(), planEstimate.estimatedCount());
        assertEquals(2.0D, planEstimate.estimatedCount());
    }

    @Test
    void selectBestOrderBreaksTiesByProjectedDepthForSmallQueries() {
        SystemRScorer scorer = new SystemRScorer(new StatsIndex(Map.of(
                "0", new CpqIndex.ComponentStats(2L, new int[] { 1, 2 }, new int[] { 10, 11 }),
                "1", new CpqIndex.ComponentStats(2L, new int[] { 10 }, new int[] { 100, 200 }))));
        ConjunctiveQuery query = ConjunctiveQuery.parse("(x,z) \u2190 0(x,y), 1(y,z)");

        SystemRScorer.OrderSelection selection = scorer.selectBestOrder(
                query.decomposeSingleEdge(),
                Long.MAX_VALUE);

        assertEquals(List.of("?x", "?z", "?y"), selection.order());
        assertEquals(2.0D, selection.estimatedCount());
    }

    private static Relation binary(
            String sourceVar,
            String targetVar,
            int[] allSources,
            int[] allTargets,
            Map<Integer, int[]> forward,
            Map<Integer, int[]> reverse) {
        Relation.RelationProjection projection = new Relation.RelationProjection(
                allSources,
                allTargets,
                forward,
                reverse);
        return Relation.binary(sourceVar, targetVar, sourceVar + "->" + targetVar, projection);
    }

    private static class DummyIndex implements CpqIndex {
        @Override
        public int k() {
            return 2;
        }

        @Override
        public int intersections() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isIndexable(CPQ cpq) {
            return true;
        }

        @Override
        public long cost(CPQ cpq) {
            return 1L;
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            return List.of();
        }
    }

    private static final class StatsIndex extends DummyIndex {
        private final Map<String, ComponentStats> statsByCpq;

        private StatsIndex(Map<String, ComponentStats> statsByCpq) {
            this.statsByCpq = statsByCpq;
        }

        @Override
        public ComponentStats componentStats(CPQ cpq) {
            return statsByCpq.getOrDefault(cpq.toString(), ComponentStats.empty());
        }
    }
}
