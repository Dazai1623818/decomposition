package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void scorePlanWithHeuristicOrderUsesDefaultHeuristicVariableOrder() {
        SystemRScorer scorer = new SystemRScorer(new StatsIndex(Map.of(
                "0", new CpqIndex.ComponentStats(2L, new int[] { 1, 2 }, new int[] { 10, 11 }),
                "1", new CpqIndex.ComponentStats(2L, new int[] { 10 }, new int[] { 100, 200 }))));
        ConjunctiveQuery query = ConjunctiveQuery.parse("(x,z) \u2190 0(x,y), 1(y,z)");

        SystemRScorer.PlanSelection selection = scorer.scorePlanWithHeuristicOrder(
                query.decomposeSingleEdge(),
                Long.MAX_VALUE);

        assertEquals(List.of("?y", "?x", "?z"), selection.order());
        assertEquals(2.0D, selection.estimatedCount());
    }

    @Test
    void selectBestOrderStaysWithinLocalNeighborhoodAroundHeuristicOrder() {
        SystemRScorer scorer = new SystemRScorer(new StatsIndex(Map.of(
                "0", new CpqIndex.ComponentStats(2L, new int[] { 1, 2 }, new int[] { 10, 11 }),
                "1", new CpqIndex.ComponentStats(2L, new int[] { 10 }, new int[] { 100, 200 }))));
        ConjunctiveQuery query = ConjunctiveQuery.parse("(x,z) \u2190 0(x,y), 1(y,z)");
        List<String> baseOrder = List.of("?y", "?x", "?z");

        SystemRScorer.OrderSelection selection = scorer.selectBestOrder(
                query.decomposeSingleEdge(),
                Long.MAX_VALUE);

        assertTrue(OrderCandidates.buildLocal(baseOrder, List.of("?x", "?z"), 0).contains(selection.order()));
        assertEquals(2.0D, selection.estimatedCount());
    }

    @Test
    void selectBestLocalOrderKeepsBaseOrderWithoutClearImprovement() {
        SystemRScorer scorer = new SystemRScorer(new DummyIndex());
        ExecutablePlan executable = executable(
                "(x,z) \u2190 0(x,y), 1(y,z)",
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
                                new int[] { 10, 11 },
                                new int[] { 100, 200 },
                                Map.of(10, new int[] { 100 }, 11, new int[] { 200 }),
                                Map.of(100, new int[] { 10 }, 200, new int[] { 11 }))),
                List.of(2L, 2L));
        List<String> baseOrder = List.of("?x", "?y", "?z");

        SystemRScorer.OrderSelection selection = scorer.selectBestLocalOrder(
                executable,
                baseOrder,
                Long.MAX_VALUE);

        assertEquals(baseOrder, selection.order());
    }

    @Test
    void selectBestLocalOrderCanReorderProjectedVariablesWithinLocalBlock() {
        SystemRScorer scorer = new SystemRScorer(new DummyIndex());
        ExecutablePlan executable = executable(
                "(x,z) \u2190 0(x,y), 1(y,z)",
                List.of(
                        binary(
                                "?x",
                                "?y",
                                new int[] { 1, 2, 3, 4 },
                                new int[] { 10 },
                                Map.of(
                                        1, new int[] { 10 },
                                        2, new int[] { 10 },
                                        3, new int[] { 10 },
                                        4, new int[] { 10 }),
                                Map.of(10, new int[] { 1, 2, 3, 4 })),
                        binary(
                                "?y",
                                "?z",
                                new int[] { 10 },
                                new int[] { 100 },
                                Map.of(10, new int[] { 100 }),
                                Map.of(100, new int[] { 10 }))),
                List.of(4L, 4L));

        SystemRScorer.OrderSelection selection = scorer.selectBestLocalOrder(
                executable,
                List.of("?y", "?x", "?z"),
                Long.MAX_VALUE);

        assertEquals(List.of("?y", "?z", "?x"), selection.order());
    }

    private static ExecutablePlan executable(
            String queryText,
            List<Relation> relations,
            List<Long> componentCounts) {
        return new ExecutablePlan(
                ConjunctiveQuery.parse(queryText).decomposeSingleEdge(),
                relations,
                componentCounts,
                new ExecutablePlan.CompilationStats(0L, 0L),
                false);
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
