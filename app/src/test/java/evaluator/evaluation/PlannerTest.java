package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.index.CpqIndex;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PlannerTest {
    private static final String VALID_QUERY = "(x,y) \u2190 0(x,y)";

    @Test
    void supportedMethodsUseAllImplementedMethodsByDefault() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.clearProperty("cpq.decompose.methods");
        try {
            Planner planner = new Planner(new FakeCpqIndex());

            assertEquals(
                    Set.of(DecompositionMethod.values()),
                    planner.supportedMethods());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void defaultPlannerAllowsLeafSystemRMethod() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.clearProperty("cpq.decompose.methods");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.MethodSelection selection = planner.planMethod(
                    cq,
                    DecompositionMethod.EXHAUSTIVE_LEAF_SYSTEM_R,
                    1,
                    2,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    DecompositionMethod.EXHAUSTIVE_LEAF_SYSTEM_R,
                    selection.candidates().get(0).method());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void planAllRespectsMethodAllowlist() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge,cost");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 0, 2, 0);

            assertEquals(List.of(DecompositionMethod.SINGLE_EDGE, DecompositionMethod.COST),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(Set.of(DecompositionMethod.SINGLE_EDGE, DecompositionMethod.COST),
                    selection.decompositionNanosByMethod().keySet());
            assertTrue(selection.timedOutMethods().isEmpty());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void planAllAcceptsBaselineMethodAliases() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "count_only,max_collapse");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 1, 2, 0);

            assertEquals(List.of(DecompositionMethod.COST, DecompositionMethod.MAX_COLLAPSE),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(Set.of(DecompositionMethod.COST, DecompositionMethod.MAX_COLLAPSE),
                    selection.decompositionNanosByMethod().keySet());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void planAllCanExplicitlyEnableReplacementAliases() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty(
                "cpq.decompose.methods",
                "single_edge_system_r,exhaustive_system_r,leaf_cost");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 1, 2, 0);

            assertEquals(
                    List.of(
                            DecompositionMethod.SINGLE_EDGE_SYSTEM_R,
                            DecompositionMethod.EXHAUSTIVE_LEAF_COST,
                            DecompositionMethod.EXHAUSTIVE_LEAF_SYSTEM_R),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(
                    Set.of(
                            DecompositionMethod.SINGLE_EDGE_SYSTEM_R,
                            DecompositionMethod.EXHAUSTIVE_LEAF_COST,
                            DecompositionMethod.EXHAUSTIVE_LEAF_SYSTEM_R),
                    selection.decompositionNanosByMethod().keySet());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void singleEdgeSystemRMarksPlanForSystemROrderSelection() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "single_edge_system_r");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 1, 2, 0);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    Plan.OrderPolicy.SYSTEM_R,
                    selection.candidates().get(0).plan().orderPolicy());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void leafSystemRAliasSelectsTerminalLeafMethod() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "leaf_system_r");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse("(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)");

            Planner.Selection selection = planner.planAll(cq, 0, 3, 0);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    DecompositionMethod.EXHAUSTIVE_LEAF_SYSTEM_R,
                    selection.candidates().get(0).method());
            assertEquals(
                    Plan.OrderPolicy.SYSTEM_R,
                    selection.candidates().get(0).plan().orderPolicy());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void leafCostAliasSelectsTerminalLeafCostMethod() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "leaf_cost");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse("(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)");

            Planner.Selection selection = planner.planAll(cq, 0, 3, 0);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    DecompositionMethod.EXHAUSTIVE_LEAF_COST,
                    selection.candidates().get(0).method());
            assertEquals(
                    Plan.OrderPolicy.HEURISTIC,
                    selection.candidates().get(0).plan().orderPolicy());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void leafCostMethodChoosesCheaperTerminalLeaf() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "exhaustive_leaf_cost");
        try {
            Planner planner = new Planner(new CostSensitiveFakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse("(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)");

            Planner.Selection selection = planner.planAll(cq, 0, 4, 0);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    "(((0◦1)◦2)◦3)",
                    selection.candidates().get(0).plan().components().get(0).cpq().toString());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void planAllUsesCoverLimitAsSeriesParallelRestartCount() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "series_parallel");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 1, 2, 0);

            assertEquals(List.of(DecompositionMethod.SERIES_PARALLEL),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(1, selection.candidates().size());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void planAllFallsBackToSingleEdgeWhenSeriesParallelCoverLimitIsZero() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "series_parallel");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 0, 2, 0);

            assertEquals(List.of(DecompositionMethod.SERIES_PARALLEL),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(1, selection.candidates().size());
            assertEquals(1, selection.candidates().get(0).plan().components().size());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void planMethodCountsEagerDecompositionWorkInReportedTiming() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "cost");
        try {
            Planner planner = new Planner(new SlowCostFakeCpqIndex(40L));
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.MethodSelection selection = planner.planMethod(
                    cq,
                    DecompositionMethod.COST,
                    1,
                    2,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE);

            assertEquals(1, selection.candidates().size());
            assertTrue(selection.decomposeNanos() >= 20_000_000L,
                    "Expected eager decomposition work to be included in decomposeNanos");
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void plannerRejectsRemovedDiameterAlias() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "diameter");
        try {
            assertThrows(IllegalArgumentException.class, () -> new Planner(new FakeCpqIndex()));
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void plannerRejectsRemovedSeriesParallelSystemRAlias() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "series_parallel_system_r");
        try {
            assertThrows(IllegalArgumentException.class, () -> new Planner(new FakeCpqIndex()));
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    private static class FakeCpqIndex implements CpqIndex {
        private final List<Edge> edges = List.of(
                new Edge(1, 2),
                new Edge(2, 3),
                new Edge(3, 4));

        @Override
        public int k() {
            return 3;
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
            return edges;
        }
    }

    private static final class CostSensitiveFakeCpqIndex implements CpqIndex {
        private final List<Edge> edges = List.of(
                new Edge(1, 2),
                new Edge(2, 3),
                new Edge(3, 4));

        @Override
        public int k() {
            return 4;
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
            String text = cpq.toString();
            return text.contains("⁻") ? 10L : 1L;
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            return edges;
        }
    }

    private static final class SlowCostFakeCpqIndex extends FakeCpqIndex {
        private final long delayMs;

        private SlowCostFakeCpqIndex(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public long cost(CPQ cpq) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while computing cost", ex);
            }
            return super.cost(cpq);
        }
    }
}
