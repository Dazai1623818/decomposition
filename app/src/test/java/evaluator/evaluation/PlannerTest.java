package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.index.CpqIndex;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PlannerTest {
    private static final String VALID_QUERY = "(x,y) \u2190 0(x,y)";
    private static final String COMPLEX_QUERY = "(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,x1)";
    private static final String PATH_FOUR_EDGES =
            "(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)";

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
                            DecompositionMethod.EXHAUSTIVE_SYSTEM_R,
                            DecompositionMethod.EXHAUSTIVE_LEAF_COST),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(
                    Set.of(
                            DecompositionMethod.SINGLE_EDGE_SYSTEM_R,
                            DecompositionMethod.EXHAUSTIVE_SYSTEM_R,
                            DecompositionMethod.EXHAUSTIVE_LEAF_COST),
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
            ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);

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
    void exhaustiveSystemRAliasSelectsFullCoverMethod() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "exhaustive_system_r");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 1, 2, 0);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    DecompositionMethod.EXHAUSTIVE_SYSTEM_R,
                    selection.candidates().get(0).method());
            assertEquals(
                    Plan.OrderPolicy.SYSTEM_R,
                    selection.candidates().get(0).plan().orderPolicy());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void exhaustiveSystemRCoverOnlyAliasSelectsFullCoverMethodWithHeuristicOrder() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "exhaustive_system_r_cover_only");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 1, 2, 0);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    DecompositionMethod.EXHAUSTIVE_SYSTEM_R_COVER_ONLY,
                    selection.candidates().get(0).method());
            assertEquals(
                    Plan.OrderPolicy.HEURISTIC,
                    selection.candidates().get(0).plan().orderPolicy());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void costSystemROrderOnlyUsesBaselineCostSelectionWithEstimatedOrder() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "cost_system_r_order_only");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

            Planner.Selection selection = planner.planAll(cq, 1, 2, 0);

            assertEquals(1, selection.candidates().size());
            assertEquals(
                    DecompositionMethod.COST_SYSTEM_R_ORDER_ONLY,
                    selection.candidates().get(0).method());
            assertEquals(
                    Plan.OrderPolicy.SYSTEM_R,
                    selection.candidates().get(0).plan().orderPolicy());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void pathDecompositionDisablesParallelIntersectionExpansionButKeepsUnaryIdentity() {
        Planner planner = new Planner(new FakeCpqIndex());
        ConjunctiveQuery cq = ConjunctiveQuery.parse("(x) \u2190 0(x,x), 1(x,x)");

        Planner.MethodSelection collapseSelection = planner.planMethod(
                cq,
                DecompositionMethod.MAX_COLLAPSE,
                0,
                1,
                Long.MAX_VALUE,
                Long.MAX_VALUE);
        Planner.MethodSelection pathSelection = planner.planMethod(
                cq,
                DecompositionMethod.PATH_DECOMPOSITION,
                0,
                1,
                Long.MAX_VALUE,
                Long.MAX_VALUE);

        assertEquals(1, collapseSelection.candidates().get(0).plan().components().size());
        assertEquals(2, pathSelection.candidates().get(0).plan().components().size());
        assertTrue(pathSelection.candidates().get(0).plan().components().stream()
                .allMatch(component -> component.isUnary() && component.maskUnsafe().cardinality() == 1));
    }

    @Test
    void selectJoinOrderUsesMinNdvHeuristicWhenConfigured() {
        String previous = System.getProperty("cpq.join.heuristicOrder");
        System.setProperty("cpq.join.heuristicOrder", "min_ndv");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ExecutablePlan executable = executablePlan(
                    "(x) \u2190 0(x,y), 1(y,z)",
                    List.of(
                            relation(
                                    "?x",
                                    "?y",
                                    new int[] { 1 },
                                    new int[] { 10, 11, 12, 13 },
                                    Map.of(1, new int[] { 10, 11, 12, 13 }),
                                    Map.of(
                                            10, new int[] { 1 },
                                            11, new int[] { 1 },
                                            12, new int[] { 1 },
                                            13, new int[] { 1 })),
                            relation(
                                    "?y",
                                    "?z",
                                    new int[] { 10, 11, 12, 13 },
                                    new int[] { 20 },
                                    Map.of(
                                            10, new int[] { 20 },
                                            11, new int[] { 20 },
                                            12, new int[] { 20 },
                                            13, new int[] { 20 }),
                                    Map.of(20, new int[] { 10, 11, 12, 13 }))),
                    List.of(100L, 100L));

            Planner.JoinOrderPlan selection = planner.selectJoinOrder(executable, false, Long.MAX_VALUE);

            assertEquals(List.of("?x", "?z", "?y"), selection.order());
        } finally {
            restoreProperty("cpq.join.heuristicOrder", previous);
        }
    }

    @Test
    void selectJoinOrderUsesProjectedMinNdvHeuristicWhenConfigured() {
        String previous = System.getProperty("cpq.join.heuristicOrder");
        System.setProperty("cpq.join.heuristicOrder", "projected_min_ndv");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ExecutablePlan executable = executablePlan(
                    "(z) \u2190 0(x,y), 1(y,z)",
                    List.of(
                            relation(
                                    "?x",
                                    "?y",
                                    new int[] { 1 },
                                    new int[] { 10, 11, 12, 13 },
                                    Map.of(1, new int[] { 10, 11, 12, 13 }),
                                    Map.of(
                                            10, new int[] { 1 },
                                            11, new int[] { 1 },
                                            12, new int[] { 1 },
                                            13, new int[] { 1 })),
                            relation(
                                    "?y",
                                    "?z",
                                    new int[] { 10, 11, 12, 13 },
                                    new int[] { 20 },
                                    Map.of(
                                            10, new int[] { 20 },
                                            11, new int[] { 20 },
                                            12, new int[] { 20 },
                                            13, new int[] { 20 }),
                                    Map.of(20, new int[] { 10, 11, 12, 13 }))),
                    List.of(100L, 100L));

            Planner.JoinOrderPlan selection = planner.selectJoinOrder(executable, false, Long.MAX_VALUE);

            assertEquals(List.of("?z", "?x", "?y"), selection.order());
        } finally {
            restoreProperty("cpq.join.heuristicOrder", previous);
        }
    }

    @Test
    void selectJoinOrderUsesLocalSystemRHeuristicWhenConfigured() {
        String previous = System.getProperty("cpq.join.heuristicOrder");
        System.setProperty("cpq.join.heuristicOrder", "local_system_r");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ExecutablePlan executable = executablePlan(
                    "(x,z) \u2190 0(x,y), 1(y,z)",
                    List.of(
                            relation(
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
                            relation(
                                    "?y",
                                    "?z",
                                    new int[] { 10 },
                                    new int[] { 100 },
                                    Map.of(10, new int[] { 100 }),
                                    Map.of(100, new int[] { 10 }))),
                    List.of(4L, 4L));

            Planner.JoinOrderPlan selection = planner.selectJoinOrder(executable, false, Long.MAX_VALUE);

            assertEquals(List.of("?y", "?z", "?x"), selection.order());
            assertTrue(selection.estimateNanos() > 0L);
        } finally {
            restoreProperty("cpq.join.heuristicOrder", previous);
        }
    }

    @Test
    void exhaustiveSystemRCanChooseNonTerminalCoverThatLeafMethodExcludes() {
        Planner planner = new Planner(new SynopsisSensitiveFakeCpqIndex());
        ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);

        Planner.MethodSelection fullSelection = planner.planMethod(
                cq,
                DecompositionMethod.EXHAUSTIVE_SYSTEM_R,
                0,
                2,
                Long.MAX_VALUE,
                Long.MAX_VALUE);
        Planner.MethodSelection leafSelection = planner.planMethod(
                cq,
                DecompositionMethod.EXHAUSTIVE_LEAF_SYSTEM_R,
                0,
                2,
                Long.MAX_VALUE,
                Long.MAX_VALUE);

        assertEquals(1, fullSelection.candidates().size());
        assertEquals(1, leafSelection.candidates().size());

        Plan fullPlan = fullSelection.candidates().get(0).plan();
        Plan leafPlan = leafSelection.candidates().get(0).plan();

        assertHasMask(fullPlan, 0);
        assertHasMask(fullPlan, 1);
        assertHasMask(fullPlan, 2);
        assertHasMask(fullPlan, 3);

        assertTrue(leafPlan.components().stream().anyMatch(component -> component.maskUnsafe().cardinality() > 1),
                () -> "Expected a terminal leaf cover with at least one merged component, got " + leafPlan.components());
    }

    @Test
    void leafCostAliasSelectsTerminalLeafCostMethod() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "leaf_cost");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);

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
            ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);

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
    void seriesParallelSystemRAliasUsesRerankedSeriesParallelCandidateWithSystemROrder() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "series_parallel_system_r");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);

            Planner.Selection selection = planner.planAll(cq, 3, 3, 0);

            assertEquals(List.of(DecompositionMethod.SERIES_PARALLEL_SYSTEM_R),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(1, selection.candidates().size());
            assertEquals(
                    Plan.OrderPolicy.SYSTEM_R,
                    selection.candidates().get(0).plan().orderPolicy());
            assertTrue(selection.candidates().get(0).plan().components().size() >= 1);
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void seriesParallelSystemRCoverOnlyAliasUsesRerankedSeriesParallelCandidateWithHeuristicOrder() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "series_parallel_system_r_cover_only");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);

            Planner.Selection selection = planner.planAll(cq, 3, 3, 0);

            assertEquals(List.of(DecompositionMethod.SERIES_PARALLEL_SYSTEM_R_COVER_ONLY),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(1, selection.candidates().size());
            assertEquals(
                    Plan.OrderPolicy.HEURISTIC,
                    selection.candidates().get(0).plan().orderPolicy());
            assertTrue(selection.candidates().get(0).plan().components().size() >= 1);
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void seriesParallelSystemROrderOnlyUsesBaselineSeriesParallelSelectionWithEstimatedOrder() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        try {
            ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);
            System.setProperty("cpq.decompose.methods", "series_parallel");
            Planner.MethodSelection baseline = new Planner(new FakeCpqIndex()).planMethod(
                    cq,
                    DecompositionMethod.SERIES_PARALLEL,
                    0,
                    3,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE);
            System.setProperty("cpq.decompose.methods", "series_parallel_system_r_order_only");
            Planner planner = new Planner(new FakeCpqIndex());

            Planner.Selection selection = planner.planAll(cq, 0, 3, 0);

            assertEquals(List.of(DecompositionMethod.SERIES_PARALLEL_SYSTEM_R_ORDER_ONLY),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(1, selection.candidates().size());
            assertEquals(
                    Plan.OrderPolicy.SYSTEM_R,
                    selection.candidates().get(0).plan().orderPolicy());
            assertEquals(
                    baseline.candidates().get(0).plan().components().stream()
                            .map(Plan.Component::signature)
                            .toList(),
                    selection.candidates().get(0).plan().components().stream()
                            .map(Plan.Component::signature)
                            .toList());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void legacyGuidedSeriesParallelAliasNowSelectsCoverOnlyMethod() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "series_parallel_system_r_guided_only");
        try {
            Planner planner = new Planner(new FakeCpqIndex());
            ConjunctiveQuery cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES);

            Planner.Selection selection = planner.planAll(cq, 0, 3, 0);

            assertEquals(List.of(DecompositionMethod.SERIES_PARALLEL_SYSTEM_R_COVER_ONLY),
                    selection.candidates().stream().map(Planner.Candidate::method).distinct().toList());
            assertEquals(1, selection.candidates().size());
            assertEquals(
                    Plan.OrderPolicy.HEURISTIC,
                    selection.candidates().get(0).plan().orderPolicy());
        } finally {
            restoreProperty("cpq.decompose.methods", previousMethods);
        }
    }

    @Test
    void planMethodCountsSlowComponentFilteringWorkInReportedTiming() {
        String previousMethods = System.getProperty("cpq.decompose.methods");
        System.setProperty("cpq.decompose.methods", "cost");
        try {
            Planner planner = new Planner(new SlowSupportsFakeCpqIndex(40L));
            ConjunctiveQuery cq = ConjunctiveQuery.parse(COMPLEX_QUERY);

            Planner.MethodSelection selection = planner.planMethod(
                    cq,
                    DecompositionMethod.COST,
                    1,
                    2,
                    Long.MAX_VALUE,
                    Long.MAX_VALUE);

            assertEquals(1, selection.candidates().size());
            assertTrue(selection.decomposeNanos() >= 20_000_000L,
                    "Expected decomposition-side component filtering work to be included in decomposeNanos");
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

    private static void assertHasMask(Plan plan, int... edges) {
        assertTrue(
                plan.components().stream().anyMatch(component -> hasMask(component.maskUnsafe(), edges)),
                () -> "Expected mask " + java.util.Arrays.toString(edges) + " in plan " + plan.components());
    }

    private static boolean hasMask(BitSet mask, int... edges) {
        if (mask.cardinality() != edges.length) {
            return false;
        }
        for (int edge : edges) {
            if (!mask.get(edge)) {
                return false;
            }
        }
        return true;
    }

    private static BitSet labelSet(CPQ cpq) {
        BitSet labels = new BitSet();
        collectLabels(cpq.toAbstractSyntaxTree(), labels);
        return labels;
    }

    private static void collectLabels(QueryTree node, BitSet labels) {
        if (node.getOperation() == OperationType.EDGE) {
            labels.set(node.getEdgeAtom().getLabel().getID());
        }
        for (int i = 0; i < node.getArity(); i++) {
            collectLabels(node.getOperand(i), labels);
        }
    }

    private static CpqIndex.ComponentStats stats(long tupleCount) {
        int size = Math.max(1, Math.toIntExact(tupleCount));
        int[] values = new int[size];
        for (int i = 0; i < size; i++) {
            values[i] = i;
        }
        return new CpqIndex.ComponentStats(tupleCount, values, values);
    }

    private static ExecutablePlan executablePlan(
            String queryText,
            List<Relation> relations,
            List<Long> componentCounts) {
        ConjunctiveQuery cq = ConjunctiveQuery.parse(queryText);
        return new ExecutablePlan(
                cq.decomposeSingleEdge(),
                relations,
                componentCounts,
                new ExecutablePlan.CompilationStats(0L, 0L),
                false);
    }

    private static Relation relation(
            String sourceVar,
            String targetVar,
            int[] allSources,
            int[] allTargets,
            Map<Integer, int[]> forward,
            Map<Integer, int[]> reverse) {
        return Relation.binary(
                sourceVar,
                targetVar,
                sourceVar + "->" + targetVar,
                new Relation.RelationProjection(allSources, allTargets, forward, reverse));
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

    private static final class SynopsisSensitiveFakeCpqIndex extends FakeCpqIndex {
        @Override
        public int k() {
            return 4;
        }

        @Override
        public CpqIndex.ComponentStats componentStats(CPQ cpq) {
            BitSet labels = labelSet(cpq);
            if (labels.cardinality() == 1) {
                return stats(1L);
            }
            return stats(100L);
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

    private static final class SlowSupportsFakeCpqIndex extends FakeCpqIndex {
        private final long delayMs;

        private SlowSupportsFakeCpqIndex(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public boolean supports(CPQ cpq) {
            if (isAtomic(cpq)) {
                return true;
            }
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while checking component support", ex);
            }
            return true;
        }

        private static boolean isAtomic(CPQ cpq) {
            String text = cpq.toString();
            return !text.contains("◦") && !text.contains("∩");
        }
    }
}
