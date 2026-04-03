package evaluator.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.EdgeCPQ;
import evaluator.bench.BenchTypes;
import evaluator.evaluation.DecompositionMethod;
import evaluator.index.CpqIndex;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TopologyDiverseWorkloadBuilderTest {
    private static final String QUERY =
            "(x0) \u2190 0(x0,gen0), 1(gen0,gen1), 2(x0,gen2), 3(gen1,gen3), 0(gen2,gen4)";
    private static final String PATH_QUERY =
            "(x0,x2) \u2190 0(x0,x1), 1(x1,x2)";

    @Test
    void randomizedHeadSelectionIsDeterministicPerSeed() {
        String first = TopologyDiverseWorkloadBuilder.randomizedHeadVarsKeyForTesting(QUERY, 3, 17L);
        String second = TopologyDiverseWorkloadBuilder.randomizedHeadVarsKeyForTesting(QUERY, 3, 17L);

        assertEquals(first, second);
    }

    @Test
    void randomizedHeadSelectionKeepsX0AnchoredAndNested() {
        List<String> arityOne = split(TopologyDiverseWorkloadBuilder.randomizedHeadVarsKeyForTesting(QUERY, 1, 23L));
        List<String> arityTwo = split(TopologyDiverseWorkloadBuilder.randomizedHeadVarsKeyForTesting(QUERY, 2, 23L));
        List<String> arityThree = split(TopologyDiverseWorkloadBuilder.randomizedHeadVarsKeyForTesting(QUERY, 3, 23L));

        assertEquals(List.of("x0"), arityOne);
        assertEquals("x0", arityTwo.get(0));
        assertEquals("x0", arityThree.get(0));
        assertEquals(arityTwo, arityThree.subList(0, 2));
    }

    @Test
    void randomizedHeadSelectionVariesAcrossSeeds() {
        Set<String> heads = new HashSet<>();
        for (long seed = 1L; seed <= 16L; seed++) {
            heads.add(TopologyDiverseWorkloadBuilder.randomizedHeadVarsKeyForTesting(QUERY, 3, seed));
        }

        assertTrue(heads.size() > 1, "expected different seeds to produce different projected heads");
    }

    @Test
    void customDatasetIndexesDefaultToSelectedCustomDatasets() {
        assertEquals(
                "ca-CondMat,wiki-RfA",
                TopologyDiverseWorkloadBuilder.selectedDatasetsForTesting(
                        "--dataset-index",
                        "ca-CondMat=/tmp/ca.idx",
                        "--dataset-index",
                        "wiki-RfA=/tmp/rfa.idx"));
        assertEquals(
                "",
                TopologyDiverseWorkloadBuilder.ignoredDatasetsForTesting(
                        "--dataset-index",
                        "ca-CondMat=/tmp/ca.idx",
                        "--dataset-index",
                        "wiki-RfA=/tmp/rfa.idx"));
        assertEquals(
                "/tmp/rfa.idx",
                TopologyDiverseWorkloadBuilder.datasetIndexPathForTesting(
                        "wiki-RfA",
                        "--dataset-index",
                        "ca-CondMat=/tmp/ca.idx",
                        "--dataset-index",
                        "wiki-RfA=/tmp/rfa.idx"));
    }

    @Test
    void workersPerDatasetDefaultsToInProcessAndParsesOverrides() {
        assertEquals(0, TopologyDiverseWorkloadBuilder.workersPerDatasetForTesting());
        assertEquals(
                8,
                TopologyDiverseWorkloadBuilder.workersPerDatasetForTesting(
                        "--workers-per-dataset",
                        "8"));
    }

    @Test
    void queryConcurrencyDefaultsToOneAndParsesOverrides() {
        assertEquals(1, TopologyDiverseWorkloadBuilder.queryConcurrencyForTesting());
        assertEquals(
                64,
                TopologyDiverseWorkloadBuilder.queryConcurrencyForTesting(
                        "--query-concurrency",
                        "64"));
    }

    @Test
    void validationModeDefaultsToFirstAnswerAndParsesQuickScan() {
        assertEquals("first_answer", TopologyDiverseWorkloadBuilder.validationModeForTesting());
        assertEquals(
                "quick_scan",
                TopologyDiverseWorkloadBuilder.validationModeForTesting(
                        "--validation-mode",
                        "quick_scan"));
    }

    @Test
    void independentEdgeDirectionsFlagParses() {
        assertFalse(TopologyDiverseWorkloadBuilder.independentEdgeDirectionsForTesting());
        assertTrue(TopologyDiverseWorkloadBuilder.independentEdgeDirectionsForTesting(
                "--independent-edge-directions"));
    }

    @Test
    void labelCountDefaultsToFourAndParsesOverrides() {
        assertEquals(4, TopologyDiverseWorkloadBuilder.labelCountForTesting());
        assertEquals(8, TopologyDiverseWorkloadBuilder.labelCountForTesting("--label-count", "8"));
    }

    @Test
    void independentEdgeDirectionsDoubleChoicePerEdge() {
        assertEquals(16L, TopologyDiverseWorkloadBuilder.candidateSpaceForTesting(PATH_QUERY, 4, false));
        assertEquals(64L, TopologyDiverseWorkloadBuilder.candidateSpaceForTesting(PATH_QUERY, 4, true));
        assertEquals(64L, TopologyDiverseWorkloadBuilder.candidateSpaceForTesting(PATH_QUERY, 8, false));
        assertEquals(256L, TopologyDiverseWorkloadBuilder.candidateSpaceForTesting(PATH_QUERY, 8, true));
    }

    @Test
    void renderWithDirectionsCanReverseEdgesWithoutChangingShapeFamily() {
        assertEquals(
                "(x0) \u2190 0(x0,x1), 1(x2,x1)",
                TopologyDiverseWorkloadBuilder.renderWithDirectionsForTesting(
                        PATH_QUERY,
                        new int[] { 0, 1 },
                        new boolean[] { false, true },
                        "x0"));
    }

    @Test
    void maxCollapseZeroScreenCanCatchZerosThatSingleEdgeMisses() {
        CpqIndex index = new LengthSensitiveIndex();

        assertFalse(TopologyDiverseWorkloadBuilder.definitelyZeroByCompilationForTesting(
                PATH_QUERY,
                index,
                DecompositionMethod.SINGLE_EDGE,
                1_000));
        assertTrue(TopologyDiverseWorkloadBuilder.definitelyZeroByCompilationForTesting(
                PATH_QUERY,
                index,
                DecompositionMethod.MAX_COLLAPSE,
                1_000));
    }

    @Test
    void firstAnswerEvaluationCanRejectJoinEmptyBodiesThatCompileNonEmpty() {
        CpqIndex index = new JoinEmptySingleEdgeIndex();

        TopologyDiverseWorkloadBuilder.QueryEvaluation evaluation =
                TopologyDiverseWorkloadBuilder.firstAnswerQueryForTesting(
                        PATH_QUERY,
                        index,
                        DecompositionMethod.MAX_COLLAPSE,
                        1_000);

        assertEquals(BenchTypes.EvalFileStatus.OK, evaluation.status());
        assertEquals(0L, evaluation.answers());
    }

    @Test
    void quickScanCanAcceptJoinEmptyBodiesThatExactEvaluationRejects() {
        CpqIndex index = new JoinEmptySingleEdgeIndex();

        TopologyDiverseWorkloadBuilder.QueryEvaluation evaluation =
                TopologyDiverseWorkloadBuilder.quickScanQueryForTesting(
                        PATH_QUERY,
                        index,
                        DecompositionMethod.MAX_COLLAPSE,
                        1_000);

        assertEquals(BenchTypes.EvalFileStatus.OK, evaluation.status());
        assertTrue(evaluation.answers() > 0L);
    }

    @Test
    void timeoutOutcomesAreAcceptedWhenOtherAritiesArePositive() {
        assertTrue(TopologyDiverseWorkloadBuilder.acceptedBodyForTesting(
                BenchTypes.EvalFileStatus.OK,
                3L,
                BenchTypes.EvalFileStatus.TIMEOUT,
                0L,
                BenchTypes.EvalFileStatus.OK,
                7L));
    }

    @Test
    void zeroAnswerStillRejectsBodyEvenWhenTimeoutsAreAllowed() {
        assertFalse(TopologyDiverseWorkloadBuilder.acceptedBodyForTesting(
                BenchTypes.EvalFileStatus.OK,
                3L,
                BenchTypes.EvalFileStatus.TIMEOUT,
                0L,
                BenchTypes.EvalFileStatus.OK,
                0L));
    }

    @Test
    void rejectionReasonIncludesDatasetAndArityForEarlyStopFailures() {
        assertEquals(
                "ca-CondMat_a2_zero_answers",
                TopologyDiverseWorkloadBuilder.rejectionReasonForTesting(
                        "ca-CondMat",
                        BenchTypes.EvalFileStatus.OK,
                        3L,
                        BenchTypes.EvalFileStatus.OK,
                        0L,
                        BenchTypes.EvalFileStatus.TIMEOUT,
                        0L));
    }

    @Test
    void quickScanRejectionReasonUsesComponentLanguage() {
        assertEquals(
                "ca-CondMat_a2_empty_component",
                TopologyDiverseWorkloadBuilder.rejectionReasonForTesting(
                        "ca-CondMat",
                        "quick_scan",
                        BenchTypes.EvalFileStatus.OK,
                        3L,
                        BenchTypes.EvalFileStatus.OK,
                        0L,
                        BenchTypes.EvalFileStatus.TIMEOUT,
                        0L));
    }

    private static List<String> split(String key) {
        return List.of(key.split("\\|"));
    }

    /**
     * Makes single-edge CPQs non-empty while any collapsed multi-edge CPQ is empty.
     * This isolates the benefit of the compile-only zero screen from join execution.
     */
    private static final class LengthSensitiveIndex implements CpqIndex {
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
            return Math.max(1, cpq.toQueryGraph().getEdges().size());
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            if (cpq.toQueryGraph().getEdges().size() <= 1) {
                return List.of(new Edge(1, 2));
            }
            return List.of();
        }
    }

    /**
     * Exposes the join-empty case that compile-only validation misses when k=1
     * forces a multi-edge query to stay decomposed into single-edge parts.
     */
    private static final class JoinEmptySingleEdgeIndex implements CpqIndex {
        @Override
        public int k() {
            return 1;
        }

        @Override
        public int intersections() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isIndexable(CPQ cpq) {
            return cpq.toQueryGraph().getEdges().size() <= 1;
        }

        @Override
        public long cost(CPQ cpq) {
            return 1L;
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            return switch (extractSingleLabelId(cpq)) {
                case 0 -> List.of(new Edge(1, 2));
                case 1 -> List.of(new Edge(3, 4));
                default -> List.of();
            };
        }

        private static int extractSingleLabelId(CPQ cpq) {
            if (cpq instanceof EdgeCPQ edge) {
                return edge.getLabel().getID();
            }
            QueryTree tree = cpq.toAbstractSyntaxTree();
            if (tree.getOperation() != OperationType.EDGE) {
                throw new IllegalStateException("Expected single-edge CPQ but found " + tree.getOperation());
            }
            return tree.getEdgeAtom().getLabel().getID();
        }
    }
}
