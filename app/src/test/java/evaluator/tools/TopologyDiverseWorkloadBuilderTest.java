package evaluator.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cpq.CPQ;
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
}
