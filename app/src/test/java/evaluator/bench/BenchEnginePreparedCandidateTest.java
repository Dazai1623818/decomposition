package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.bench.BenchTypes.DecompositionCandidate;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.evaluation.ExecutablePlan;
import evaluator.index.CpqIndex;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.Test;

class BenchEnginePreparedCandidateTest {
    private static final String VALID_QUERY = "(x,y) \u2190 0(x,y)";

    @Test
    void preparedWinnerIsReusedForEvaluation() throws Exception {
        CountingIndex index = new CountingIndex();
        EngineConfig config = new EngineConfig(
                true,
                2,
                1,
                0,
                0,
                0,
                EngineConfig.EstimatorType.WANDERJOIN,
                true,
                true,
                123L);
        BenchEngine engine = new BenchEngine(index, config);
        Plan plan = ConjunctiveQuery.parse(VALID_QUERY).decomposeSingleEdge();
        List<DecompositionCandidate> candidates = List.of(
                new DecompositionCandidate(evaluator.evaluation.DecompositionMethod.SINGLE_EDGE, 0, plan, 0L),
                new DecompositionCandidate(evaluator.evaluation.DecompositionMethod.SINGLE_EDGE, 1, plan, 0L));

        Method pick = BenchEngine.class.getDeclaredMethod(
                "pickBestPreparedCandidate",
                List.class,
                int.class,
                int.class,
                int.class,
                long.class);
        pick.setAccessible(true);
        Object prepared = pick.invoke(engine, candidates, 1, 0, 1, Long.MAX_VALUE);

        assertEquals(2, index.queryCalls);

        Method executableMethod = prepared.getClass().getDeclaredMethod("executable");
        executableMethod.setAccessible(true);
        ExecutablePlan executable = (ExecutablePlan) executableMethod.invoke(prepared);

        Method evaluate = BenchEngine.class.getDeclaredMethod(
                "evaluateWithStats",
                ExecutablePlan.class,
                BenchTypes.EvaluationMode.class,
                long.class);
        evaluate.setAccessible(true);
        evaluate.invoke(engine, executable, BenchTypes.EvaluationMode.COUNT, Long.MAX_VALUE);

        assertEquals(2, index.queryCalls);
    }

    private static final class CountingIndex implements CpqIndex {
        private final List<Edge> edges = List.of(
                new Edge(1, 2),
                new Edge(2, 3),
                new Edge(3, 4));
        private int queryCalls = 0;

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
            queryCalls++;
            return edges;
        }
    }
}
