package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import dev.roanh.gmark.lang.cpq.CPQ;
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
    void preparedSelectionIsReusedForEvaluation() throws Exception {
        CountingIndex index = new CountingIndex();
        BenchEngine engine = new BenchEngine(index, EngineConfig.defaults());
        ConjunctiveQuery cq = ConjunctiveQuery.parse(VALID_QUERY);

        Method select = BenchEngine.class.getDeclaredMethod(
                "selectBestPreparedCandidatesWithTimeoutInfo",
                ConjunctiveQuery.class,
                int.class,
                int.class,
                int.class,
                long.class);
        select.setAccessible(true);
        Object selection = select.invoke(engine, cq, 1, 2, 0, Long.MAX_VALUE);

        Method candidatesMethod = selection.getClass().getDeclaredMethod("candidates");
        candidatesMethod.setAccessible(true);
        List<?> candidates = (List<?>) candidatesMethod.invoke(selection);

        assertFalse(candidates.isEmpty());
        assertEquals(candidates.size(), index.queryCalls);

        Object prepared = candidates.get(0);
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

        assertEquals(candidates.size(), index.queryCalls);
    }

    @Test
    void systemROrderSelectionReusesPreparedExecutable() throws Exception {
        CountingIndex index = new CountingIndex();
        BenchEngine engine = new BenchEngine(index, EngineConfig.defaults());
        Plan plan = ConjunctiveQuery.parse(VALID_QUERY)
                .decomposeSingleEdge()
                .withOrderPolicy(Plan.OrderPolicy.SYSTEM_R);
        ExecutablePlan executable = ExecutablePlan.compile(plan, index, Long.MAX_VALUE);

        assertEquals(1, index.queryCalls);

        Method evaluate = BenchEngine.class.getDeclaredMethod(
                "evaluateWithStats",
                ExecutablePlan.class,
                BenchTypes.EvaluationMode.class,
                long.class);
        evaluate.setAccessible(true);
        BenchTypes.EvaluationWithStats evaluation = (BenchTypes.EvaluationWithStats) evaluate.invoke(
                engine,
                executable,
                BenchTypes.EvaluationMode.COUNT,
                Long.MAX_VALUE);

        assertEquals(1, index.queryCalls);
        assertEquals(3.0D, evaluation.estimatedCount());
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

        @Override
        public ComponentStats componentStats(CPQ cpq) {
            return ComponentStats.fromEdges(edges);
        }
    }
}
