package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchEngineDeadlineTest {
    private static final String VALID_QUERY = "(x,y) \u2190 0(x,y)";

    @Test
    void evaluateWithStatsRejectsExpiredDeadline() {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());

        assertThrows(Deadline.Exceeded.class, () -> engine.evaluateWithStats(plan(), BenchTypes.EvaluationMode.COUNT, System.nanoTime() - 1));
    }

    @Test
    void estimateCountRejectsExpiredDeadline() {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());

        assertThrows(Deadline.Exceeded.class, () -> engine.estimateCount(plan(), System.nanoTime() - 1));
    }

    @Test
    void profileOrdersRejectsExpiredDeadline() {
        BenchEngine engine = new BenchEngine(new FakeCpqIndex());

        assertThrows(Deadline.Exceeded.class, () -> engine.profileOrders(plan(), 2, 7L, System.nanoTime() - 1));
    }

    @Test
    void rowResultRejectsExpiredDeadlineDuringFinalMaterialization() {
        List<Map<String, Integer>> rows = List.of(
                Map.of("x", 1, "y", 2),
                Map.of("x", 1, "y", 2),
                Map.of("x", 2, "y", 3));

        assertThrows(Deadline.Exceeded.class, () -> BenchTypes.RowResult.fromRows(rows, System.nanoTime() - 1));
    }

    @Test
    void rowResultDeduplicatesRowsWhenDeadlineAllows() {
        List<Map<String, Integer>> rows = List.of(
                Map.of("x", 1, "y", 2),
                Map.of("x", 1, "y", 2),
                Map.of("x", 2, "y", 3));

        BenchTypes.RowResult result = BenchTypes.RowResult.fromRows(rows, Long.MAX_VALUE);

        assertEquals(2, result.rows().size());
    }

    private static Plan plan() {
        return ConjunctiveQuery.parse(VALID_QUERY).decomposeSingleEdge();
    }

    private static final class FakeCpqIndex implements CpqIndex {
        private final List<Edge> edges = List.of(
                new Edge(1, 2),
                new Edge(2, 3),
                new Edge(3, 4));

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
            return edges;
        }
    }
}
