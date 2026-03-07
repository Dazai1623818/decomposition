package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.index.CpqIndex;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PlannerTest {
    private static final String VALID_QUERY = "(x,y) \u2190 0(x,y)";

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

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
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
