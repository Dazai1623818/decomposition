package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.index.CpqIndex;
import evaluator.util.Deadline;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExecutablePlanIntegrationTest {
    @Test
    void compilesSingleEdgePlanAndCountsAnswersWithLeapfrog() {
        CQ syntax = CQ.empty();
        VarCQ x = syntax.addFreeVariable("x");
        VarCQ y = syntax.addFreeVariable("y");
        syntax.addAtom(x, new Predicate(0, "0"), y);

        ConjunctiveQuery query = ConjunctiveQuery.from(syntax);
        Plan plan = query.decomposeSingleEdge();

        CpqIndex index = new FakeCpqIndex(List.of(
                new CpqIndex.Edge(1, 2),
                new CpqIndex.Edge(2, 3),
                new CpqIndex.Edge(3, 4)));

        ExecutablePlan executable = ExecutablePlan.compile(plan, index);
        assertFalse(executable.isEmpty());

        LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                plan.variableOrder(),
                LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                true);
        assertEquals(3L, count.count());
    }

    @Test
    void compileRejectsExpiredDeadline() {
        CQ syntax = CQ.empty();
        VarCQ x = syntax.addFreeVariable("x");
        VarCQ y = syntax.addFreeVariable("y");
        syntax.addAtom(x, new Predicate(0, "0"), y);

        ConjunctiveQuery query = ConjunctiveQuery.from(syntax);
        Plan plan = query.decomposeSingleEdge();
        CpqIndex index = new FakeCpqIndex(List.of(new CpqIndex.Edge(1, 2)));

        org.junit.jupiter.api.Assertions.assertThrows(
                Deadline.Exceeded.class,
                () -> ExecutablePlan.compile(plan, index, System.nanoTime() - 1));
    }

    private static final class FakeCpqIndex implements CpqIndex {
        private final List<Edge> edges;

        private FakeCpqIndex(List<Edge> edges) {
            this.edges = List.copyOf(edges);
        }

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
            return edges.size();
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            return edges;
        }
    }
}
