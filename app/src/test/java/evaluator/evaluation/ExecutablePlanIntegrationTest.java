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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Test
    void compileReusesDuplicateCompositeComponentsWithinOnePlan() {
        Plan plan = duplicatePathPlan("(x,b,d) \u2190 0(x,a), 1(a,b), 0(x,c), 1(c,d)");
        String path = plan.components().get(0).cpq().toString();
        CountingCpqIndex index = new CountingCpqIndex(Map.of(
                path,
                List.of(
                        new CpqIndex.Edge(1, 100),
                        new CpqIndex.Edge(2, 200))));

        ExecutablePlan executable = ExecutablePlan.compile(plan, index);

        assertFalse(executable.isEmpty());
        assertEquals(1, index.queryCalls());
        assertEquals(1, index.queryCalls(path));
        assertEquals(List.of(2L, 2L), executable.componentCounts());

        LeapfrogJoin.JoinResult.Count count = (LeapfrogJoin.JoinResult.Count) executable.join(
                plan.variableOrder(),
                LeapfrogJoin.JoinMode.PROJECTED_COUNT,
                true);
        assertEquals(2L, count.count());
    }

    @Test
    void compileCacheDoesNotPersistAcrossPlans() {
        Plan firstPlan = duplicatePathPlan("(x,b,d) \u2190 0(x,a), 1(a,b), 0(x,c), 1(c,d)");
        Plan secondPlan = duplicatePathPlan("(u,p,q) \u2190 0(u,m), 1(m,p), 0(u,n), 1(n,q)");
        String path = firstPlan.components().get(0).cpq().toString();
        CountingCpqIndex index = new CountingCpqIndex(Map.of(
                path,
                List.of(
                        new CpqIndex.Edge(1, 100),
                        new CpqIndex.Edge(2, 200))));

        ExecutablePlan first = ExecutablePlan.compile(firstPlan, index);
        ExecutablePlan second = ExecutablePlan.compile(secondPlan, index);

        assertFalse(first.isEmpty());
        assertFalse(second.isEmpty());
        assertEquals(2, index.queryCalls());
        assertEquals(2, index.queryCalls(path));
    }

    @Test
    void compileUsesPrimitiveMatchFastPathWhenAvailable() {
        CQ syntax = CQ.empty();
        VarCQ x = syntax.addFreeVariable("x");
        VarCQ y = syntax.addFreeVariable("y");
        syntax.addAtom(x, new Predicate(0, "0"), y);

        ConjunctiveQuery query = ConjunctiveQuery.from(syntax);
        Plan plan = query.decomposeSingleEdge();
        FastPathCpqIndex index = new FastPathCpqIndex(List.of(
                new CpqIndex.Edge(1, 2),
                new CpqIndex.Edge(2, 3)));

        ExecutablePlan executable = ExecutablePlan.compile(plan, index);

        assertFalse(executable.isEmpty());
        assertEquals(0, index.queryCalls());
        assertEquals(1, index.queryMatchesCalls());
        assertEquals(List.of(2L), executable.componentCounts());
    }

    private static Plan duplicatePathPlan(String queryText) {
        ConjunctiveQuery query = ConjunctiveQuery.parse(queryText);
        VarCQ source = variable(query, "x", "u");
        VarCQ firstTarget = variable(query, "b", "p");
        VarCQ secondTarget = variable(query, "d", "q");
        CPQ path = CPQ.concat(
                CPQ.label(new Predicate(0, "0")),
                CPQ.label(new Predicate(1, "1")));
        return new Plan(query, List.of(
                new Plan.Component(
                        source,
                        firstTarget,
                        2,
                        mask(0, 1),
                        path,
                        path.toString()),
                new Plan.Component(
                        source,
                        secondTarget,
                        2,
                        mask(2, 3),
                        path,
                        path.toString())));
    }

    private static VarCQ variable(ConjunctiveQuery query, String primary, String alternate) {
        for (VarCQ variable : query.vertices()) {
            String name = variable.getName();
            if (name.equals(primary) || name.equals(alternate)) {
                return variable;
            }
        }
        throw new IllegalArgumentException("Missing variable " + primary + "/" + alternate);
    }

    private static BitSet mask(int... edges) {
        BitSet mask = new BitSet();
        for (int edge : edges) {
            mask.set(edge);
        }
        return mask;
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

    private static final class CountingCpqIndex implements CpqIndex {
        private final Map<String, List<Edge>> edgesByCpq;
        private final Map<String, Integer> queryCallsByCpq = new HashMap<>();
        private int queryCalls = 0;

        private CountingCpqIndex(Map<String, List<Edge>> edgesByCpq) {
            this.edgesByCpq = Map.copyOf(edgesByCpq);
        }

        private int queryCalls() {
            return queryCalls;
        }

        private int queryCalls(String cpq) {
            return queryCallsByCpq.getOrDefault(cpq, 0);
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
            return edgesByCpq.getOrDefault(cpq.toString(), List.of()).size();
        }

        @Override
        public List<Edge> query(CPQ cpq) {
            String key = cpq.toString();
            queryCalls++;
            queryCallsByCpq.merge(key, 1, Integer::sum);
            return edgesByCpq.getOrDefault(key, List.of());
        }
    }

    private static final class FastPathCpqIndex implements CpqIndex {
        private final List<Edge> edges;
        private int queryCalls = 0;
        private int queryMatchesCalls = 0;

        private FastPathCpqIndex(List<Edge> edges) {
            this.edges = List.copyOf(edges);
        }

        private int queryCalls() {
            return queryCalls;
        }

        private int queryMatchesCalls() {
            return queryMatchesCalls;
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
            queryCalls++;
            return edges;
        }

        @Override
        public QueryMatches queryMatches(CPQ cpq) {
            queryMatchesCalls++;
            return new QueryMatches() {
                @Override
                public int size() {
                    return edges.size();
                }

                @Override
                public void forEach(IntPairConsumer consumer) {
                    for (Edge edge : edges) {
                        consumer.accept(edge.source(), edge.target());
                    }
                }
            };
        }
    }
}
