package evaluator.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.bench.BenchTypes.CountResult;
import evaluator.bench.BenchTypes.EvaluationMode;
import evaluator.bench.BenchTypes.RowResult;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.Plan;
import evaluator.index.CpqIndex.Edge;
import evaluator.index.NativeCpqIndex;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class CpqEvaluatorTest {
    private static final Path INDEX_PATH = Path.of("local/indices/robotssmall.k2.idx");
    private static final int LABEL_SEARCH_LIMIT = 50;

    @Test
    void evaluateSingleEdgeMatchesIndexPairs() throws Exception {
        NativeCpqIndex index = loadIndex();
        LabelQuery labelQuery = findLabelWithPairs(index);
        Assumptions.assumeTrue(labelQuery != null, "No label produced matches in index");

        BenchEngine evaluator = new BenchEngine(index);
        ConjunctiveQuery query = buildSingleEdgeQuery(labelQuery.predicate(), true, true);
        Plan decomposition = query.decomposeSingleEdge();
        Component component = decomposition.components().get(0);

        List<Edge> pairs = index.query(component.cpq());
        Assumptions.assumeTrue(!pairs.isEmpty(), "Empty pair list for selected label");

        RowResult rows = (RowResult) evaluator.evaluate(decomposition, EvaluationMode.ROWS);
        Set<IntPair> expected = toPairSet(pairs);
        Set<IntPair> actual = toPairSet(rows.rows(), component);
        assertEquals(expected, actual);

        CountResult count = (CountResult) evaluator.evaluate(decomposition, EvaluationMode.COUNT);
        assertEquals(expected.size(), count.count());
    }

    @Test
    void countUsesDistinctFreeVariableValues() throws Exception {
        NativeCpqIndex index = loadIndex();
        LabelQuery labelQuery = findLabelWithPairs(index);
        Assumptions.assumeTrue(labelQuery != null, "No label produced matches in index");

        BenchEngine evaluator = new BenchEngine(index);
        ConjunctiveQuery query = buildSingleEdgeQuery(labelQuery.predicate(), true, false);
        Plan decomposition = query.decomposeSingleEdge();
        Component component = decomposition.components().get(0);

        List<Edge> pairs = index.query(component.cpq());
        Assumptions.assumeTrue(!pairs.isEmpty(), "Empty pair list for selected label");

        long expected = pairs.stream().map(Edge::source).distinct().count();
        CountResult count = (CountResult) evaluator.evaluate(decomposition, EvaluationMode.COUNT);
        assertEquals(expected, count.count());
    }

    private static NativeCpqIndex loadIndex() throws Exception {
        Assumptions.assumeTrue(Files.exists(INDEX_PATH), "Missing index file " + INDEX_PATH);
        return NativeCpqIndex.load(INDEX_PATH);
    }

    private static LabelQuery findLabelWithPairs(NativeCpqIndex index) {
        for (int id = 0; id <= LABEL_SEARCH_LIMIT; id++) {
            Predicate predicate = new Predicate(id, String.valueOf(id));
            List<Edge> pairs = index.query(CPQ.label(predicate));
            if (!pairs.isEmpty()) {
                return new LabelQuery(predicate);
            }
        }
        return null;
    }

    private static ConjunctiveQuery buildSingleEdgeQuery(Predicate label, boolean freeSource, boolean freeTarget) {
        CQ cq = CQ.empty();
        VarCQ source = freeSource ? cq.addFreeVariable("a") : cq.addBoundVariable("a");
        VarCQ target = freeTarget ? cq.addFreeVariable("b") : cq.addBoundVariable("b");
        cq.addAtom(source, label, target);
        return ConjunctiveQuery.from(cq);
    }

    private static Set<IntPair> toPairSet(List<Edge> pairs) {
        Set<IntPair> out = new HashSet<>();
        for (Edge pair : pairs) {
            out.add(new IntPair(pair.source(), pair.target()));
        }
        return out;
    }

    private static Set<IntPair> toPairSet(Iterable<Map<String, Integer>> rows, Component component) {
        String sourceVar = Plan.varName(component.s());
        String targetVar = Plan.varName(component.t());
        Set<IntPair> out = new HashSet<>();
        for (Map<String, Integer> row : rows) {
            Integer source = row.get(sourceVar);
            Integer target = row.get(targetVar);
            if (source != null && target != null) {
                out.add(new IntPair(source, target));
            }
        }
        return out;
    }

    private record LabelQuery(Predicate predicate) {
    }

    private record IntPair(int source, int target) {
    }
}
