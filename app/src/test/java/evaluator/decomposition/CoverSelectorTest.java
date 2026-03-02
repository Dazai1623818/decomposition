package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;
import org.junit.jupiter.api.Test;

class CoverSelectorTest {
    @Test
    void costOrderPrefersLowerCostBeforeHigherDiameter() {
        QueryFixture fixture = singleEdgeQuery();
        Component lowDiamCheap = component(fixture, "low-diam", 10, 1);
        Component highDiamExpensive = component(fixture, "high-diam", 11, 4);

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                lowDiamCheap.cpq(), 1L,
                highDiamExpensive.cpq(), 5L));

        CoverSelector selector = new CoverSelector(1, CoverSelector.Order.COST, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(lowDiamCheap, highDiamExpensive)).toList();

        assertEquals(1, covers.size());
        assertEquals("low-diam", covers.get(0).components().get(0).normalized());
    }

    @Test
    void diameterOrderPrefersHigherDiameterBeforeLowerCost() {
        QueryFixture fixture = singleEdgeQuery();
        Component highDiamExpensive = component(fixture, "high-diam", 20, 5);
        Component lowDiamCheap = component(fixture, "low-diam", 21, 2);

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                highDiamExpensive.cpq(), 100L,
                lowDiamCheap.cpq(), 1L));

        CoverSelector selector = new CoverSelector(1, CoverSelector.Order.DIAMETER, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(lowDiamCheap, highDiamExpensive)).toList();

        assertEquals(1, covers.size());
        assertEquals("high-diam", covers.get(0).components().get(0).normalized());
    }

    @Test
    void costOrderBreaksCostTiesByLargerMask() {
        PathQueryFixture fixture = twoEdgePathQuery();
        Component twoEdge = component(
                fixture.x(),
                fixture.z(),
                "two-edge",
                22,
                2,
                mask(0, 1));
        Component left = component(
                fixture.x(),
                fixture.y(),
                "left",
                23,
                1,
                mask(0));
        Component right = component(
                fixture.y(),
                fixture.z(),
                "right",
                24,
                1,
                mask(1));

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                twoEdge.cpq(), 10L,
                left.cpq(), 5L,
                right.cpq(), 5L));

        CoverSelector selector = new CoverSelector(1, CoverSelector.Order.COST, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(twoEdge, left, right)).toList();

        assertEquals(1, covers.size());
        assertEquals("two-edge", covers.get(0).components().get(0).normalized());
    }

    @Test
    void limitCountsUniqueValidCovers() {
        QueryFixture fixture = singleEdgeQuery();
        Component dupFirst = component(fixture, "dup", 30, 1);
        Component dupSecond = component(fixture, "dup", 30, 1);
        Component alternative = component(fixture, "alt", 31, 1);

        CoverSelector selector = new CoverSelector(2);
        List<Plan> covers = selector.select(fixture.query(), List.of(dupFirst, dupSecond, alternative)).toList();

        assertEquals(2, covers.size());
        Set<String> labels = covers.stream()
                .map(cover -> cover.components().get(0).normalized())
                .collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of("dup", "alt"), labels);
    }

    @Test
    void returnsAllUniqueValidCoversWhenLimitExceedsAvailable() {
        QueryFixture fixture = singleEdgeQuery();
        Component dupFirst = component(fixture, "dup", 40, 1);
        Component dupSecond = component(fixture, "dup", 40, 1);
        Component alternative = component(fixture, "alt", 41, 1);

        CoverSelector selector = new CoverSelector(5);
        List<Plan> covers = selector.select(fixture.query(), List.of(dupFirst, dupSecond, alternative)).toList();

        assertEquals(2, covers.size());
    }

    private static QueryFixture singleEdgeQuery() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addBoundVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        cq.addAtom(x, new Predicate(0, "0"), y);
        return new QueryFixture(ConjunctiveQuery.from(cq), x, y);
    }

    private static PathQueryFixture twoEdgePathQuery() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addBoundVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        VarCQ z = cq.addBoundVariable("z");
        cq.addAtom(x, new Predicate(0, "0"), y);
        cq.addAtom(y, new Predicate(1, "1"), z);
        return new PathQueryFixture(ConjunctiveQuery.from(cq), x, y, z);
    }

    private static Component component(QueryFixture fixture, String normalized, int labelId, int diameter) {
        BitSet mask = mask(0);
        CPQ cpq = CPQ.label(new Predicate(labelId, String.valueOf(labelId)));
        return new Component(fixture.s(), fixture.t(), diameter, mask, cpq, normalized);
    }

    private static Component component(
            VarCQ s,
            VarCQ t,
            String normalized,
            int labelId,
            int diameter,
            BitSet mask) {
        CPQ cpq = CPQ.label(new Predicate(labelId, String.valueOf(labelId)));
        return new Component(s, t, diameter, mask, cpq, normalized);
    }

    private static BitSet mask(int... edges) {
        BitSet mask = new BitSet();
        for (int edge : edges) {
            mask.set(edge);
        }
        return mask;
    }

    private static ToLongFunction<CPQ> cpqCostFn(Map<CPQ, Long> costs) {
        Map<CPQ, Long> byCpq = new HashMap<>(costs);
        return cpq -> byCpq.getOrDefault(cpq, Long.MAX_VALUE);
    }

    private record QueryFixture(ConjunctiveQuery query, VarCQ s, VarCQ t) {
    }

    private record PathQueryFixture(ConjunctiveQuery query, VarCQ x, VarCQ y, VarCQ z) {
    }
}
