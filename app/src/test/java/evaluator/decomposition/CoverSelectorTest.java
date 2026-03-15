package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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

        CoverSelector selector = new CoverSelector(0, CoverSelector.Order.COST, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(lowDiamCheap, highDiamExpensive)).toList();

        assertFalse(covers.isEmpty());
        assertEquals("low-diam", covers.get(0).components().get(0).normalized());
    }

    @Test
    void maxCollapsePrefersHigherDiameterBeforeLowerCost() {
        QueryFixture fixture = singleEdgeQuery();
        Component highDiamExpensive = component(fixture, "high-diam", 20, 5);
        Component lowDiamCheap = component(fixture, "low-diam", 21, 2);

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                highDiamExpensive.cpq(), 100L,
                lowDiamCheap.cpq(), 1L));

        CoverSelector selector = new CoverSelector(0, CoverSelector.Order.MAX_COLLAPSE, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(lowDiamCheap, highDiamExpensive)).toList();

        assertFalse(covers.isEmpty());
        assertEquals("high-diam", covers.get(0).components().get(0).normalized());
    }

    @Test
    void costOrderUsesSummedCoverCostInsteadOfTraversalOrder() {
        ThreeEdgePathQueryFixture fixture = threeEdgePathQuery();
        Component singleLeft = component(
                fixture.x(),
                fixture.y(),
                "single-left",
                25,
                1,
                mask(0));
        Component singleMiddle = component(
                fixture.y(),
                fixture.z(),
                "single-middle",
                26,
                1,
                mask(1));
        Component singleRight = component(
                fixture.z(),
                fixture.w(),
                "single-right",
                27,
                1,
                mask(2));
        Component leftPair = component(
                fixture.x(),
                fixture.z(),
                "left-pair",
                28,
                2,
                mask(0, 1));

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                singleLeft.cpq(), 1L,
                singleMiddle.cpq(), 10L,
                singleRight.cpq(), 1L,
                leftPair.cpq(), 5L));

        CoverSelector selector = new CoverSelector(0, CoverSelector.Order.COST, costFn);
        List<Plan> covers = selector.select(
                fixture.query(),
                List.of(singleLeft, singleMiddle, singleRight, leftPair)).toList();

        assertFalse(covers.isEmpty());
        assertEquals(
                Set.of("left-pair", "single-right"),
                covers.get(0).components().stream().map(Component::normalized).collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void maxCollapsePrefersFewerComponentsBeforeLowerCost() {
        PathQueryFixture fixture = twoEdgePathQuery();
        Component twoEdge = component(
                fixture.x(),
                fixture.z(),
                "two-edge",
                29,
                2,
                mask(0, 1));
        Component left = component(
                fixture.x(),
                fixture.y(),
                "left",
                30,
                1,
                mask(0));
        Component right = component(
                fixture.y(),
                fixture.z(),
                "right",
                31,
                1,
                mask(1));

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                twoEdge.cpq(), 100L,
                left.cpq(), 1L,
                right.cpq(), 1L));

        CoverSelector selector = new CoverSelector(0, CoverSelector.Order.MAX_COLLAPSE, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(twoEdge, left, right)).toList();

        assertFalse(covers.isEmpty());
        assertEquals("two-edge", covers.get(0).components().get(0).normalized());
    }

    @Test
    void maxCollapsePrefersLargerComponentBeforeLowerCost() {
        FourEdgePathQueryFixture fixture = fourEdgePathQuery();
        Component triple = component(
                fixture.x(),
                fixture.w(),
                "triple",
                32,
                3,
                mask(0, 1, 2));
        Component tail = component(
                fixture.w(),
                fixture.v(),
                "tail",
                33,
                1,
                mask(3));
        Component leftPair = component(
                fixture.x(),
                fixture.z(),
                "left-pair",
                34,
                2,
                mask(0, 1));
        Component rightPair = component(
                fixture.z(),
                fixture.v(),
                "right-pair",
                35,
                2,
                mask(2, 3));

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                triple.cpq(), 100L,
                tail.cpq(), 1L,
                leftPair.cpq(), 1L,
                rightPair.cpq(), 1L));

        CoverSelector selector = new CoverSelector(0, CoverSelector.Order.MAX_COLLAPSE, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(triple, tail, leftPair, rightPair)).toList();

        assertFalse(covers.isEmpty());
        assertEquals(
                Set.of("triple", "tail"),
                covers.get(0).components().stream().map(Component::normalized).collect(java.util.stream.Collectors.toSet()));
    }

    @Test
    void maxCollapseBreaksExactStructuralTiesByLowerTotalCost() {
        QueryFixture fixture = singleEdgeQuery();
        Component expensive = component(fixture, "expensive", 36, 4);
        Component cheap = component(fixture, "cheap", 37, 4);

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                expensive.cpq(), 10L,
                cheap.cpq(), 1L));

        CoverSelector selector = new CoverSelector(0, CoverSelector.Order.MAX_COLLAPSE, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(expensive, cheap)).toList();

        assertFalse(covers.isEmpty());
        assertEquals("cheap", covers.get(0).components().get(0).normalized());
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
    void generatedCoverLimitAppliesBeforeRanking() {
        QueryFixture fixture = singleEdgeQuery();
        Component firstGenerated = component(fixture, "first", 50, 5);
        Component cheaperAlternative = component(fixture, "cheaper", 51, 1);

        ToLongFunction<CPQ> costFn = cpqCostFn(Map.of(
                firstGenerated.cpq(), 100L,
                cheaperAlternative.cpq(), 1L));

        CoverSelector selector = new CoverSelector(1, CoverSelector.Order.COST, costFn);
        List<Plan> covers = selector.select(fixture.query(), List.of(firstGenerated, cheaperAlternative)).toList();

        assertEquals(1, covers.size());
        assertEquals("first", covers.get(0).components().get(0).normalized());
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

    private static ThreeEdgePathQueryFixture threeEdgePathQuery() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addBoundVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        VarCQ z = cq.addBoundVariable("z");
        VarCQ w = cq.addBoundVariable("w");
        cq.addAtom(x, new Predicate(0, "0"), y);
        cq.addAtom(y, new Predicate(1, "1"), z);
        cq.addAtom(z, new Predicate(2, "2"), w);
        return new ThreeEdgePathQueryFixture(ConjunctiveQuery.from(cq), x, y, z, w);
    }

    private static FourEdgePathQueryFixture fourEdgePathQuery() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addBoundVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        VarCQ z = cq.addBoundVariable("z");
        VarCQ w = cq.addBoundVariable("w");
        VarCQ v = cq.addBoundVariable("v");
        cq.addAtom(x, new Predicate(0, "0"), y);
        cq.addAtom(y, new Predicate(1, "1"), z);
        cq.addAtom(z, new Predicate(2, "2"), w);
        cq.addAtom(w, new Predicate(3, "3"), v);
        return new FourEdgePathQueryFixture(ConjunctiveQuery.from(cq), x, y, z, w, v);
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

    private record ThreeEdgePathQueryFixture(ConjunctiveQuery query, VarCQ x, VarCQ y, VarCQ z, VarCQ w) {
    }

    private record FourEdgePathQueryFixture(ConjunctiveQuery query, VarCQ x, VarCQ y, VarCQ z, VarCQ w, VarCQ v) {
    }
}
