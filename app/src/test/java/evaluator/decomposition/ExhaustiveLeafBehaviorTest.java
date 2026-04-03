package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ExhaustiveLeafBehaviorTest {
    private static final String PATH_FOUR_EDGES =
            "(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)";

    @Test
    void terminalLeafCoverSearchKeepsOnlyIrreduciblePathCoversAtDiameterTwo() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        List<Plan> plans = Decomposer.cpqkCoverTerminalLeaves(
                        2,
                        0,
                        cpq -> 0L,
                        cpq -> cpq.getDiameter() <= 2)
                .decompose(cq)
                .toList();

        assertEquals(
                Set.of(List.of(1, 1, 2), List.of(2, 2)),
                plans.stream().map(ExhaustiveLeafBehaviorTest::componentSizes).collect(Collectors.toSet()));
        assertTrue(plans.stream().allMatch(plan -> TerminalLeafFilter.isTerminalLeaf(plan, cpq -> cpq.getDiameter() <= 2)));
        assertFalse(TerminalLeafFilter.isTerminalLeaf(ConjunctiveQuery.parse(PATH_FOUR_EDGES).decomposeSingleEdge(), cpq -> true));
    }

    @Test
    void terminalLeafCoverSearchKeepsBothFullPathEndpointOrientations() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        List<Plan> plans = Decomposer.cpqkCoverTerminalLeaves(
                        4,
                        0,
                        cpq -> 0L,
                        null)
                .decompose(cq)
                .toList();

        assertEquals(2, plans.size());
        assertTrue(plans.stream().allMatch(plan -> componentSizes(plan).equals(List.of(4))));
    }

    @Test
    void precomputedTerminalLeafFilterMatchesLegacyCheck() {
        assertPrecomputedMatchesLegacy(PATH_FOUR_EDGES, cpq -> cpq.getDiameter() <= 2, 2);
        assertPrecomputedMatchesLegacy(
                "(x0,x1) \u2190 0(x0,gen0), 1(gen0,x1), 2(x0,gen1), 3(gen1,x1)",
                cpq -> cpq.getDiameter() <= 2,
                2);
    }

    private static List<Integer> componentSizes(Plan plan) {
        return plan.components().stream()
                .map(component -> component.maskUnsafe().cardinality())
                .sorted()
                .toList();
    }

    private static void assertPrecomputedMatchesLegacy(
            String queryText,
            java.util.function.Predicate<CPQ> componentFilter,
            int k) {
        ConjunctiveQuery query = ConjunctiveQuery.parse(queryText);
        List<Component> components = new ExhaustiveComponentEnumerator(k, componentFilter, Long.MAX_VALUE)
                .enumerate(query);
        java.util.function.Predicate<Plan> precomputed = TerminalLeafFilter.precomputed(
                components,
                query.freeVariables(),
                componentFilter);
        List<Plan> covers = new ArrayList<>(new CoverSelector(0, CoverSelector.Order.COST, cpq -> 0L)
                .select(query, components)
                .toList());

        covers.add(query.decomposeSingleEdge());
        assertTrue(covers.stream().allMatch(plan -> precomputed.test(plan) == TerminalLeafFilter.isTerminalLeaf(
                plan,
                componentFilter)));
    }
}
