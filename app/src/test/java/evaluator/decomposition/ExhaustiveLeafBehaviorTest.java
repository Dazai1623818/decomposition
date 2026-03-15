package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.CQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
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

    private static List<Integer> componentSizes(Plan plan) {
        return plan.components().stream()
                .map(component -> component.maskUnsafe().cardinality())
                .sorted()
                .toList();
    }
}
