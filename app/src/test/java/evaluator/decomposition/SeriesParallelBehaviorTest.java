package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.CQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import org.junit.jupiter.api.Test;

class SeriesParallelBehaviorTest {
    private static final String PATH_FOUR_EDGES =
            "(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)";

    @Test
    void seriesParallelGreedyCollapsesPathUnderSameDiameterFilter() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        Plan plan = Decomposer.seriesParallelGreedy(cpq -> cpq.getDiameter() <= 2)
                .decompose(cq)
                .findFirst()
                .orElseThrow();

        assertEquals(2, plan.components().size());
        assertTrue(plan.components().stream().allMatch(c -> c.maskUnsafe().cardinality() == 2));
    }

    @Test
    void seriesParallelGreedyWithoutFilterCollapsesPathToSingleComponent() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        Plan plan = Decomposer.seriesParallelGreedy()
                .decompose(cq)
                .findFirst()
                .orElseThrow();

        assertEquals(1, plan.components().size());
        assertEquals(4, plan.components().get(0).diameter());
    }

    @Test
    void seriesParallelCandidateSearchReturnsRankedUniqueCandidates() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        java.util.List<Plan> plans = Decomposer.seriesParallelCandidates(
                        8,
                        3,
                        42L,
                        cpq -> cpq.getDiameter() <= 2)
                .decompose(cq)
                .toList();

        assertTrue(plans.size() >= 1);
        assertTrue(plans.size() <= 3);
        assertEquals(2, plans.get(0).components().size());
    }

}
