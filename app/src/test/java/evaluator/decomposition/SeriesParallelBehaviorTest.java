package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import java.util.BitSet;
import java.util.ArrayDeque;
import java.util.List;
import org.junit.jupiter.api.Test;

class SeriesParallelBehaviorTest {
    private static final String PATH_FOUR_EDGES =
            "(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)";
    private static final String THREE_PARALLEL_EDGES =
            "(x0,x1) \u2190 0(x0,x1), 1(x0,x1), 2(x0,x1)";

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
                        3,
                        42L,
                        cpq -> cpq.getDiameter() <= 2)
                .decompose(cq)
                .toList();

        assertTrue(plans.size() >= 1);
        assertTrue(plans.size() <= 3);
        assertEquals(2, plans.get(0).components().size());
    }

    @Test
    void seriesParallelCandidateSearchWithoutRestartsReturnsSingleEdgePlan() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        java.util.List<Plan> plans = Decomposer.seriesParallelCandidates(0, 42L)
                .decompose(cq)
                .toList();

        assertEquals(1, plans.size());
        assertEquals(4, plans.get(0).components().size());
        assertTrue(plans.get(0).components().stream().allMatch(component -> component.diameter() == 1));
    }

    @Test
    void seriesParallelGreedyMergesParallelEdgesPairwiseUnderBinaryIntersectionFilter() {
        CQ cq = ConjunctiveQuery.parse(THREE_PARALLEL_EDGES).syntax();
        Plan plan = Decomposer.seriesParallelGreedy(SeriesParallelBehaviorTest::allowsBinaryIntersectionsOnly)
                .decompose(cq)
                .findFirst()
                .orElseThrow();

        assertEquals(2, plan.components().size());
        assertTrue(plan.components().stream().anyMatch(c -> c.maskUnsafe().cardinality() == 2));
    }

    @Test
    void seriesParallelCandidateSearchDeduplicatesAssociativePathVariants() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        java.util.List<Plan> plans = Decomposer.seriesParallelCandidates(64, 42L)
                .decompose(cq)
                .toList();

        assertEquals(2, plans.size());
        assertTrue(plans.stream().allMatch(plan -> plan.components().size() == 1));
    }

    @Test
    void seriesParallelCandidatesBreakCollapseTiesByCumulativeCost() {
        CQ cq = ConjunctiveQuery.parse(THREE_PARALLEL_EDGES).syntax();
        java.util.List<Plan> plans = Decomposer.seriesParallelCandidates(
                        64,
                        42L,
                        SeriesParallelBehaviorTest::componentCost,
                        SeriesParallelBehaviorTest::allowsBinaryIntersectionsOnly)
                .decompose(cq)
                .toList();

        assertEquals(3, plans.size());
        assertHasMergedMask(plans.get(0), 1, 2);
    }

    private static boolean allowsBinaryIntersectionsOnly(CPQ cpq) {
        return maxIntersectionWidth(cpq.toAbstractSyntaxTree()) <= 2;
    }

    private static void assertHasMergedMask(Plan plan, int first, int second) {
        assertTrue(plan.components().stream().anyMatch(component -> hasMergedMask(component.maskUnsafe(), first, second)));
    }

    private static boolean hasMergedMask(BitSet mask, int first, int second) {
        return mask.cardinality() == 2 && mask.get(first) && mask.get(second);
    }

    private static long componentCost(CPQ cpq) {
        BitSet labels = new BitSet();
        collectLabels(cpq.toAbstractSyntaxTree(), labels);
        if (labels.cardinality() == 1) {
            return 10L;
        }
        if (labels.cardinality() == 2) {
            if (labels.get(1) && labels.get(2)) {
                return 1L;
            }
            if (labels.get(0) && labels.get(2)) {
                return 25L;
            }
            if (labels.get(0) && labels.get(1)) {
                return 50L;
            }
        }
        return 100L;
    }

    private static void collectLabels(QueryTree node, BitSet labels) {
        if (node.getOperation() == OperationType.EDGE) {
            labels.set(node.getEdgeAtom().getLabel().getID());
        }
        for (int i = 0; i < node.getArity(); i++) {
            collectLabels(node.getOperand(i), labels);
        }
    }

    private static int maxIntersectionWidth(QueryTree node) {
        int max = 0;
        if (node.getOperation() == OperationType.INTERSECTION) {
            max = intersectionWidth(node);
        }
        for (int i = 0; i < node.getArity(); i++) {
            max = Math.max(max, maxIntersectionWidth(node.getOperand(i)));
        }
        return max;
    }

    private static int intersectionWidth(QueryTree node) {
        int width = 0;
        ArrayDeque<QueryTree> stack = new ArrayDeque<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            QueryTree current = stack.pop();
            if (current.getOperation() == OperationType.INTERSECTION) {
                for (int i = 0; i < current.getArity(); i++) {
                    stack.push(current.getOperand(i));
                }
            } else {
                width++;
            }
        }
        return width;
    }

}
