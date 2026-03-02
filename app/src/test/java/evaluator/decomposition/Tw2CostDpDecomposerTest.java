package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;

class Tw2CostDpDecomposerTest {
    private static final String PATH_FOUR_EDGES =
            "(x0,x1) \u2190 0(x0,gen0), 1(gen0,gen1), 2(gen1,gen2), 3(gen2,x1)";
    private static final String FLOWER_DUPLICATE_INTERSECTION =
            "(x0,x1) \u2190 0(gen1,x0), 0(x1,x0), 0(gen2,x0), 0(gen0,gen1), 0(gen0,x0), 0(gen2,gen1)";
    private static final String K4_CLAUSE =
            "(x0) \u2190 0(x0,x1), 0(x0,x2), 0(x0,x3), 0(x1,x2), 0(x1,x3), 0(x2,x3)";

    @Test
    void tw2CostDpRejectsKLessThanOne() {
        assertThrows(IllegalArgumentException.class,
                () -> Decomposer.tw2CostDp(0, cpq -> 1L, cpq -> true));
    }

    @Test
    void tw2CostDpUsesJoinCostToPreferFewerComponentsOnTw2Query() {
        CQ cq = ConjunctiveQuery.parse(FLOWER_DUPLICATE_INTERSECTION).syntax();
        Plan series = Decomposer.seriesParallelGreedy(Tw2CostDpDecomposerTest::diameterTwoAndWidthTwo)
                .decompose(cq)
                .findFirst()
                .orElseThrow();
        Plan plan = Decomposer.tw2CostDp(2, cpq -> 100L, Tw2CostDpDecomposerTest::diameterTwoAndWidthTwo)
                .decompose(cq)
                .findFirst()
                .orElseThrow();

        assertEquals(3, series.components().size());
        assertEquals(2, plan.components().size());
    }

    @Test
    void tw2CostDpRespectsDiameterBoundOnPathQuery() {
        CQ cq = ConjunctiveQuery.parse(PATH_FOUR_EDGES).syntax();
        Plan plan = Decomposer.tw2CostDp(2, cpq -> 100L, cpq -> cpq.getDiameter() <= 2)
                .decompose(cq)
                .findFirst()
                .orElseThrow();

        assertEquals(2, plan.components().size());
    }

    @Test
    void tw2CostDpFallsBackToSeriesParallelWhenQueryWidthExceedsTwo() {
        CQ cq = ConjunctiveQuery.parse(K4_CLAUSE).syntax();
        Plan baseline = Decomposer.seriesParallelGreedy(cpq -> cpq.getDiameter() <= 2)
                .decompose(cq)
                .findFirst()
                .orElseThrow();
        Plan tw2 = Decomposer.tw2CostDp(2, cpq -> 100L, cpq -> cpq.getDiameter() <= 2)
                .decompose(cq)
                .findFirst()
                .orElseThrow();

        assertEquals(planSignature(baseline), planSignature(tw2));
    }

    private static boolean diameterTwoAndWidthTwo(CPQ cpq) {
        return cpq.getDiameter() <= 2 && maxIntersectionWidth(cpq.toAbstractSyntaxTree()) <= 2;
    }

    private static int maxIntersectionWidth(QueryTree node) {
        int max = 0;
        if (node.getOperation() == OperationType.INTERSECTION) {
            max = intersectionWidth(node);
        }
        int arity = node.getArity();
        for (int i = 0; i < arity; i++) {
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
                int arity = current.getArity();
                for (int i = 0; i < arity; i++) {
                    stack.push(current.getOperand(i));
                }
            } else {
                width++;
            }
        }
        return width;
    }

    private static String planSignature(Plan plan) {
        List<String> components = new ArrayList<>(plan.components().size());
        for (Component component : plan.components()) {
            components.add(component.sourceVarName()
                    + "->" + component.targetVarName()
                    + "|" + component.maskUnsafe()
                    + "|" + component.cpq());
        }
        components.sort(Comparator.naturalOrder());
        return String.join(";", components);
    }
}
