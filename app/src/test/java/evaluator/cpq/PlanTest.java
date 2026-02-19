package evaluator.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.Plan.Component;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class PlanTest {
    @Test
    void variableOrderIsStableWhenCallerMutatesInputList() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addBoundVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        VarCQ z = cq.addBoundVariable("z");

        cq.addAtom(x, new Predicate(0, "0"), y);
        cq.addAtom(y, new Predicate(1, "1"), z);

        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        ArrayList<Component> input = new ArrayList<>();
        input.add(component(x, y, 0, "c0"));
        input.add(component(y, z, 1, "c1"));

        Plan decomposition = new Plan(query, input);
        List<String> baselineOrder = decomposition.variableOrder();
        List<Component> baselineComponents = decomposition.components();

        input.clear();
        input.add(component(x, x, 0, "mutated"));

        assertEquals(baselineOrder, decomposition.variableOrder());
        assertEquals(baselineComponents, decomposition.components());
        assertEquals(List.of("?y", "?x", "?z"), decomposition.variableOrder());
        assertEquals(List.of(), decomposition.projectedVariableNames());
    }

    @Test
    void projectedVariableNamesAreSortedAndStable() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addFreeVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        VarCQ z = cq.addFreeVariable("z");
        cq.addAtom(x, new Predicate(0, "0"), y);
        cq.addAtom(y, new Predicate(1, "1"), z);

        ConjunctiveQuery query = ConjunctiveQuery.from(cq);
        List<Component> components = List.of(
                component(x, y, 0, "c0"),
                component(y, z, 1, "c1"));
        Plan decomposition = new Plan(query, components);

        assertEquals(List.of("?x", "?z"), decomposition.projectedVariableNames());
    }

    private static Component component(VarCQ source, VarCQ target, int edgeIndex, String normalized) {
        BitSet mask = new BitSet();
        mask.set(edgeIndex);
        CPQ cpq = CPQ.label(new Predicate(edgeIndex, String.valueOf(edgeIndex)));
        return new Component(source, target, 1, mask, cpq, normalized);
    }
}
