package evaluator.decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import java.util.List;
import org.junit.jupiter.api.Test;

class ExhaustiveComponentEnumeratorTest {
    @Test
    void singleSelfLoopEnumeratesOneAtomicComponent() {
        ConjunctiveQuery query = selfLoopQuery();

        List<Component> components = new ExhaustiveComponentEnumerator(1).enumerate(query);

        assertEquals(1, components.size());
        assertTrue(components.get(0).isUnary());
        assertEquals(1, components.get(0).diameter());
        assertEquals(1, components.get(0).maskUnsafe().cardinality());
    }

    @Test
    void singleSelfLoopProducesOneExhaustiveDecomposition() {
        CQ cq = selfLoopQuery().syntax();

        List<Plan> plans = Decomposer.cpqkCoverExhaustive(1, 0).decompose(cq).toList();

        assertEquals(1, plans.size());
        assertEquals(1, plans.get(0).components().size());
        assertTrue(plans.get(0).components().get(0).isUnary());
    }

    @Test
    void intersectionOfTwoSelfLoopsStillProducesUnaryComponent() {
        ConjunctiveQuery query = sameVariableSelfLoopsQuery();

        List<Component> components = new ExhaustiveComponentEnumerator(1).enumerate(query);

        long unaryTwoEdgeComponents = components.stream()
                .filter(Component::isUnary)
                .filter(component -> component.maskUnsafe().cardinality() == 2)
                .filter(component -> component.diameter() == 1)
                .count();

        assertEquals(1L, unaryTwoEdgeComponents);
    }

    @Test
    void disablingIntersectionSkipsTwoEdgeSelfLoopConjunctionButKeepsUnaryIdentity() {
        ConjunctiveQuery query = sameVariableSelfLoopsQuery();

        List<Component> components = new ExhaustiveComponentEnumerator(1, null, Long.MAX_VALUE, false).enumerate(query);

        long unaryTwoEdgeComponents = components.stream()
                .filter(Component::isUnary)
                .filter(component -> component.maskUnsafe().cardinality() == 2)
                .filter(component -> component.diameter() == 1)
                .count();
        long unarySingleEdgeComponents = components.stream()
                .filter(Component::isUnary)
                .filter(component -> component.maskUnsafe().cardinality() == 1)
                .filter(component -> component.diameter() == 1)
                .count();

        assertEquals(0L, unaryTwoEdgeComponents);
        assertEquals(2L, unarySingleEdgeComponents);
    }

    @Test
    void concatenationCanCreateUnaryComponentForCycle() {
        ConjunctiveQuery query = twoEdgeCycleQuery();

        List<Component> components = new ExhaustiveComponentEnumerator(2).enumerate(query);

        long unaryTwoEdgeComponents = components.stream()
                .filter(Component::isUnary)
                .filter(component -> component.maskUnsafe().cardinality() == 2)
                .filter(component -> component.diameter() == 2)
                .count();

        assertTrue(unaryTwoEdgeComponents >= 1L);
    }

    private static ConjunctiveQuery selfLoopQuery() {
        CQ cq = CQ.empty();
        var x = cq.addBoundVariable("x");
        cq.addAtom(x, new Predicate(0, "0"), x);
        return ConjunctiveQuery.from(cq);
    }

    private static ConjunctiveQuery sameVariableSelfLoopsQuery() {
        CQ cq = CQ.empty();
        var x = cq.addBoundVariable("x");
        cq.addAtom(x, new Predicate(0, "0"), x);
        cq.addAtom(x, new Predicate(1, "1"), x);
        return ConjunctiveQuery.from(cq);
    }

    private static ConjunctiveQuery twoEdgeCycleQuery() {
        CQ cq = CQ.empty();
        var x = cq.addBoundVariable("x");
        var y = cq.addBoundVariable("y");
        cq.addAtom(x, new Predicate(0, "0"), y);
        cq.addAtom(y, new Predicate(1, "1"), x);
        return ConjunctiveQuery.from(cq);
    }
}
