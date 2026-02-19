package evaluator.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.Plan.Component;
import java.util.BitSet;
import org.junit.jupiter.api.Test;

class PlanComponentTest {
    @Test
    void maskReturnsDefensiveCopy() {
        Component component = componentFixture();

        int hashBefore = component.hashCode();
        BitSet leaked = component.mask();
        leaked.clear(0);

        assertTrue(component.mask().get(0));
        assertEquals(hashBefore, component.hashCode());
    }

    private static Component componentFixture() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addBoundVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        BitSet mask = new BitSet();
        mask.set(0);
        return new Component(
                x,
                y,
                1,
                mask,
                CPQ.label(new Predicate(1, "1")),
                "edge");
    }
}
