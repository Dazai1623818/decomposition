package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import decomposition.core.Component;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class ComponentKeyTest {

  @Test
  void dedupeUsesOnlyEdgesAndEndpoints() {
    BitSet bits = new BitSet(3);
    bits.set(0);

    Component component = new Component(bits, Set.of("a", "b"), Set.of(), Map.of());
    CPQExpression first = new CPQExpression(CPQ.parse("0"), component, "a", "b", "first");
    CPQExpression second = new CPQExpression(CPQ.parse("0"), component, "a", "b", "second");

    List<CPQExpression> deduped =
        ComponentExpressionBuilder.dedupeExpressions(List.of(first, second));
    assertEquals(List.of(first), deduped, "Dedupe keeps first match for identical edge bits");
  }

  @Test
  void dedupeDistinguishesOrientation() {
    BitSet bits = new BitSet(3);
    bits.set(0);
    Component component = new Component(bits, Set.of("a", "b"), Set.of(), Map.of());
    CPQExpression forward = new CPQExpression(CPQ.parse("0"), component, "a", "b", "forward");
    CPQExpression reverse = new CPQExpression(CPQ.parse("0"), component, "b", "a", "reverse");

    List<CPQExpression> deduped =
        ComponentExpressionBuilder.dedupeExpressions(List.of(forward, reverse));
    assertEquals(2, deduped.size(), "Dedupe keeps distinct oriented endpoints");
  }
}
