package decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import dev.roanh.gmark.lang.cq.CQ;
import org.junit.jupiter.api.Test;

final class RandomExampleTest {

  @Test
  void randomExampleRespectsConfigCounts() {
    RandomExampleConfig config = new RandomExampleConfig(2, 3, 7, 4, 123L);
    CQ cq = Example.random(config);

    assertEquals(
        config.boundVariableCount(),
        cq.getBoundVariables().size(),
        "bound variable count mismatch");
    assertEquals(
        config.freeVariableCount(), cq.getFreeVariables().size(), "free variable count mismatch");
    assertEquals(config.edgeCount(), cq.toQueryGraph().getEdgeCount(), "edge count mismatch");
  }

  @Test
  void randomExampleDeterministicForSeed() {
    RandomExampleConfig config = new RandomExampleConfig(1, 2, 5, 3, 42L);
    CQ first = Example.random(config);
    CQ second = Example.random(config);
    RandomExampleConfig otherSeed = new RandomExampleConfig(1, 2, 5, 3, 99L);
    CQ different = Example.random(otherSeed);

    assertEquals(
        first.toFormalSyntax(), second.toFormalSyntax(), "generated CQ should repeat with seed");
    assertNotEquals(
        first.toFormalSyntax(),
        different.toFormalSyntax(),
        "different seeds should yield different CQ");
  }
}
