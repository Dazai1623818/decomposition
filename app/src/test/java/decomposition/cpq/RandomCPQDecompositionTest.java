package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.core.CPQExpression;
import decomposition.decompose.Decomposer;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;
import org.junit.jupiter.api.Test;

final class RandomCPQDecompositionTest {

  private static final int ITERATIONS = 50;
  private static final int MAX_DEPTH = 5;
  private static final int LABEL_COUNT = 4;

  @Test
  void decompositionRunsOnRandomCpqs() {
    for (int i = 0; i < ITERATIONS; i++) {
      CPQ original = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
      CQ cq = original.toCQ();

      assertTrue(
          !Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION)
              .isEmpty(),
          () -> "Expected at least one decomposition for random CPQ: " + original);
    }
  }

  @Test
  void decompositionHandlesSingleEdgeCpq() {
    CPQ single = CPQ.parse("0");
    CQ cq = single.toCQ();

    assertTrue(
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION).stream()
            .anyMatch(list -> !list.isEmpty()),
        "Single edge CPQ should have at least one valid decomposition");
  }

  @Test
  void decompositionHandlesLoopCpq() {
    CPQ loop = CPQ.parse("(0 ◦ 0⁻ ∩ id)");
    CQ cq = loop.toCQ();

    List<List<CPQExpression>> decompositions =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);
    assertTrue(!decompositions.isEmpty(), "Loop CPQ should produce decompositions");
  }

  @Test
  void decompositionHandlesIntersectionCpq() {
    CPQ intersection = CPQ.parse("(0 ∩ 1)");
    CQ cq = intersection.toCQ();

    List<List<CPQExpression>> decompositions =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);
    assertTrue(!decompositions.isEmpty(), "Intersection CPQ should execute successfully");
  }
}
