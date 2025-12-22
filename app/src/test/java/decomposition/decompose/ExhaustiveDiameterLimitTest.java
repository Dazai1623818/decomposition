package decomposition.decompose;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.Example;
import decomposition.core.CPQExpression;
import decomposition.eval.EvaluationRun;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;
import org.junit.jupiter.api.Test;

final class ExhaustiveDiameterLimitTest {

  @Test
  void exhaustiveRespectsDiameterCap() {
    CQ cq = Example.example1();

    EvaluationRun unbounded =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);
    EvaluationRun bounded =
        Decomposer.decomposeWithRun(
            cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION, 1, 0);

    assertFalse(bounded.decompositions().isEmpty(), "Expected decompositions with k=1");
    assertTrue(
        bounded.size() <= unbounded.size(), "k-limited enumeration should not add decompositions");

    for (List<CPQExpression> tuple : bounded.decompositions()) {
      for (CPQExpression expr : tuple) {
        assertTrue(
            expr.cpq().getDiameter() <= 1,
            () -> "Found diameter " + expr.cpq().getDiameter() + " in " + expr.cpq());
      }
    }
  }
}
