package decomposition.pipeline.extract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.pipeline.extract.CQExtractor.ExtractionResult;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import org.junit.jupiter.api.Test;

final class CQExtractorTest {

  @Test
  void extractsEdgesAndFreeVariables() {
    CQ cq = CQ.empty();
    VarCQ x = cq.addFreeVariable("x");
    VarCQ y = cq.addBoundVariable("y");
    VarCQ z = cq.addBoundVariable("z");

    cq.addAtom(x, new Predicate(1, "r1"), y);
    cq.addAtom(y, new Predicate(2, "r2"), z);

    CQExtractor extractor = new CQExtractor();
    ExtractionResult result = extractor.extract(cq, null);

    assertEquals(2, result.edges().size(), "Expected two edges");
    assertTrue(result.freeVariables().contains("x"), "Free variables should include x");
  }
}
