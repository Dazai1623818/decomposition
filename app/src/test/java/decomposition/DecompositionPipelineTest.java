package decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class DecompositionPipelineTest {

    @Test
    void singleEdgeQueryProducesSimpleRule() {
        CQ cq = CQ.empty();
        VarCQ x = cq.addFreeVariable("x");
        VarCQ y = cq.addBoundVariable("y");
        cq.addAtom(x, new Predicate(1, "r1"), y);

        DecompositionPipeline pipeline = new DecompositionPipeline();
        DecompositionResult result = pipeline.execute(cq, Set.of(), DecompositionOptions.defaults());

        assertEquals(1, result.cpqPartitions().size(), "Single-edge query should have one valid partition");
        assertTrue(result.recognizedCatalogue().stream().anyMatch(kc -> "r1".equals(kc.cpqRule())),
                "Recognized catalogue should include the plain edge label");
        assertTrue(result.hasFinalComponent(), "Final CPQ should be available");
        assertEquals("r1", result.finalComponent().cpqRule(), "Single edge rule should match label");
        assertEquals(1, result.partitionEvaluations().size(), "Exactly one partition evaluation expected");
        assertEquals(1, result.partitionEvaluations().get(0).componentOptionCounts().size(),
                "Single component in the partition");
        assertTrue(result.partitionEvaluations().get(0).componentOptionCounts().get(0) >= 1,
                "Single component should have at least one CPQ option");
    }

    @Test
    void exampleOnePartitionsAreRecognized() {
        CQ cq = Example.example1();

        DecompositionPipeline pipeline = new DecompositionPipeline();
        DecompositionOptions defaults = DecompositionOptions.defaults();
        DecompositionOptions options = new DecompositionOptions(
                DecompositionOptions.Mode.VALIDATE,
                defaults.maxPartitions(),
                defaults.maxCovers(),
                defaults.timeBudgetMs(),
                defaults.enumerationLimit());

        DecompositionResult result = pipeline.execute(cq, Set.of(), options);

        assertEquals(12, result.filteredPartitionList().size(), "Example1 should yield 12 filtered partitions");
        assertEquals(12, result.cpqPartitions().size(), "All filtered partitions should be CPQ constructable");
        assertEquals(result.cpqPartitions().size(), result.partitionEvaluations().size(),
                "Evaluations should align with valid partitions");
    }
}
