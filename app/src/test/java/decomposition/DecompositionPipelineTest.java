package decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.cpq.KnownComponent;
import decomposition.testing.TestDefaults;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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

    assertEquals(
        1, result.cpqPartitions().size(), "Single-edge query should have one valid partition");
    assertTrue(
        result.recognizedCatalogue().stream().anyMatch(kc -> "r1".equals(kc.cpqRule())),
        "Recognized catalogue should include the plain edge label");
    assertTrue(result.hasFinalComponent(), "Final CPQ should be available");
    assertEquals("r1", result.finalComponent().cpqRule(), "Single edge rule should match label");
    assertEquals(
        1, result.partitionEvaluations().size(), "Exactly one partition evaluation expected");
    assertEquals(
        1,
        result.partitionEvaluations().get(0).componentRuleCounts().size(),
        "Single component in the partition");
    assertTrue(
        result.partitionEvaluations().get(0).componentRuleCounts().get(0) >= 1,
        "Single component should have at least one construction rule");
  }

  @Test
  void exampleOnePartitionsAreRecognized() {
    CQ cq = Example.example1();

    DecompositionPipeline pipeline = new DecompositionPipeline();
    DecompositionOptions defaults = DecompositionOptions.defaults();
    DecompositionOptions options =
        new DecompositionOptions(
            DecompositionOptions.Mode.VALIDATE,
            defaults.maxPartitions(),
            defaults.maxCovers(),
            defaults.timeBudgetMs(),
            defaults.enumerationLimit(),
            TestDefaults.singleTuplePerPartition());

    DecompositionResult result = pipeline.execute(cq, Set.of(), options);

    assertEquals(
        12, result.filteredPartitionList().size(), "Example1 should yield 12 filtered partitions");
    assertEquals(
        12, result.cpqPartitions().size(), "All filtered partitions should be CPQ constructable");
    assertEquals(
        result.cpqPartitions().size(),
        result.partitionEvaluations().size(),
        "Evaluations should align with valid partitions");
  }

  @Test
  void exampleSevenSingleEdgePartitionProducesTuple() {
    CQ cq = Example.example7();

    DecompositionOptions defaults = DecompositionOptions.defaults();
    DecompositionOptions options =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            defaults.maxPartitions(),
            defaults.maxCovers(),
            defaults.timeBudgetMs(),
            defaults.enumerationLimit(),
            TestDefaults.singleTuplePerPartition());

    DecompositionPipeline pipeline = new DecompositionPipeline();
    DecompositionResult result = pipeline.execute(cq, Set.of("A"), options);

    Optional<PartitionEvaluation> singleEdgePartition =
        result.partitionEvaluations().stream()
            .filter(
                eval ->
                    eval.partition().components().stream()
                        .allMatch(component -> component.edgeCount() == 1))
            .findFirst();

    assertTrue(
        singleEdgePartition.isPresent(),
        "Example7 should contain a partition of single-edge components");
    PartitionEvaluation evaluation = singleEdgePartition.orElseThrow();

    assertTrue(
        evaluation.componentRuleCounts().stream().allMatch(count -> count > 0),
        "Every single-edge component must yield at least one construction rule");
    assertFalse(
        evaluation.decompositionTuples().isEmpty(),
        "Single-edge partition should produce at least one decomposition tuple");
    assertEquals(
        evaluation.partition().components().size(),
        evaluation.decompositionTuples().get(0).size(),
        "Tuple length should match the number of components");

    assertTrue(
        result.recognizedCatalogue().stream()
            .anyMatch(
                kc ->
                    kc.source().equals("C")
                        && kc.target().equals("C")
                        && kc.cpqRule().contains("∩ id")),
        () ->
            "Recognized catalogue should contain an anchored self-loop at C but had: "
                + result.recognizedCatalogue().stream()
                    .map(kc -> kc.cpqRule() + " [" + kc.source() + "→" + kc.target() + "]")
                    .collect(Collectors.joining(", ")));

    assertTrue(
        evaluation.decompositionTuples().stream()
            .flatMap(tuple -> tuple.stream())
            .anyMatch(
                kc ->
                    kc.source().equals("C")
                        && kc.target().equals("C")
                        && kc.cpqRule().contains("∩ id")),
        "Enumerated tuples should include the anchored C→C component");

    assertTrue(
        result.recognizedCatalogue().stream()
            .filter(kc -> kc.source().equals("C") && kc.target().equals("C"))
            .allMatch(kc -> kc.cpqRule().contains("∩ id")),
        "Self-loop catalogue entries must enforce equality via id");
  }

  @Test
  void singleComponentAcceptsReversedOrientationHomomorphism() {
    // Regression test: query graph homomorphism can work in either direction
    // The query (1⁻ ∩ ((2⁻ ∩ 3⁻)◦0⁻)) should find a single-component decomposition
    // even though its natural orientation (src→trg) is reversed from the free variable order
    // (src,trg)
    CQ cq = CQ.empty();
    VarCQ src = cq.addFreeVariable("src");
    VarCQ trg = cq.addFreeVariable("trg");
    VarCQ v0 = cq.addBoundVariable("0");

    cq.addAtom(v0, new Predicate(2, "2"), src);
    cq.addAtom(v0, new Predicate(3, "3"), src);
    cq.addAtom(trg, new Predicate(1, "1"), src);
    cq.addAtom(trg, new Predicate(0, "0"), v0);

    DecompositionOptions defaults = DecompositionOptions.defaults();
    DecompositionOptions options =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            defaults.maxPartitions(),
            defaults.maxCovers(),
            defaults.timeBudgetMs(),
            defaults.enumerationLimit(),
            TestDefaults.singleTuplePerPartition());

    DecompositionPipeline pipeline = new DecompositionPipeline();
    DecompositionResult result = pipeline.execute(cq, Set.of("src", "trg"), options);

    // Find the single-component partition
    Optional<PartitionEvaluation> singleComponent =
        result.partitionEvaluations().stream()
            .filter(eval -> eval.partition().components().size() == 1)
            .findFirst();

    assertTrue(
        singleComponent.isPresent(), "Should find a single-component partition for this query");

    PartitionEvaluation evaluation = singleComponent.orElseThrow();
    assertTrue(
        evaluation.componentRuleCounts().get(0) >= 1,
        "Single component should have at least one construction rule");

    assertFalse(
        evaluation.decompositionTuples().isEmpty(),
        "Single component should produce at least one decomposition tuple");

    // Verify the single component is recognized in the catalogue
    assertTrue(
        result.recognizedCatalogue().stream().anyMatch(kc -> kc.edges().cardinality() == 4),
        "Recognized catalogue should include the full 4-edge component");

    // Verify the decomposition is valid regardless of orientation
    KnownComponent fullComponent = evaluation.decompositionTuples().get(0).get(0);
    assertEquals(
        4, fullComponent.edges().cardinality(), "Single component should cover all 4 edges");

    // The component should match one of these patterns (forward or reversed):
    // (1 ∩ (0◦(2 ∩ 3))) from trg→src, or
    // (1⁻ ∩ ((2⁻ ∩ 3⁻)◦0⁻)) from src→trg
    String rule = fullComponent.cpqRule();
    boolean hasValidPattern =
        rule.contains("1")
            && rule.contains("0")
            && rule.contains("2")
            && rule.contains("3")
            && rule.contains("∩");
    assertTrue(
        hasValidPattern,
        "Component CPQ should contain all labels in intersection pattern, got: " + rule);
  }

  @Test
  void enumerationSingleTupleOptionLimitsResults() {
    CQ cq = Example.example7();

    DecompositionOptions defaults = DecompositionOptions.defaults();
    DecompositionOptions options =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            defaults.maxPartitions(),
            defaults.maxCovers(),
            defaults.timeBudgetMs(),
            defaults.enumerationLimit(),
            true);

    DecompositionPipeline pipeline = new DecompositionPipeline();
    DecompositionResult result = pipeline.execute(cq, Set.of("A"), options);

    for (PartitionEvaluation evaluation : result.partitionEvaluations()) {
      assertTrue(
          evaluation.decompositionTuples().size() <= 1,
          "Single-tuple mode should cap tuples to at most one per partition");
      if (!evaluation.decompositionTuples().isEmpty()) {
        assertEquals(
            evaluation.partition().components().size(),
            evaluation.decompositionTuples().get(0).size(),
            "First tuple should still cover all components");
      }
    }
  }
}
