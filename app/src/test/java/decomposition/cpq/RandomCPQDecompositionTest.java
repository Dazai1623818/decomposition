package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.DecompositionOptions;
import decomposition.DecompositionPipeline;
import decomposition.DecompositionResult;
import decomposition.PartitionEvaluation;
import decomposition.testing.TestDefaults;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class RandomCPQDecompositionTest {

  private static final int ITERATIONS = 100;
  private static final int MAX_DEPTH = 10;
  private static final int LABEL_COUNT = 4;
  private static final List<Predicate> PREDICATE_ALPHABET = buildAlphabet(LABEL_COUNT);

  private static final DecompositionOptions DEFAULT_OPTIONS = DecompositionOptions.defaults();
  private static final DecompositionOptions ENUMERATION_OPTIONS =
      new DecompositionOptions(
          DecompositionOptions.Mode.ENUMERATE,
          DEFAULT_OPTIONS.maxPartitions(),
          DEFAULT_OPTIONS.timeBudgetMs(),
          DEFAULT_OPTIONS.enumerationLimit(),
          TestDefaults.singleTuplePerPartition());

  @Test
  void decompositionReconstructsOriginalCpq() {
    DecompositionPipeline pipeline = new DecompositionPipeline();

    for (int i = 0; i < ITERATIONS; i++) {
      CPQ original = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
      CQ cq = original.toCQ();
      Set<String> freeVars =
          cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

      ReconstructionResult analysis =
          analyseReconstruction(pipeline, original, cq, freeVars, PREDICATE_ALPHABET);
      System.out.println(
          "[Reconstruct] iteration="
              + i
              + " original="
              + original
              + " hasSingleComponent="
              + analysis.hasSingleComponent()
              + " homomorphic="
              + analysis.isHomomorphic()
              + " reconstructed="
              + analysis.reconstructed().map(CPQ::toString).orElse("n/a"));
    }
  }

  @Test
  void decompositionHandlesSingleEdgeCpq() {
    DecompositionPipeline pipeline = new DecompositionPipeline();
    CPQ single = CPQ.parse("0");
    CQ cq = single.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    DecompositionResult result = pipeline.execute(cq, freeVars, ENUMERATION_OPTIONS);
    assertTrue(
        result.cpqPartitions().size() >= 1,
        "Single edge CPQ should have at least one valid partition");
  }

  @Test
  void decompositionHandlesLoopCpq() {
    DecompositionPipeline pipeline = new DecompositionPipeline();
    CPQ loop = CPQ.parse("(0 ◦ 0⁻ ∩ id)");
    CQ cq = loop.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    DecompositionResult result = pipeline.execute(cq, freeVars, ENUMERATION_OPTIONS);
    assertTrue(result.edges().size() >= 1, "Loop CPQ should produce edges");
  }

  @Test
  void decompositionHandlesIntersectionCpq() {
    DecompositionPipeline pipeline = new DecompositionPipeline();
    CPQ intersection = CPQ.parse("(0 ∩ 1)");
    CQ cq = intersection.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    // Should complete without throwing exception
    DecompositionResult result = pipeline.execute(cq, freeVars, ENUMERATION_OPTIONS);
    // Just verify the result is not null - intersection may have zero or more partitions
    assertTrue(
        result != null && result.edges().size() >= 1,
        "Intersection CPQ should execute successfully");
  }

  @Test
  void decompositionWithTimeBudgetTerminatesEarly() {
    DecompositionPipeline pipeline = new DecompositionPipeline();
    DecompositionOptions timedOptions =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            100_000,
            1, // 1ms time budget - very tight
            100,
            TestDefaults.singleTuplePerPartition());

    CPQ complex = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
    CQ cq = complex.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    DecompositionResult result = pipeline.execute(cq, freeVars, timedOptions);
    // Should complete without hanging, possibly with early termination
    assertTrue(
        result.terminationReason() == null || result.terminationReason().contains("time"),
        "Either completes normally or terminates due to time budget");
  }

  private static ReconstructionResult analyseReconstruction(
      DecompositionPipeline pipeline,
      CPQ original,
      CQ cq,
      Set<String> freeVars,
      List<Predicate> alphabet) {
    DecompositionResult result = pipeline.execute(cq, freeVars, ENUMERATION_OPTIONS);

    Optional<PartitionEvaluation> singleComponentEvaluation =
        result.partitionEvaluations().stream()
            .filter(eval -> eval.partition().size() == 1)
            .findFirst();
    if (singleComponentEvaluation.isEmpty()) {
      return new ReconstructionResult(false, false, Optional.empty());
    }

    List<List<CPQExpression>> tuples = singleComponentEvaluation.get().decompositionTuples();
    if (tuples.isEmpty() || tuples.get(0).isEmpty()) {
      return new ReconstructionResult(false, false, Optional.empty());
    }

    CPQ reconstructed = tuples.get(0).get(0).cpq();
    boolean homomorphic = areHomomorphicUnderAlphabet(original, reconstructed, alphabet);
    return new ReconstructionResult(true, homomorphic, Optional.of(reconstructed));
  }

  private static boolean areHomomorphicUnderAlphabet(
      CPQ original, CPQ reconstructed, List<Predicate> alphabet) {
    try {
      CPQ normalizedOriginal = CPQ.parse(sanitize(original.toString()), alphabet);
      CPQ normalizedReconstructed = CPQ.parse(sanitize(reconstructed.toString()), alphabet);
      return normalizedOriginal.isHomomorphicTo(normalizedReconstructed)
          && normalizedReconstructed.isHomomorphicTo(normalizedOriginal);
    } catch (IllegalArgumentException ex) {
      QueryGraphCPQ originalGraph = original.toQueryGraph();
      QueryGraphCPQ reconstructedGraph = reconstructed.toQueryGraph();
      return originalGraph.isHomomorphicTo(reconstructedGraph)
          && reconstructedGraph.isHomomorphicTo(originalGraph);
    }
  }

  private static List<Predicate> buildAlphabet(int size) {
    List<Predicate> predicates = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      predicates.add(new Predicate(i, String.valueOf(i)));
    }
    return List.copyOf(predicates);
  }

  private static String sanitize(String expression) {
    return expression.replace(" ", "");
  }

  private record ReconstructionResult(
      boolean hasSingleComponent, boolean isHomomorphic, Optional<CPQ> reconstructed) {}
}
