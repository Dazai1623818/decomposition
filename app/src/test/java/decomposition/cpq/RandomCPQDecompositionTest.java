package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.pipeline.builder.CpqBuilder;
import decomposition.testing.TestDefaults;
import decomposition.util.BitsetUtils;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class RandomCPQDecompositionTest {

  private static final int ITERATIONS = 50;
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
          TestDefaults.singleTuplePerPartition(),
          false);

  @Test
  void decompositionReconstructsOriginalCpq() {
    CpqBuilder builder = CpqBuilder.defaultBuilder();

    for (int i = 0; i < ITERATIONS; i++) {
      CPQ original = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
      CQ cq = original.toCQ();
      Set<String> freeVars =
          cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

      ReconstructionResult analysis =
          analyseReconstruction(builder, original, cq, freeVars, PREDICATE_ALPHABET);
      System.out.println(
          "[Reconstruct] iteration="
              + i
              + " original="
              + original
              + " hasSingleComponent="
              + analysis.hasSingleComponent()
              + " homomorphic="
              + analysis.isHomomorphic()
              + " reversedHomomorphic="
              + analysis.isReversedHomomorphic()
              + " reconstructed="
              + analysis.reconstructed().map(CPQ::toString).orElse("n/a")
              + " candidates="
              + analysis.candidates().size());
      assertTrue(
          analysis.isHomomorphic(),
          () ->
              "Reconstruction is not homomorphic to original; reversedHomomorphic="
                  + analysis.isReversedHomomorphic()
                  + " original="
                  + original
                  + " reconstructed="
                  + analysis.reconstructed().map(CPQ::toString).orElse("n/a")
                  + " candidates="
                  + summary(analysis.candidates()));
    }
  }

  @Test
  void decompositionHandlesSingleEdgeCpq() {
    CpqBuilder builder = CpqBuilder.defaultBuilder();
    CPQ single = CPQ.parse("0");
    CQ cq = single.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    DecompositionResult result = builder.build(cq, freeVars, ENUMERATION_OPTIONS).result();
    assertTrue(
        result.cpqPartitions().size() >= 1,
        "Single edge CPQ should have at least one valid partition");
  }

  @Test
  void decompositionHandlesLoopCpq() {
    CpqBuilder builder = CpqBuilder.defaultBuilder();
    CPQ loop = CPQ.parse("(0 ◦ 0⁻ ∩ id)");
    CQ cq = loop.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    DecompositionResult result = builder.build(cq, freeVars, ENUMERATION_OPTIONS).result();
    assertTrue(result.edges().size() >= 1, "Loop CPQ should produce edges");
  }

  @Test
  void decompositionHandlesIntersectionCpq() {
    CpqBuilder builder = CpqBuilder.defaultBuilder();
    CPQ intersection = CPQ.parse("(0 ∩ 1)");
    CQ cq = intersection.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    // Should complete without throwing exception
    DecompositionResult result = builder.build(cq, freeVars, ENUMERATION_OPTIONS).result();
    // Just verify the result is not null - intersection may have zero or more
    // partitions
    assertTrue(
        result != null && result.edges().size() >= 1,
        "Intersection CPQ should execute successfully");
  }

  @Test
  void decompositionWithTimeBudgetTerminatesEarly() {
    CpqBuilder builder = CpqBuilder.defaultBuilder();
    DecompositionOptions timedOptions =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            100_000,
            1, // 1ms time budget - very tight
            100,
            TestDefaults.singleTuplePerPartition(),
            false);

    CPQ complex = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
    CQ cq = complex.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    DecompositionResult result = builder.build(cq, freeVars, timedOptions).result();
    // Should complete without hanging, possibly with early termination
    assertTrue(
        result.terminationReason() == null || result.terminationReason().contains("time"),
        "Either completes normally or terminates due to time budget");
  }

  private static ReconstructionResult analyseReconstruction(
      CpqBuilder builder, CPQ original, CQ cq, Set<String> freeVars, List<Predicate> alphabet) {
    DecompositionResult result = builder.build(cq, freeVars, ENUMERATION_OPTIONS).result();

    Optional<PartitionEvaluation> singleComponentEvaluation =
        result.partitionEvaluations().stream()
            .filter(eval -> eval.partition().size() == 1)
            .findFirst();
    if (singleComponentEvaluation.isEmpty()) {
      return new ReconstructionResult(false, false, false, Optional.empty(), List.of());
    }

    List<List<CPQExpression>> tuples = singleComponentEvaluation.get().decompositionTuples();
    if (tuples.isEmpty() || tuples.get(0).isEmpty()) {
      return new ReconstructionResult(false, false, false, Optional.empty(), List.of());
    }

    BitSet fullEdgeMask = BitsetUtils.allOnes(result.edges().size());
    List<CPQExpression> fullCoverageCandidates =
        Stream.concat(result.recognizedCatalogue().stream(), tuples.stream().flatMap(List::stream))
            // Only consider expressions that actually cover every edge of the query graph
            .filter(candidate -> candidate.edges().equals(fullEdgeMask))
            .toList();

    List<CPQ> candidates = fullCoverageCandidates.stream().map(CPQExpression::cpq).toList();

    Optional<CPQ> matchingCandidate =
        candidates.stream()
            .filter(candidate -> areHomomorphicUnderAlphabet(original, candidate, alphabet))
            .findFirst();

    boolean homomorphic = matchingCandidate.isPresent();
    boolean reversedHomomorphic =
        candidates.stream().anyMatch(candidate -> areReversedHomomorphic(original, candidate));

    return new ReconstructionResult(
        true,
        homomorphic,
        reversedHomomorphic,
        matchingCandidate.or(() -> candidates.stream().findFirst()),
        candidates);
  }

  private static String summary(List<CPQ> candidates) {
    int max = Math.min(5, candidates.size());
    List<String> preview = candidates.subList(0, max).stream().map(CPQ::toString).toList();
    return preview + (candidates.size() > max ? "..." : "");
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
      return areMutuallyHomomorphic(originalGraph, reconstructedGraph);
    }
  }

  private static boolean areReversedHomomorphic(CPQ original, CPQ reconstructed) {
    QueryGraphCPQ originalGraph = original.toQueryGraph();
    QueryGraphCPQ reconstructedGraph = reconstructed.toQueryGraph();

    QueryGraphCPQ reconstructedReversed = reconstructed.toQueryGraph();
    reconstructedReversed.reverse();
    QueryGraphCPQ originalReversed = original.toQueryGraph();
    originalReversed.reverse();

    return areMutuallyHomomorphic(originalGraph, reconstructedReversed)
        || areMutuallyHomomorphic(originalReversed, reconstructedGraph);
  }

  private static boolean areMutuallyHomomorphic(QueryGraphCPQ left, QueryGraphCPQ right) {
    return left.isHomomorphicTo(right) && right.isHomomorphicTo(left);
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
      boolean hasSingleComponent,
      boolean isHomomorphic,
      boolean isReversedHomomorphic,
      Optional<CPQ> reconstructed,
      List<CPQ> candidates) {}
}
