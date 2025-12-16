package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.decompose.Decomposer;
import decomposition.pipeline.Pipeline;
import decomposition.pipeline.PlanMode;
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

  private static final int ITERATIONS = 50;
  private static final int MAX_DEPTH = 5;
  private static final int LABEL_COUNT = 4;
  private static final List<Predicate> PREDICATE_ALPHABET = buildAlphabet(LABEL_COUNT);

  @Test
  void decompositionReconstructsOriginalCpq() {
    for (int i = 0; i < ITERATIONS; i++) {
      CPQ original = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
      CQ cq = original.toCQ();

      ReconstructionResult analysis = analyseReconstruction(original, cq, PREDICATE_ALPHABET);
      logReconstructions(i, original, analysis);
      assertTrue(
          analysis.isHomomorphic(),
          () ->
              "Reconstruction is not homomorphic to original; original="
                  + original
                  + " reconstructed="
                  + analysis.reconstructed().map(CPQ::toString).orElse("n/a")
                  + " candidates="
                  + summary(analysis.candidates()));
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

    List<List<CPQ>> decompositions =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);
    assertTrue(!decompositions.isEmpty(), "Loop CPQ should produce decompositions");
  }

  @Test
  void decompositionHandlesIntersectionCpq() {
    CPQ intersection = CPQ.parse("(0 ∩ 1)");
    CQ cq = intersection.toCQ();

    List<List<CPQ>> decompositions =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);
    assertTrue(!decompositions.isEmpty(), "Intersection CPQ should execute successfully");
  }

  @Test
  void decompositionWithTimeBudgetTerminatesEarly() {
    Pipeline pipeline = new Pipeline();
    DecompositionOptions timedOptions =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            100_000,
            1, // 1ms time budget - very tight
            100,
            false,
            PlanMode.ALL,
            0);

    CPQ complex = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
    CQ cq = complex.toCQ();
    Set<String> freeVars =
        cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());

    DecompositionResult result = pipeline.decompose(cq, freeVars, timedOptions);
    // Should complete without hanging, possibly with early termination
    assertTrue(
        result.terminationReason() == null || result.terminationReason().contains("time"),
        "Either completes normally or terminates due to time budget");
  }

  private static ReconstructionResult analyseReconstruction(
      CPQ original, CQ cq, List<Predicate> alphabet) {
    List<List<CPQ>> decompositions =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);

    boolean hasSingleComponent = decompositions.stream().anyMatch(decomp -> decomp.size() == 1);
    List<CPQ> candidates = decompositions.stream().flatMap(List::stream).toList();

    Optional<CPQ> matchingCandidate =
        candidates.stream()
            .filter(candidate -> areHomomorphicUnderAlphabet(original, candidate, alphabet))
            .findFirst();

    boolean homomorphic = matchingCandidate.isPresent();

    return new ReconstructionResult(
        hasSingleComponent,
        homomorphic,
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
      Optional<CPQ> reconstructed,
      List<CPQ> candidates) {}

  private static void logReconstructions(int iteration, CPQ original, ReconstructionResult result) {
    System.out.println(
        "[Reconstruct] iteration="
            + iteration
            + " original="
            + original
            + " hasSingleComponent="
            + result.hasSingleComponent()
            + " homomorphic="
            + result.isHomomorphic()
            + " reconstructed="
            + result.reconstructed().map(CPQ::toString).orElse("n/a")
            + " candidates="
            + result.candidates().size());

    System.out.println("Original CPQ: " + original);
    for (CPQ candidate : result.candidates()) {
      System.out.println("Reconstruction candidate: " + candidate);
    }
  }
}
