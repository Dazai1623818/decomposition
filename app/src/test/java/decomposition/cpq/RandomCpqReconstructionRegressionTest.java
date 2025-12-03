package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.pipeline.Pipeline;
import decomposition.pipeline.PlanMode;
import decomposition.pipeline.extract.CQExtractor;
import decomposition.pipeline.extract.CQExtractor.ExtractionResult;
import decomposition.util.JoinAnalysisBuilder;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Regression checks for reconstructing a CPQ from its CQ expansion. Certain randomly generated CPQs
 * used to reconstruct homomorphically but recently drifted.
 */
final class RandomCpqReconstructionRegressionTest {

  private static final String REGRESSION_EXPRESSION = "(((id ∩ 1⁻)◦((2⁻◦0) ∩ 0))◦(3◦0⁻))";

  @Test
  void cpqFromRegressionExampleIsReconstructed() {
    CPQ original = CPQ.parse(REGRESSION_EXPRESSION);
    CQ cq = original.toCQ();
    Set<String> freeVars = freeVarsOf(cq);

    DecompositionOptions defaults = DecompositionOptions.defaults();
    DecompositionOptions options =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            defaults.maxPartitions(),
            defaults.timeBudgetMs(),
            defaults.tupleLimit() > 0 ? defaults.tupleLimit() : 1,
            false,
            PlanMode.ALL,
            0);

    Pipeline pipeline = new Pipeline();
    DecompositionResult result = pipeline.decompose(cq, freeVars, options);

    PartitionEvaluation single =
        result.partitionEvaluations().stream()
            .filter(eval -> eval.partition().size() == 1)
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "Expected a single-component partition evaluation for regression CPQ"));

    List<CPQ> reconstructed =
        single.decompositionTuples().stream()
            .flatMap(List::stream)
            .map(CPQExpression::cpq)
            .toList();

    ExtractionResult extraction = new CQExtractor().extract(cq, freeVars);
    BitSet fullBits = new BitSet(extraction.edges().size());
    fullBits.set(0, extraction.edges().size());
    Set<String> joinNodes =
        JoinAnalysisBuilder.analyzePartition(single.partition(), freeVars).globalJoinNodes();
    List<CPQExpression> rawExpressions =
        new ComponentExpressionBuilder(extraction.edges())
            .build(fullBits, joinNodes, extraction.variableNodeMap());

    List<String> reconstructedStrings =
        reconstructed.stream().map(CPQ::toString).collect(Collectors.toList());
    List<String> rawStrings =
        rawExpressions.stream().map(expr -> expr.cpq().toString()).collect(Collectors.toList());
    List<String> rawHomomorphic =
        rawExpressions.stream()
            .filter(expr -> areHomomorphic(original, expr.cpq()))
            .map(expr -> expr.cpq().toString())
            .collect(Collectors.toList());

    assertTrue(
        !rawHomomorphic.isEmpty(),
        () -> "Component builder should generate a homomorphic expression; raw=" + rawStrings);
    assertTrue(
        reconstructed.stream().anyMatch(candidate -> areHomomorphic(original, candidate)),
        () ->
            "Expected at least one homomorphic reconstruction. Candidates: "
                + reconstructedStrings
                + " :: raw="
                + rawStrings
                + " :: homomorphicRaw="
                + rawHomomorphic);
  }

  private static Set<String> freeVarsOf(CQ cq) {
    return cq.getFreeVariables().stream()
        .map(VarCQ::getName)
        .collect(Collectors.toCollection(HashSet::new));
  }

  private static boolean areHomomorphic(CPQ original, CPQ reconstructed) {
    List<Predicate> alphabet = buildAlphabet(4); // matches RandomCPQDecompositionTest
    try {
      CPQ normalizedOriginal = CPQ.parse(original.toString().replace(" ", ""), alphabet);
      CPQ normalizedReconstructed = CPQ.parse(reconstructed.toString().replace(" ", ""), alphabet);
      return homomorphicBothWays(normalizedOriginal, normalizedReconstructed);
    } catch (IllegalArgumentException ex) {
      return homomorphicBothWays(original, reconstructed);
    }
  }

  private static boolean homomorphicBothWays(CPQ a, CPQ b) {
    return a.toQueryGraph().isHomomorphicTo(b.toQueryGraph())
        && b.toQueryGraph().isHomomorphicTo(a.toQueryGraph());
  }

  private static List<Predicate> buildAlphabet(int size) {
    List<Predicate> predicates = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      predicates.add(new Predicate(i, String.valueOf(i)));
    }
    return List.copyOf(predicates);
  }
}
