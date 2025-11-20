package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertFalse;

import decomposition.DecompositionOptions;
import decomposition.DecompositionResult;
import decomposition.Example;
import decomposition.PartitionEvaluation;
import decomposition.builder.CpqBuilder;
import decomposition.builder.CpqBuilderResult;
import decomposition.testing.TestDefaults;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class CPQTupleEnumerationExampleTest {

  @Test
  void printCartesianTupleExample() {
    CQ cq = Example.example7();

    DecompositionOptions defaults = DecompositionOptions.defaults();
    boolean singleTupleOnly = TestDefaults.singleTuplePerPartition();
    DecompositionOptions options =
        new DecompositionOptions(
            DecompositionOptions.Mode.ENUMERATE,
            defaults.maxPartitions(),
            defaults.timeBudgetMs(),
            defaults.enumerationLimit(),
            singleTupleOnly,
            false);

    CpqBuilderResult builderResult = CpqBuilder.defaultBuilder().build(cq, Set.of("A"), options);
    DecompositionResult result = builderResult.result();

    PartitionEvaluation evaluation =
        result.partitionEvaluations().stream()
            .filter(eval -> !eval.decompositionTuples().isEmpty())
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "Expected at least one partition with enumeration tuples for example7"));

    List<List<CPQExpression>> tuples = evaluation.decompositionTuples();
    assertFalse(
        tuples.isEmpty(), "Partition evaluation should produce at least one tuple for the example");

    System.out.println(
        "Partition #"
            + evaluation.partitionIndex()
            + " has "
            + tuples.size()
            + " tuples ("
            + evaluation.componentExpressionCounts()
            + ")");

    int examplesToShow = Math.min(tuples.size(), 5);
    for (int tupleIndex = 0; tupleIndex < examplesToShow; tupleIndex++) {
      List<CPQExpression> tuple = tuples.get(tupleIndex);
      System.out.println("Tuple " + (tupleIndex + 1) + "/" + tuples.size() + ":");
      for (int compIndex = 0; compIndex < tuple.size(); compIndex++) {
        CPQExpression component = tuple.get(compIndex);
        System.out.println(
            "  comp#"
                + (compIndex + 1)
                + " ["
                + component.source()
                + "→"
                + component.target()
                + "] "
                + component.cpqRule());
      }
    }
  }
}
