package decomposition.eval;

import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.util.Timing;
import dev.roanh.cpqindex.Index;
import dev.roanh.gmark.lang.cq.CQ;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** High-level orchestration for evaluating query decompositions against a baseline. */
public final class EvaluationPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(EvaluationPipeline.class);
  private final IndexManager indexManager = new IndexManager();

  public void runBenchmark(CQ query, DecompositionResult result, Path graphPath)
      throws IOException {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(graphPath, "graphPath");

    // 1. Setup
    Index index = indexManager.loadOrBuild(graphPath, 3);
    CpqIndexExecutor executor = new CpqIndexExecutor(index);

    // 2. Baseline
    // Note: Using a fresh runner helper or extracting logic to get components from atoms
    // In a full refactor, 'componentsFromAtoms' should be a static utility.
    List<CpqIndexExecutor.Component> baselineComponents =
        new QueryEvaluationRunner().componentsFromAtomsPublic(query);

    LOG.info("Executing baseline...");
    Timing baselineTimer = Timing.start();
    List<Map<String, Integer>> baselineResults = executor.execute(baselineComponents);
    LOG.info(
        "Baseline finished in {}ms. Results: {}",
        baselineTimer.elapsedMillis(),
        baselineResults.size());

    // 3. Decompositions
    for (Partition partition : result.cpqPartitions()) {
      PartitionEvaluation evaluation = findEvaluation(result, partition);
      if (evaluation == null || evaluation.decompositionTuples().isEmpty()) {
        continue;
      }

      int tupleIdx = 1;
      for (List<CPQExpression> tuple : evaluation.decompositionTuples()) {
        List<CpqIndexExecutor.Component> components = executor.componentsFromTuple(tuple);

        Timing tupleTimer = Timing.start();
        List<Map<String, Integer>> tupleResults = executor.execute(components);
        long elapsed = tupleTimer.elapsedMillis();

        boolean match =
            DecompositionComparisonReporter.compare(
                baselineResults, tupleResults, result.freeVariables());
        LOG.info("Tuple #{} finished in {}ms. Matches baseline? {}", tupleIdx++, elapsed, match);
      }
    }
  }

  private PartitionEvaluation findEvaluation(DecompositionResult result, Partition partition) {
    return result.partitionEvaluations().stream()
        .filter(e -> e.partition().equals(partition))
        .findFirst()
        .orElse(null);
  }
}
