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

  public void runBenchmark(CQ query, DecompositionResult result, Path graphPath, int indexK)
      throws IOException {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(graphPath, "graphPath");

    // 1. Setup
    int k = indexK > 0 ? indexK : 3;
    Timing indexTimer = Timing.start();
    Index index = indexManager.loadOrBuild(graphPath, k);
    long indexLoadMs = indexTimer.elapsedMillis();
    CpqIndexExecutor executor = new CpqIndexExecutor(index);

    // 2. Baseline
    // Note: Using a fresh runner helper or extracting logic to get components from atoms
    // In a full refactor, 'componentsFromAtoms' should be a static utility.
    List<CpqIndexExecutor.Component> baselineComponents =
        CpqIndexExecutor.componentsFromAtoms(query);

    LOG.info("Executing baseline...");
    Timing baselineTimer = Timing.start();
    List<Map<String, Integer>> baselineResults = executor.execute(baselineComponents);
    long baselineMs = baselineTimer.elapsedMillis();
    LOG.info("Baseline finished in {}ms. Results: {}", baselineMs, baselineResults.size());

    // 3. Decompositions
    long tupleTotalMs = 0L;
    Long firstTupleMs = null;
    for (Partition partition : result.cpqPartitions()) {
      PartitionEvaluation evaluation = findEvaluation(result, partition);
      if (evaluation == null || evaluation.decompositionTuples().isEmpty()) {
        continue;
      }

      if (evaluation.maxDiameter() > k) {
        LOG.info(
            "Skipping partition #{} (diameter {} > {})",
            evaluation.partitionIndex(),
            evaluation.maxDiameter(),
            k);
        continue;
      }

      int tupleIdx = 1;
      for (List<CPQExpression> tuple : evaluation.decompositionTuples()) {
        List<CpqIndexExecutor.Component> components = executor.componentsFromTuple(tuple);

        Timing tupleTimer = Timing.start();
        List<Map<String, Integer>> tupleResults = executor.execute(components);
        long elapsed = tupleTimer.elapsedMillis();
        tupleTotalMs += elapsed;
        if (firstTupleMs == null) {
          firstTupleMs = elapsed;
        }

        boolean match =
            DecompositionComparisonReporter.compare(
                baselineResults, tupleResults, result.freeVariables());
        LOG.info("Tuple #{} finished in {}ms. Matches baseline? {}", tupleIdx++, elapsed, match);
      }
    }

    LOG.info(
        "Evaluation timing (ms): indexLoad={}, baseline(single-edge atoms)={}, tuplesTotal={}, firstTuple={}",
        indexLoadMs,
        baselineMs,
        tupleTotalMs,
        firstTupleMs == null ? "n/a" : firstTupleMs);
  }

  private PartitionEvaluation findEvaluation(DecompositionResult result, Partition partition) {
    return result.partitionEvaluations().stream()
        .filter(e -> e.partition().equals(partition))
        .findFirst()
        .orElse(null);
  }
}
