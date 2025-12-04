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

  /**
   * Executes baseline and decomposition tuple evaluation against the CPQ index.
   *
   * @return timing metrics (index load is reported but excluded from aggregates)
   */
  public EvaluationMetrics runBenchmark(
      CQ query, DecompositionResult result, Path graphPath, int indexK) throws IOException {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(graphPath, "graphPath");

    // 1. Setup
    int k = indexK > 0 ? indexK : 2;
    Timing indexTimer = Timing.start();
    Index index = indexManager.loadOrBuild(graphPath, k);
    long indexLoadMs = indexTimer.elapsedMillis();
    return runBenchmarkWithIndex(query, result, index, k, indexLoadMs);
  }

  /**
   * Executes baseline and decomposition tuple evaluation against a preloaded CPQ index.
   *
   * @return timing metrics (index load is reported but excluded from aggregates)
   */
  public EvaluationMetrics runBenchmarkWithIndex(
      CQ query, DecompositionResult result, Index index, int k, long indexLoadMs) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(index, "index");

    CpqIndexExecutor executor = new CpqIndexExecutor(index);

    // 2. Baseline
    // Note: Using a fresh runner helper or extracting logic to get components from
    // atoms
    // In a full refactor, 'componentsFromAtoms' should be a static utility.
    LOG.info("Executing baseline...");
    Timing evalTimer = Timing.start(); // excludes index load
    Timing baselineTimer = Timing.start(); // includes atom->component extraction
    List<CpqIndexExecutor.Component> baselineComponents =
        CpqIndexExecutor.componentsFromAtoms(query);
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

    long evalMs = evalTimer.elapsedMillis();
    long baselineTotalMs = baselineMs; // already includes atom extraction + execution
    long decompositionEvalMs = tupleTotalMs;

    LOG.info(
        "Evaluation timing (ms): indexLoad={}, baselineTotal={}, tuplesTotal={}, firstTuple={}, evalNoLoad={}",
        indexLoadMs,
        baselineTotalMs,
        decompositionEvalMs,
        firstTupleMs == null ? "n/a" : firstTupleMs,
        evalMs);
    return new EvaluationMetrics(indexLoadMs, baselineTotalMs, decompositionEvalMs, evalMs);
  }

  /** Structured timing metrics for evaluation. */
  public record EvaluationMetrics(long indexLoadMs, long baselineMs, long tuplesMs, long evalMs) {}

  private PartitionEvaluation findEvaluation(DecompositionResult result, Partition partition) {
    return result.partitionEvaluations().stream()
        .filter(e -> e.partition().equals(partition))
        .findFirst()
        .orElse(null);
  }
}
