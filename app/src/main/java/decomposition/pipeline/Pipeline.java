package decomposition.pipeline;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.eval.EvaluationPipeline;
import decomposition.eval.IndexManager;
import decomposition.pipeline.builder.CpqBuilder;
import decomposition.pipeline.builder.CpqBuilderResult;
import dev.roanh.cpqindex.Index;
import dev.roanh.gmark.lang.cq.CQ;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-level orchestrator for the application's workflows.
 *
 * <p>Provides dedicated entry points for decomposition, index construction, and full benchmarking
 * that stitches decomposition together with evaluation.
 */
public final class Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private static final int INDEX_DIAMETER = 3;

  /**
   * Workflow 1: Decomposition only.
   *
   * @return the decomposition result (without running evaluation)
   */
  public DecompositionResult decompose(
      CQ query, Set<String> freeVariables, DecompositionOptions options) {
    LOG.info("Executing decomposition pipeline...");
    CpqBuilderResult result = CpqBuilder.defaultBuilder().build(query, freeVariables, options);

    if (result.result().terminationReason() != null) {
      LOG.warn("Decomposition terminated early: {}", result.result().terminationReason());
    } else {
      LOG.info(
          "Decomposition complete. Found {} valid partitions.",
          result.result().cpqPartitions().size());
    }
    return result.result();
  }

  /** Workflow 2: Index construction only. */
  public void buildIndex(Path graphPath) throws IOException {
    LOG.info("Executing index construction pipeline for: {}", graphPath);
    Index index = new IndexManager().loadOrBuild(graphPath, INDEX_DIAMETER);
    try {
      index.print();
    } catch (NullPointerException ex) {
      // Some persisted indexes were saved without label metadata; skip printing in that case.
      LOG.warn("Index loaded without labels; skipping printable summary.");
    }
  }

  /**
   * Workflow 3: Full benchmark.
   *
   * <ol>
   *   <li>Loads/builds index.
   *   <li>Runs decomposition (forces enumeration mode).
   *   <li>Executes baseline and decomposition tuple evaluations.
   * </ol>
   */
  public DecompositionResult benchmark(
      CQ query, Set<String> freeVariables, DecompositionOptions options, Path graphPath)
      throws IOException {
    LOG.info("Executing full benchmark pipeline...");

    // 1. Run decomposition (force ENUMERATE to ensure we have tuples to evaluate)
    DecompositionOptions benchmarkOptions =
        options.mode().enumerateTuples()
            ? options
            : new DecompositionOptions(
                DecompositionOptions.Mode.ENUMERATE,
                options.maxPartitions(),
                options.timeBudgetMs(),
                options.enumerationLimit(),
                options.singleTuplePerPartition(),
                options.deepVerification());

    DecompositionResult result = decompose(query, freeVariables, benchmarkOptions);
    if (result.cpqPartitions().isEmpty()) {
      LOG.warn("No valid partitions generated. Aborting evaluation phase.");
      return result;
    }

    // 2. Run comparative evaluation
    EvaluationPipeline evaluator = new EvaluationPipeline();
    evaluator.runBenchmark(query, result, graphPath);
    return result;
  }
}
