package decomposition.pipeline;

import decomposition.core.DecompositionOptions;
import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Component;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.PartitionExpressionAssembler.ComponentCacheKey;
import decomposition.cpq.model.CacheStats;
import decomposition.eval.EvaluationPipeline;
import decomposition.eval.IndexManager;
import decomposition.pipeline.extract.CQExtractor;
import decomposition.pipeline.extract.CQExtractor.ExtractionResult;
import decomposition.pipeline.generation.PartitionGenerator;
import decomposition.pipeline.partitioning.FilteredPartition;
import decomposition.pipeline.partitioning.PartitionFilter;
import decomposition.pipeline.partitioning.PartitionFilter.FilterResult;
import decomposition.util.BitsetUtils;
import decomposition.util.DecompositionPipelineUtils;
import decomposition.util.GraphUtils;
import decomposition.util.JoinAnalysisBuilder;
import decomposition.util.Timing;
import dev.roanh.cpqindex.Index;
import dev.roanh.gmark.lang.cq.CQ;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
  private static final int DEFAULT_INDEX_DIAMETER = 3;
  private static final int MAX_RANDOM_ATTEMPTS = 5;

  /** Workflow 1: Decomposition only. */
  public DecompositionResult decompose(
      CQ query, Set<String> freeVariables, DecompositionOptions options) {
    return decompose(query, freeVariables, options, null);
  }

  private DecompositionResult decompose(
      CQ query,
      Set<String> freeVariables,
      DecompositionOptions options,
      Map<ComponentCacheKey, CachedComponentExpressions> sharedCache) {
    DecompositionOptions effective = DecompositionOptions.normalize(options);
    PlanMode mode = effective.planMode();
    LOG.info("Executing decomposition pipeline (plan mode: {})...", mode);
    Timing timer = Timing.start();
    long tExtractStart = timer.elapsedMillis();

    // 1) Extract
    CQExtractor extractor = new CQExtractor();
    ExtractionResult extraction = extractor.extract(query, freeVariables);
    List<Edge> edges = extraction.edges();
    Map<String, String> varToNodeMap = extraction.variableNodeMap();
    BitSet fullBits = BitsetUtils.allOnes(edges.size());
    long tExtract = timer.elapsedMillis() - tExtractStart;

    PartitionFilter filter = new PartitionFilter(2);
    PartitionExpressionAssembler assembler = new PartitionExpressionAssembler(edges);
    CacheStats cacheStats = new CacheStats();
    Map<ComponentCacheKey, CachedComponentExpressions> componentCache =
        sharedCache != null ? sharedCache : new ConcurrentHashMap<>();

    List<Partition> partitions = new ArrayList<>();
    List<FilteredPartition> filtered = new ArrayList<>();
    List<PartitionDiagnostic> diagnostics = new ArrayList<>();
    List<Partition> validPartitions = new ArrayList<>();
    List<CPQExpression> recognisedCatalogue = new ArrayList<>();
    List<PartitionEvaluation> evaluations = new ArrayList<>();

    int tupleLimit = effective.tupleLimit();
    boolean enumerateTuples = effective.mode().enumerateTuples() && tupleLimit > 0;
    boolean firstHit = enumerateTuples && tupleLimit == 1 && effective.planMode() != PlanMode.ALL;
    String terminationReason = null;

    long tEnum = 0L;
    long tFilter = 0L;
    long tSynthesize = 0L;
    long tTupleEnum = 0L;

    if (mode == PlanMode.FIRST || mode == PlanMode.RANDOM) {
      long tEnumStart = timer.elapsedMillis();
      PartitionGenerator generator = new PartitionGenerator(effective.maxPartitions());
      List<Component> components = generator.enumerateConnectedComponents(edges);
      if (mode == PlanMode.RANDOM) {
        Collections.shuffle(components);
      }

      class FirstPlanState {
        long lastMark = timer.elapsedMillis();
        long enumerate = 0L;
        long filter = 0L;
        long synthesize = 0L;
        long tupleEnum = 0L;
        String termination;
      }

      FirstPlanState state = new FirstPlanState();
      state.enumerate += timer.elapsedMillis() - tEnumStart;
      state.lastMark = timer.elapsedMillis();

      generator.visitPartitions(
          edges,
          components,
          extraction.freeVariables(),
          2,
          partition -> {
            long now = timer.elapsedMillis();
            state.enumerate += now - state.lastMark;
            partitions.add(partition);

            if (state.termination == null && isOverBudget(effective, timer)) {
              state.termination = "time_budget_exceeded";
              return false;
            }

            long filterStart = timer.elapsedMillis();
            FilterResult single = filter.filter(List.of(partition), extraction.freeVariables());
            state.filter += timer.elapsedMillis() - filterStart;
            diagnostics.addAll(single.diagnostics());
            if (single.partitions().isEmpty()) {
              state.lastMark = timer.elapsedMillis();
              return true;
            }

            FilteredPartition filteredPartition = single.partitions().get(0);
            filtered.add(filteredPartition);

            long synthesizeStart = timer.elapsedMillis();
            List<List<CPQExpression>> perComponent =
                assembler.synthesize(
                    filteredPartition,
                    extraction.freeVariables(),
                    varToNodeMap,
                    componentCache,
                    cacheStats,
                    effective.diameterCap(),
                    firstHit);
            state.synthesize += timer.elapsedMillis() - synthesizeStart;

            if (perComponent == null) {
              diagnostics.add(PartitionDiagnostic.diagnosticsUnavailable(partitions.size()));
              state.lastMark = timer.elapsedMillis();
              return true;
            }

            List<CPQExpression> catalogForPartition =
                perComponent.stream().flatMap(List::stream).toList();
            recognisedCatalogue.addAll(catalogForPartition);

            long tupleStart = timer.elapsedMillis();
            List<List<CPQExpression>> tuples =
                enumerateTuples ? enumerateTuples(perComponent, tupleLimit) : List.of();
            state.tupleEnum += timer.elapsedMillis() - tupleStart;
            int maxDiameter = maxDiameter(tuples, catalogForPartition);

            PartitionEvaluation evaluation =
                new PartitionEvaluation(
                    filteredPartition.partition(),
                    partitions.size(),
                    perComponent.stream().map(List::size).toList(),
                    tuples,
                    maxDiameter);

            validPartitions.add(filteredPartition.partition());
            evaluations.add(evaluation);

            state.lastMark = timer.elapsedMillis();
            return false; // Stop after the first synthesized partition
          });

      tEnum = state.enumerate;
      tFilter = state.filter;
      tSynthesize = state.synthesize;
      tTupleEnum = state.tupleEnum;
      terminationReason = state.termination;
    } else {
      // Enumerate partitions according to plan mode
      long tEnumStart = timer.elapsedMillis();
      partitions.addAll(enumeratePartitions(mode, extraction, effective));
      tEnum = timer.elapsedMillis() - tEnumStart;

      // Filter partitions (join-node constraints + free-var visibility)
      long tFilterStart = timer.elapsedMillis();
      FilterResult filterResult = filter.filter(partitions, extraction.freeVariables());
      filtered.addAll(filterResult.partitions());
      diagnostics.addAll(filterResult.diagnostics());
      tFilter = timer.elapsedMillis() - tFilterStart;

      // Generate CPQ expressions for each partition
      for (int i = 0; i < filtered.size(); i++) {
        if (isOverBudget(effective, timer)) {
          terminationReason = "time_budget_exceeded";
          break;
        }

        int partitionIndex = i + 1;
        FilteredPartition filteredPartition = filtered.get(i);
        long synthesizeStart = timer.elapsedMillis();
        List<List<CPQExpression>> perComponent =
            assembler.synthesize(
                filteredPartition,
                extraction.freeVariables(),
                varToNodeMap,
                componentCache,
                cacheStats,
                effective.diameterCap(),
                firstHit);
        tSynthesize += timer.elapsedMillis() - synthesizeStart;

        if (perComponent == null) {
          diagnostics.add(PartitionDiagnostic.diagnosticsUnavailable(partitionIndex));
          continue;
        }

        List<CPQExpression> catalogForPartition =
            perComponent.stream().flatMap(List::stream).toList();
        recognisedCatalogue.addAll(catalogForPartition);

        long tupleStart = timer.elapsedMillis();
        List<List<CPQExpression>> tuples =
            enumerateTuples ? enumerateTuples(perComponent, tupleLimit) : List.of();
        tTupleEnum += timer.elapsedMillis() - tupleStart;
        int maxDiameter = maxDiameter(tuples, catalogForPartition);

        PartitionEvaluation evaluation =
            new PartitionEvaluation(
                filteredPartition.partition(),
                partitionIndex,
                perComponent.stream().map(List::size).toList(),
                tuples,
                maxDiameter);

        validPartitions.add(filteredPartition.partition());
        evaluations.add(evaluation);
      }
    }

    // 5) Assemble global candidate
    long tAssembleStart = timer.elapsedMillis();
    List<CPQExpression> globalCatalogue = List.of();
    CPQExpression finalExpression = null;
    long tAssemble = 0L;
    boolean skipGlobal = mode == PlanMode.FIRST || mode == PlanMode.RANDOM;
    if (!skipGlobal) {
      Set<String> globalJoinNodes =
          JoinAnalysisBuilder.analyzePartition(
                  new Partition(
                      List.of(new Component(fullBits, GraphUtils.vertices(fullBits, edges)))),
                  extraction.freeVariables())
              .globalJoinNodes();
      globalCatalogue = assembler.synthesizeGlobal(fullBits, globalJoinNodes, varToNodeMap);
      finalExpression = globalCatalogue.isEmpty() ? null : globalCatalogue.get(0);
      tAssemble = timer.elapsedMillis() - tAssembleStart;
    }

    long elapsed = timer.elapsedMillis();
    if (terminationReason == null && isOverBudget(effective, elapsed)) {
      terminationReason = "time_budget_exceeded";
    }

    List<CPQExpression> dedupCatalogue =
        validPartitions.size() <= 1
            ? recognisedCatalogue
            : ComponentExpressionBuilder.dedupeExpressions(recognisedCatalogue);

    DecompositionResult result =
        DecompositionPipelineUtils.buildResult(
            extraction,
            GraphUtils.vertices(fullBits, edges),
            partitions,
            filtered.stream().map(FilteredPartition::partition).toList(),
            validPartitions,
            dedupCatalogue,
            finalExpression,
            globalCatalogue,
            evaluations,
            diagnostics,
            elapsed,
            terminationReason);

    LOG.info(
        "Decomposition complete. Found {} valid partition(s) in {} ms.",
        result.cpqPartitions().size(),
        result.elapsedMillis());
    LOG.info(
        "Timing breakdown (ms): extract={}, enumerate={}, filter={}, synthesize={}, tuples={}, assemble={}",
        tExtract,
        tEnum,
        tFilter,
        tSynthesize,
        tTupleEnum,
        tAssemble);
    if (terminationReason != null) {
      LOG.warn("Decomposition terminated early: {}", terminationReason);
    }
    return result;
  }

  /** Workflow 2: Index construction only. */
  public void buildIndex(Path graphPath, int k) throws IOException {
    LOG.info("Executing index construction pipeline for: {}", graphPath);
    int effectiveK = k > 0 ? k : DEFAULT_INDEX_DIAMETER;
    Index index = new IndexManager().loadOrBuild(graphPath, effectiveK);
    // Intentionally skip index.print() to avoid noisy console output.
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
      CQ query, Set<String> freeVariables, DecompositionOptions options, Path graphPath, int indexK)
      throws IOException {
    int effectiveK = indexK > 0 ? indexK : DEFAULT_INDEX_DIAMETER;
    IndexManager indexManager = new IndexManager();
    Timing indexTimer = Timing.start();
    Index index = indexManager.loadOrBuild(graphPath, effectiveK);
    long indexLoadMs = indexTimer.elapsedMillis();
    return benchmarkWithIndex(
        query, freeVariables, options, graphPath, index, effectiveK, indexLoadMs);
  }

  /**
   * Executes full benchmark given a preloaded index. Keeps index in-memory so callers can reuse it
   * across multiple queries/graphs without reloading.
   */
  public DecompositionResult benchmarkWithIndex(
      CQ query,
      Set<String> freeVariables,
      DecompositionOptions options,
      Path graphPath,
      Index index,
      int indexK,
      long indexLoadMs)
      throws IOException {
    LOG.info("Executing full benchmark pipeline...");
    Objects.requireNonNull(graphPath, "graphPath");
    Objects.requireNonNull(index, "index");

    DecompositionOptions normalized = DecompositionOptions.normalize(options);
    int effectiveK = indexK > 0 ? indexK : DEFAULT_INDEX_DIAMETER;
    int diameterCap = normalized.diameterCap() > 0 ? normalized.diameterCap() : effectiveK;

    // 1. Run decomposition (force ENUMERATE to ensure we have tuples to evaluate)
    DecompositionOptions benchmarkOptions =
        normalized.mode().enumerateTuples()
            ? new DecompositionOptions(
                normalized.mode(),
                normalized.maxPartitions(),
                normalized.timeBudgetMs(),
                normalized.tupleLimit(),
                normalized.deepVerification(),
                normalized.planMode(),
                diameterCap)
            : new DecompositionOptions(
                DecompositionOptions.Mode.ENUMERATE,
                normalized.maxPartitions(),
                normalized.timeBudgetMs(),
                normalized.tupleLimit(),
                normalized.deepVerification(),
                normalized.planMode(),
                diameterCap);

    EvaluationPipeline evaluator = new EvaluationPipeline();
    if (benchmarkOptions.planMode() == PlanMode.RANDOM) {
      Map<ComponentCacheKey, CachedComponentExpressions> sharedCache = new ConcurrentHashMap<>();

      DecompositionResult chosenResult = null;
      for (int attempt = 1; attempt <= MAX_RANDOM_ATTEMPTS; attempt++) {
        LOG.info("Random attempt {}/{}...", attempt, MAX_RANDOM_ATTEMPTS);
        DecompositionResult attemptResult =
            decompose(query, freeVariables, benchmarkOptions, sharedCache);
        PartitionEvaluation eval = firstEvaluable(attemptResult, effectiveK);
        if (eval != null) {
          chosenResult = attemptResult;
          break;
        }
        LOG.info(
            "No evaluable partition found in attempt {} (maxDiameter too large or no tuples).",
            attempt);
      }

      if (chosenResult == null) {
        LOG.warn("No evaluable random partition found after {} attempts.", MAX_RANDOM_ATTEMPTS);
        return null;
      }

      EvaluationPipeline.EvaluationMetrics metrics =
          evaluator.runBenchmarkWithIndex(query, chosenResult, index, effectiveK, indexLoadMs);
      long totalDecomposeEval = chosenResult.elapsedMillis() + metrics.tuplesMs();
      LOG.info(
          "Evaluation phase took {} ms (excluding index load); decomposition total: {} ms; tuples: {} ms; decompose+tuples: {} ms; baseline: {} ms",
          metrics.evalMs(),
          chosenResult.elapsedMillis(),
          metrics.tuplesMs(),
          totalDecomposeEval,
          metrics.baselineMs());
      return chosenResult;
    }

    DecompositionResult result = decompose(query, freeVariables, benchmarkOptions);
    if (result.cpqPartitions().isEmpty()) {
      LOG.warn("No valid partitions generated. Aborting evaluation phase.");
      return result;
    }

    // 2. Run comparative evaluation
    EvaluationPipeline.EvaluationMetrics metrics =
        evaluator.runBenchmarkWithIndex(query, result, index, effectiveK, indexLoadMs);
    long totalDecompositionMs = result.elapsedMillis();
    long decomposePlusTuples = totalDecompositionMs + metrics.tuplesMs();
    LOG.info(
        "Evaluation phase took {} ms (excluding index load); decomposition total: {} ms; tuples: {} ms; decompose+tuples: {} ms; baseline: {} ms",
        metrics.evalMs(),
        totalDecompositionMs,
        metrics.tuplesMs(),
        decomposePlusTuples,
        metrics.baselineMs());
    return result;
  }

  private List<Partition> enumeratePartitions(
      PlanMode planMode, ExtractionResult extraction, DecompositionOptions options) {
    if (planMode == PlanMode.SINGLE_EDGE) {
      return List.of(buildSingleEdgePartition(extraction.edges()));
    }

    int max = planMode == PlanMode.FIRST ? 1 : options.maxPartitions();
    PartitionGenerator generator = new PartitionGenerator(max);
    List<Component> components = generator.enumerateConnectedComponents(extraction.edges());
    return generator.enumeratePartitions(
        extraction.edges(), components, extraction.freeVariables(), 2);
  }

  private Partition buildSingleEdgePartition(List<Edge> edges) {
    if (edges.isEmpty()) {
      return new Partition(List.of());
    }
    List<Component> components = new ArrayList<>(edges.size());
    for (int i = 0; i < edges.size(); i++) {
      BitSet bits = new BitSet(edges.size());
      bits.set(i);
      components.add(GraphUtils.buildComponent(bits, edges));
    }
    return new Partition(components);
  }

  private List<List<CPQExpression>> enumerateTuples(
      List<List<CPQExpression>> perComponent, int tupleLimit) {
    int componentCount = perComponent.size();
    int[] idx = new int[componentCount];
    List<List<CPQExpression>> tuples = new ArrayList<>();

    while (true) {
      List<CPQExpression> tuple = new ArrayList<>(componentCount);
      for (int i = 0; i < componentCount; i++) {
        tuple.add(perComponent.get(i).get(idx[i]));
      }
      tuples.add(tuple);
      if (tupleLimit > 0 && tuples.size() >= tupleLimit) {
        break;
      }

      int position = componentCount - 1;
      while (position >= 0) {
        idx[position]++;
        if (idx[position] < perComponent.get(position).size()) {
          break;
        }
        idx[position] = 0;
        position--;
      }
      if (position < 0) {
        break;
      }
    }
    return tuples;
  }

  private int maxDiameter(List<List<CPQExpression>> tuples, List<CPQExpression> catalogue) {
    int max = 0;
    for (List<CPQExpression> tuple : tuples) {
      for (CPQExpression expr : tuple) {
        max = Math.max(max, expr.cpq().getDiameter());
      }
    }
    if (max == 0) {
      for (CPQExpression expr : catalogue) {
        max = Math.max(max, expr.cpq().getDiameter());
      }
    }
    return max;
  }

  private boolean isOverBudget(DecompositionOptions options, Timing timer) {
    return isOverBudget(options, timer.elapsedMillis());
  }

  private boolean isOverBudget(DecompositionOptions options, long elapsedMillis) {
    return options.timeBudgetMs() > 0 && elapsedMillis > options.timeBudgetMs();
  }

  private PartitionEvaluation firstEvaluable(DecompositionResult result, int indexK) {
    if (result == null) {
      return null;
    }
    for (PartitionEvaluation evaluation : result.partitionEvaluations()) {
      if (evaluation.decompositionTuples().isEmpty()) {
        continue;
      }
      if (evaluation.maxDiameter() > indexK) {
        continue;
      }
      return evaluation;
    }
    return null;
  }
}
