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
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.PartitionExpressionAssembler.ComponentCacheKey;
import decomposition.cpq.model.CacheStats;
import decomposition.eval.EvaluationPipeline;
import decomposition.eval.IndexManager;
import decomposition.pipeline.extract.CQExtractor;
import decomposition.pipeline.extract.CQExtractor.ExtractionResult;
import decomposition.pipeline.generation.GeneratorDefaults;
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
import java.util.List;
import java.util.Map;
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

  /** Workflow 1: Decomposition only. */
  public DecompositionResult decompose(
      CQ query, Set<String> freeVariables, DecompositionOptions options) {
    return decompose(query, freeVariables, options, PlanMode.ALL);
  }

  /**
   * Lean decomposition flow that can return a single-edge plan, the first valid plan, or all plans.
   */
  public DecompositionResult decompose(
      CQ query, Set<String> freeVariables, DecompositionOptions options, PlanMode planMode) {
    LOG.info("Executing decomposition pipeline (plan mode: {})...", planMode);
    DecompositionOptions effective = options != null ? options : DecompositionOptions.defaults();
    PlanMode mode = planMode == null ? PlanMode.ALL : planMode;
    Timing timer = Timing.start();

    // 1) Extract
    CQExtractor extractor = new CQExtractor();
    ExtractionResult extraction = extractor.extract(query, freeVariables);
    List<Edge> edges = extraction.edges();
    Map<String, String> varToNodeMap = extraction.variableNodeMap();
    BitSet fullBits = BitsetUtils.allOnes(edges.size());

    // 2) Enumerate partitions according to plan mode
    List<Partition> partitions = enumeratePartitions(mode, extraction, effective);

    // 3) Filter partitions (join-node constraints + free-var visibility)
    PartitionFilter filter = new PartitionFilter(GeneratorDefaults.MAX_JOIN_NODES);
    FilterResult filterResult = filter.filter(partitions, extraction.freeVariables());
    List<FilteredPartition> filtered = filterResult.partitions();
    List<PartitionDiagnostic> diagnostics = new ArrayList<>(filterResult.diagnostics());

    // 4) Generate CPQ expressions for each partition
    PartitionExpressionAssembler assembler = new PartitionExpressionAssembler(edges);
    CacheStats cacheStats = new CacheStats();
    PartitionDiagnostics partitionDiagnostics = new PartitionDiagnostics();
    Map<ComponentCacheKey, CachedComponentExpressions> componentCache = new ConcurrentHashMap<>();

    List<Partition> validPartitions = new ArrayList<>();
    List<CPQExpression> recognisedCatalogue = new ArrayList<>();
    List<PartitionEvaluation> evaluations = new ArrayList<>();

    int tupleLimit = computeTupleLimit(effective);
    boolean enumerateTuples = effective.mode().enumerateTuples();
    String terminationReason = null;

    for (int i = 0; i < filtered.size(); i++) {
      if (isOverBudget(effective, timer)) {
        terminationReason = "time_budget_exceeded";
        break;
      }

      int partitionIndex = i + 1;
      FilteredPartition filteredPartition = filtered.get(i);
      List<List<CPQExpression>> perComponent =
          assembler.synthesize(
              filteredPartition,
              extraction.freeVariables(),
              varToNodeMap,
              componentCache,
              cacheStats,
              partitionDiagnostics,
              partitionIndex);

      if (perComponent == null) {
        diagnostics.addAll(partitionDiagnostics.lastComponentDiagnostics());
        continue;
      }

      List<CPQExpression> catalogForPartition =
          perComponent.stream().flatMap(List::stream).toList();
      recognisedCatalogue.addAll(catalogForPartition);

      List<List<CPQExpression>> tuples =
          enumerateTuples ? enumerateTuples(perComponent, tupleLimit) : List.of();
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

      if (mode == PlanMode.FIRST) {
        break;
      }
    }

    // 5) Assemble global candidate
    Set<String> globalJoinNodes =
        JoinAnalysisBuilder.analyzePartition(
                new Partition(
                    List.of(new Component(fullBits, GraphUtils.vertices(fullBits, edges)))),
                extraction.freeVariables())
            .globalJoinNodes();
    List<CPQExpression> globalCatalogue =
        assembler.synthesizeGlobal(fullBits, globalJoinNodes, varToNodeMap);
    CPQExpression finalExpression = globalCatalogue.isEmpty() ? null : globalCatalogue.get(0);

    long elapsed = timer.elapsedMillis();
    if (terminationReason == null && isOverBudget(effective, elapsed)) {
      terminationReason = "time_budget_exceeded";
    }

    List<CPQExpression> dedupCatalogue =
        ComponentExpressionBuilder.dedupeExpressions(recognisedCatalogue);

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
      CQ query,
      Set<String> freeVariables,
      DecompositionOptions options,
      PlanMode planMode,
      Path graphPath,
      int indexK)
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

    PlanMode effectivePlan = planMode == null ? PlanMode.ALL : planMode;
    DecompositionResult result = decompose(query, freeVariables, benchmarkOptions, effectivePlan);
    if (result.cpqPartitions().isEmpty()) {
      LOG.warn("No valid partitions generated. Aborting evaluation phase.");
      return result;
    }

    // 2. Run comparative evaluation
    EvaluationPipeline evaluator = new EvaluationPipeline();
    evaluator.runBenchmark(query, result, graphPath, indexK > 0 ? indexK : DEFAULT_INDEX_DIAMETER);
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
        extraction.edges(),
        components,
        extraction.freeVariables(),
        GeneratorDefaults.MAX_JOIN_NODES);
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

  private int computeTupleLimit(DecompositionOptions options) {
    if (options.singleTuplePerPartition()) {
      return 1;
    }
    int limit = options.enumerationLimit();
    return limit <= 0 ? 1 : limit;
  }

  private boolean isOverBudget(DecompositionOptions options, Timing timer) {
    return isOverBudget(options, timer.elapsedMillis());
  }

  private boolean isOverBudget(DecompositionOptions options, long elapsedMillis) {
    return options.timeBudgetMs() > 0 && elapsedMillis > options.timeBudgetMs();
  }
}
