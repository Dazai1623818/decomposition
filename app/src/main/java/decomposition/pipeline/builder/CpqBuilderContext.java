package decomposition.pipeline.builder;

import decomposition.core.DecompositionOptions;
import decomposition.core.PartitionEvaluation;
import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.PartitionExpressionAssembler.ComponentCacheKey;
import decomposition.cpq.model.CacheStats;
import decomposition.pipeline.extract.CQExtractor.ExtractionResult;
import decomposition.pipeline.generation.ComponentGenerator;
import decomposition.pipeline.partitioning.FilteredPartition;
import decomposition.util.DecompositionPipelineUtils;
import decomposition.util.GraphUtils;
import decomposition.util.Timing;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mutable state container that flows through the builder stages. Holds inputs, intermediates,
 * diagnostics, and generation artifacts.
 */
public final class CpqBuilderContext {
  public static final String TERMINATION_BUDGET_AFTER_PARTITIONING =
      "time_budget_exceeded_after_partitioning";
  public static final String TERMINATION_BUDGET_DURING_GENERATION =
      "time_budget_exceeded_during_generation";
  public static final String TERMINATION_BUDGET_FINAL = "time_budget_exceeded";

  private final CQ query;
  private final Set<String> explicitFreeVariables;
  private final DecompositionOptions options;
  private final Timing timing;

  private ExtractionResult extraction;
  private List<Edge> edges = List.of();
  private Map<String, String> varToNodeMap = Map.of();
  private Set<String> vertices = Set.of();
  private BitSet fullBits = new BitSet();
  private int edgeCount;

  private List<Partition> partitions = List.of();
  private List<FilteredPartition> workList = List.of();
  private List<Partition> filteredPartitions = List.of();

  private final List<PartitionDiagnostic> diagnostics = new ArrayList<>();
  private final List<Partition> cpqPartitions = new ArrayList<>();
  private final List<CPQExpression> recognizedCatalogue = new ArrayList<>();
  private final List<PartitionEvaluation> partitionEvaluations = new ArrayList<>();
  private final List<CPQExpression> globalCatalogue = new ArrayList<>();
  private CPQExpression finalExpression;

  private final CacheStats cacheStats = new CacheStats();
  private final PartitionDiagnostics partitionDiagnostics = new PartitionDiagnostics();
  private final Map<ComponentCacheKey, CachedComponentExpressions> componentCache =
      new ConcurrentHashMap<>();
  private PartitionExpressionAssembler assembler;
  private ComponentGenerator componentGenerator;

  private String terminationReason;

  public CpqBuilderContext(
      CQ query, Set<String> explicitFreeVariables, DecompositionOptions options) {
    this.query = Objects.requireNonNull(query, "query");
    this.explicitFreeVariables =
        (explicitFreeVariables == null) ? Set.of() : Set.copyOf(explicitFreeVariables);
    this.options = DecompositionPipelineUtils.resolveOptions(options);
    this.timing = Timing.start();
  }

  public CQ query() {
    return query;
  }

  public Set<String> explicitFreeVariables() {
    return explicitFreeVariables;
  }

  public DecompositionOptions options() {
    return options;
  }

  public Timing timing() {
    return timing;
  }

  public ExtractionResult extraction() {
    return extraction;
  }

  public List<Edge> edges() {
    return edges;
  }

  public Map<String, String> varToNodeMap() {
    return varToNodeMap;
  }

  public Set<String> vertices() {
    return vertices;
  }

  public BitSet fullBits() {
    return (BitSet) fullBits.clone();
  }

  public int edgeCount() {
    return edgeCount;
  }

  public List<Partition> partitions() {
    return partitions;
  }

  public List<FilteredPartition> workList() {
    return workList;
  }

  public List<Partition> filteredPartitions() {
    return filteredPartitions;
  }

  public List<PartitionDiagnostic> diagnostics() {
    return List.copyOf(diagnostics);
  }

  public List<Partition> cpqPartitions() {
    return List.copyOf(cpqPartitions);
  }

  public List<CPQExpression> recognizedCatalogue() {
    return List.copyOf(recognizedCatalogue);
  }

  public List<CPQExpression> globalCatalogue() {
    return List.copyOf(globalCatalogue);
  }

  public CPQExpression finalExpression() {
    return finalExpression;
  }

  public List<PartitionEvaluation> partitionEvaluations() {
    return List.copyOf(partitionEvaluations);
  }

  public CacheStats cacheStats() {
    return cacheStats;
  }

  public PartitionDiagnostics partitionDiagnostics() {
    return partitionDiagnostics;
  }

  public Map<ComponentCacheKey, CachedComponentExpressions> componentCache() {
    return componentCache;
  }

  public PartitionExpressionAssembler assembler() {
    return assembler;
  }

  public String terminationReason() {
    return terminationReason;
  }

  public void applyExtraction(ExtractionResult extraction) {
    this.extraction = Objects.requireNonNull(extraction, "extraction");
    this.edges = List.copyOf(extraction.edges());
    this.varToNodeMap = Map.copyOf(extraction.variableNodeMap());
    this.edgeCount = edges.size();
    this.fullBits = new BitSet(edgeCount);
    this.fullBits.set(0, edgeCount);
    this.vertices = GraphUtils.vertices(fullBits, edges);
    this.assembler = new PartitionExpressionAssembler(edges);
  }

  public void setPartitions(List<Partition> partitions) {
    this.partitions = partitions == null ? List.of() : List.copyOf(partitions);
  }

  public void setWorkList(List<FilteredPartition> workList) {
    this.workList = workList == null ? List.of() : List.copyOf(workList);
  }

  public void setFilteredPartitions(List<Partition> filteredPartitions) {
    this.filteredPartitions =
        filteredPartitions == null ? List.of() : List.copyOf(filteredPartitions);
  }

  public void addDiagnostics(List<PartitionDiagnostic> diagnostics) {
    if (diagnostics != null && !diagnostics.isEmpty()) {
      this.diagnostics.addAll(diagnostics);
    }
  }

  public void recordComponentResult(ComponentGenerator.ComponentGenerationResult result) {
    if (result == null) {
      return;
    }

    if (result.validPartitions() != null && !result.validPartitions().isEmpty()) {
      cpqPartitions.addAll(result.validPartitions());
    }
    if (result.catalogue() != null && !result.catalogue().isEmpty()) {
      recognizedCatalogue.addAll(result.catalogue());
    }
    if (result.evaluation() != null) {
      partitionEvaluations.add(result.evaluation());
    }
  }

  public void setRecognizedCatalogue(List<CPQExpression> catalogue) {
    recognizedCatalogue.clear();
    if (catalogue != null && !catalogue.isEmpty()) {
      recognizedCatalogue.addAll(catalogue);
    }
  }

  public void setGlobalCatalogue(List<CPQExpression> catalogue) {
    globalCatalogue.clear();
    if (catalogue != null && !catalogue.isEmpty()) {
      globalCatalogue.addAll(catalogue);
    }
  }

  public void setFinalExpression(CPQExpression expression) {
    this.finalExpression = expression;
  }

  public boolean overBudget() {
    return options.timeBudgetMs() > 0 && timing.elapsedMillis() > options.timeBudgetMs();
  }

  public void markTerminated(String reason) {
    if (terminationReason == null) {
      terminationReason = reason;
    }
  }

  public boolean isTerminated() {
    return terminationReason != null;
  }

  public ComponentGenerator componentGenerator() {
    if (componentGenerator == null) {
      ensureAssembler();
      componentGenerator =
          new ComponentGenerator(
              assembler,
              componentCache,
              cacheStats,
              partitionDiagnostics,
              options.mode().enumerateTuples(),
              computeTupleLimit());
    }
    return componentGenerator;
  }

  private int computeTupleLimit() {
    if (options.singleTuplePerPartition()) {
      return 1;
    }
    int limit = options.enumerationLimit();
    return limit <= 0 ? 1 : limit;
  }

  private void ensureAssembler() {
    if (assembler != null) {
      return;
    }
    if (edges == null || edges.isEmpty()) {
      throw new IllegalStateException("Extraction must run before component generation.");
    }
    assembler = new PartitionExpressionAssembler(edges);
  }
}
