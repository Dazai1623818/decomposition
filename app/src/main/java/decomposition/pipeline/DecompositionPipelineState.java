package decomposition.pipeline;

import decomposition.PartitionEvaluation;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentCacheKey;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.model.CacheStats;
import decomposition.cpq.model.ComponentKey;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Edge;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilter.FilteredPartition;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Container types used during decomposition bookkeeping. */
public final class DecompositionPipelineState {
  private DecompositionPipelineState() {}

  public record PipelineContext(
      ExtractionResult extraction,
      List<Edge> edges,
      Map<String, String> varToNodeMap,
      Set<String> vertices,
      BitSet fullBits,
      int edgeCount) {}

  public record PartitionSets(
      List<Partition> partitions,
      List<FilteredPartition> filteredWithJoins,
      List<Partition> filtered,
      List<String> diagnostics) {}

  public static final class SynthesisState {
    public final PartitionExpressionAssembler synthesizer;
    public final CacheStats cacheStats;
    public final PartitionDiagnostics partitionDiagnostics;
    public final Map<ComponentCacheKey, CachedComponentExpressions> componentCache;
    public final List<Partition> cpqPartitions = new ArrayList<>();
    public final List<PartitionEvaluation> partitionEvaluations = new ArrayList<>();
    public final Map<ComponentKey, CPQExpression> recognizedCatalogue = new LinkedHashMap<>();
    public final boolean wantTuples;
    public final int tupleLimit;

    public SynthesisState(
        PartitionExpressionAssembler synthesizer,
        CacheStats cacheStats,
        PartitionDiagnostics partitionDiagnostics,
        Map<ComponentCacheKey, CachedComponentExpressions> componentCache,
        boolean wantTuples,
        int tupleLimit) {
      this.synthesizer = synthesizer;
      this.cacheStats = cacheStats;
      this.partitionDiagnostics = partitionDiagnostics;
      this.componentCache = componentCache;
      this.wantTuples = wantTuples;
      this.tupleLimit = tupleLimit;
    }
  }

  public record GlobalResult(List<CPQExpression> globalCatalogue, CPQExpression finalExpression) {}
}
