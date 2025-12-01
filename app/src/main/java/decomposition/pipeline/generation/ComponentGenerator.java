package decomposition.pipeline.generation;

import decomposition.core.PartitionEvaluation;
import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.PartitionDiagnostics;
import decomposition.cpq.PartitionExpressionAssembler;
import decomposition.cpq.PartitionExpressionAssembler.CachedComponentExpressions;
import decomposition.cpq.PartitionExpressionAssembler.ComponentCacheKey;
import decomposition.cpq.model.CacheStats;
import decomposition.pipeline.builder.CpqBuilderContext;
import decomposition.pipeline.partitioning.FilteredPartition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds component and partition-level CPQ expressions for a single filtered partition, reusing
 * shared caches and diagnostics from the builder context.
 */
public final class ComponentGenerator {
  private final PartitionExpressionAssembler assembler;
  private final Map<ComponentCacheKey, CachedComponentExpressions> componentCache;
  private final CacheStats cacheStats;
  private final PartitionDiagnostics partitionDiagnostics;
  private final boolean enumerateTuples;
  private final int tupleLimit;

  public ComponentGenerator(
      PartitionExpressionAssembler assembler,
      Map<ComponentCacheKey, CachedComponentExpressions> componentCache,
      CacheStats cacheStats,
      PartitionDiagnostics partitionDiagnostics,
      boolean enumerateTuples,
      int tupleLimit) {
    this.assembler = Objects.requireNonNull(assembler, "assembler");
    this.componentCache = Objects.requireNonNull(componentCache, "componentCache");
    this.cacheStats = Objects.requireNonNull(cacheStats, "cacheStats");
    this.partitionDiagnostics =
        Objects.requireNonNull(partitionDiagnostics, "partitionDiagnostics");
    this.enumerateTuples = enumerateTuples;
    this.tupleLimit = tupleLimit;
  }

  public ComponentGenerationResult generate(
      FilteredPartition partition, int partitionIndex, CpqBuilderContext context) {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(context, "context");

    List<List<CPQExpression>> componentExpressions =
        assembler.synthesize(
            partition,
            context.extraction().freeVariables(),
            context.varToNodeMap(),
            componentCache,
            cacheStats,
            partitionDiagnostics,
            partitionIndex);

    if (componentExpressions == null) {
      appendDiagnostics(partitionIndex, context);
      return ComponentGenerationResult.empty();
    }

    Partition matchedPartition = partition.partition();
    List<CPQExpression> catalogue = componentExpressions.stream().flatMap(List::stream).toList();

    List<List<CPQExpression>> tuples = enumerateTuples(componentExpressions);
    int maxDiameter = 0;
    for (List<CPQExpression> tuple : tuples) {
      for (CPQExpression expr : tuple) {
        maxDiameter = Math.max(maxDiameter, expr.cpq().getDiameter());
      }
    }

    PartitionEvaluation evaluation =
        new PartitionEvaluation(
            matchedPartition,
            partitionIndex,
            componentExpressions.stream().map(List::size).toList(),
            tuples,
            maxDiameter);

    return new ComponentGenerationResult(List.of(matchedPartition), catalogue, evaluation);
  }

  private List<List<CPQExpression>> enumerateTuples(
      List<List<CPQExpression>> perComponentExpressions) {
    if (!enumerateTuples || perComponentExpressions == null || perComponentExpressions.isEmpty()) {
      return List.of();
    }

    int componentCount = perComponentExpressions.size();
    int[] idx = new int[componentCount];
    List<List<CPQExpression>> tuples = new ArrayList<>();

    while (true) {
      List<CPQExpression> tuple = new ArrayList<>(componentCount);
      for (int i = 0; i < componentCount; i++) {
        tuple.add(perComponentExpressions.get(i).get(idx[i]));
      }
      tuples.add(tuple);
      if (tupleLimit > 0 && tuples.size() >= tupleLimit) {
        break;
      }

      int position = componentCount - 1;
      while (position >= 0) {
        idx[position]++;
        if (idx[position] < perComponentExpressions.get(position).size()) {
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

  private void appendDiagnostics(int partitionIndex, CpqBuilderContext context) {
    List<PartitionDiagnostic> cached = partitionDiagnostics.lastComponentDiagnostics();
    if (cached != null && !cached.isEmpty()) {
      context.addDiagnostics(cached);
      return;
    }
    context.addDiagnostics(List.of(PartitionDiagnostic.diagnosticsUnavailable(partitionIndex)));
  }

  public record ComponentGenerationResult(
      List<Partition> validPartitions,
      List<CPQExpression> catalogue,
      PartitionEvaluation evaluation) {

    public static ComponentGenerationResult empty() {
      return new ComponentGenerationResult(List.of(), List.of(), null);
    }
  }
}
