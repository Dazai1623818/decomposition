package decomposition.pipeline;

import decomposition.DecompositionOptions;
import decomposition.diagnostics.PartitionDiagnostic;
import decomposition.model.Partition;
import decomposition.partitions.FilteredPartition;
import decomposition.partitions.PartitionFilter;
import decomposition.partitions.PartitionFilter.FilterResult;
import decomposition.pipeline.DecompositionPipelineState.PartitionSets;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import java.util.ArrayList;
import java.util.List;

/** Filters candidate partitions using {@link PartitionFilter}. */
public final class DefaultPartitionPruner implements PartitionPruner {
  @Override
  public PartitionSets prune(
      List<Partition> partitions, PipelineContext context, DecompositionOptions options) {
    FilterResult filterResult =
        new PartitionFilter(PipelineDefaults.MAX_JOIN_NODES)
            .filter(partitions, context.extraction().freeVariables());
    List<FilteredPartition> filteredWithJoins = filterResult.partitions();
    List<Partition> filtered =
        filteredWithJoins.stream().map(FilteredPartition::partition).toList();
    List<PartitionDiagnostic> diagnostics = new ArrayList<>(filterResult.diagnostics());
    return new PartitionSets(partitions, filteredWithJoins, filtered, diagnostics);
  }
}
