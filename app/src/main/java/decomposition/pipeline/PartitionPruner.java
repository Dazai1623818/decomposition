package decomposition.pipeline;

import decomposition.DecompositionOptions;
import decomposition.model.Partition;
import decomposition.pipeline.DecompositionPipelineState.PartitionSets;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import java.util.List;

/** Applies structural filters to partitions produced by the generator. */
public interface PartitionPruner {
  PartitionSets prune(
      List<Partition> partitions, PipelineContext context, DecompositionOptions options);
}
