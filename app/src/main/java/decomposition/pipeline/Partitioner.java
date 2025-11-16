package decomposition.pipeline;

import decomposition.DecompositionOptions;
import decomposition.model.Partition;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import java.util.List;

/** Enumerates candidate partitions for a CQ. */
public interface Partitioner {
  List<Partition> enumerate(PipelineContext context, DecompositionOptions options);
}
