package decomposition.pipeline;

import decomposition.DecompositionOptions;
import decomposition.model.Component;
import decomposition.model.Partition;
import decomposition.partitions.PartitionGenerator;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import java.util.List;

/** Default {@link Partitioner} that delegates to {@link PartitionGenerator}. */
public final class DefaultPartitioner implements Partitioner {
  @Override
  public List<Partition> enumerate(PipelineContext context, DecompositionOptions options) {
    PartitionGenerator generator = new PartitionGenerator(options.maxPartitions());
    List<Component> components = generator.enumerateConnectedComponents(context.edges());
    return generator.enumeratePartitions(
        context.edges(),
        components,
        context.extraction().freeVariables(),
        PipelineDefaults.MAX_JOIN_NODES);
  }
}
