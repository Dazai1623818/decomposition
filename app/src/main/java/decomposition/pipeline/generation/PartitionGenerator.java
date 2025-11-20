package decomposition.pipeline.generation;

import decomposition.core.DecompositionOptions;
import decomposition.core.model.Component;
import decomposition.core.model.Partition;
import decomposition.pipeline.builder.CpqBuilderContext;
import decomposition.pipeline.partitioning.PartitionFilter;
import decomposition.pipeline.partitioning.PartitionFilter.FilterResult;
import java.util.List;
import java.util.Objects;

/**
 * Concrete partition generator that merges the legacy partition enumeration and pruning logic into
 * a single utility for the builder pipeline.
 */
public final class PartitionGenerator {
  private final int maxPartitions;
  private final PartitionFilter filter;

  public PartitionGenerator(int maxPartitions) {
    this.maxPartitions = maxPartitions;
    this.filter = new PartitionFilter(GeneratorDefaults.MAX_JOIN_NODES);
  }

  public PartitionGenerator(DecompositionOptions options) {
    this(options != null ? options.maxPartitions() : 0);
  }

  public FilterResult generate(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    decomposition.pipeline.partitioning.PartitionGenerator delegate =
        new decomposition.pipeline.partitioning.PartitionGenerator(maxPartitions);
    List<Component> components = delegate.enumerateConnectedComponents(context.edges());
    List<Partition> partitions =
        delegate.enumeratePartitions(
            context.edges(),
            components,
            context.extraction().freeVariables(),
            GeneratorDefaults.MAX_JOIN_NODES);

    context.setPartitions(partitions);
    return filter.filter(partitions, context.extraction().freeVariables());
  }
}
