package decomposition.generator;

import decomposition.DecompositionOptions;
import decomposition.builder.CpqBuilderContext;
import decomposition.model.Component;
import decomposition.model.Partition;
import decomposition.partitions.PartitionFilter;
import decomposition.partitions.PartitionFilter.FilterResult;
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
    decomposition.partitions.PartitionGenerator delegate =
        new decomposition.partitions.PartitionGenerator(maxPartitions);
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
