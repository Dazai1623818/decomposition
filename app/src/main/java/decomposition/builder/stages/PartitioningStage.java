package decomposition.builder.stages;

import decomposition.builder.BuilderStage;
import decomposition.builder.CpqBuilderContext;
import decomposition.generator.PartitionGenerator;
import decomposition.partitions.FilteredPartition;
import decomposition.partitions.PartitionFilter.FilterResult;
import java.util.List;
import java.util.Objects;

/** Enumerates and prunes candidate partitions, populating the context work list. */
public final class PartitioningStage implements BuilderStage {
  private final PartitionGenerator generatorOverride;

  public PartitioningStage() {
    this(null);
  }

  public PartitioningStage(PartitionGenerator generatorOverride) {
    this.generatorOverride = generatorOverride;
  }

  @Override
  public void execute(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    if (context.isTerminated()) {
      return;
    }
    if (context.overBudget()) {
      context.markTerminated(CpqBuilderContext.TERMINATION_BUDGET_AFTER_PARTITIONING);
      return;
    }
    if (context.extraction() == null) {
      throw new IllegalStateException("Extraction must run before partitioning.");
    }

    PartitionGenerator generator =
        (generatorOverride != null)
            ? generatorOverride
            : new PartitionGenerator(context.options().maxPartitions());
    FilterResult filterResult = generator.generate(context);
    List<FilteredPartition> filteredWithJoins = filterResult.partitions();

    context.setWorkList(filteredWithJoins);
    context.setFilteredPartitions(
        filteredWithJoins.stream().map(FilteredPartition::partition).toList());
    context.addDiagnostics(filterResult.diagnostics());

    if (context.overBudget()) {
      context.markTerminated(CpqBuilderContext.TERMINATION_BUDGET_AFTER_PARTITIONING);
    }
  }
}
