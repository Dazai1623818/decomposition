package decomposition.builder.stages;

import decomposition.builder.BuilderStage;
import decomposition.builder.CpqBuilderContext;
import decomposition.generator.ComponentGenerator;
import decomposition.generator.ComponentGenerator.ComponentGenerationResult;
import decomposition.partitions.FilteredPartition;
import java.util.List;
import java.util.Objects;

/**
 * Generates CPQ components for each filtered partition, enforcing the time budget inside the loop
 * instead of the orchestrator.
 */
public final class ComponentGenerationStage implements BuilderStage {
  private final ComponentGenerator generatorOverride;

  public ComponentGenerationStage() {
    this(null);
  }

  public ComponentGenerationStage(ComponentGenerator generatorOverride) {
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
      throw new IllegalStateException("Extraction must run before component generation.");
    }

    ComponentGenerator generator =
        (generatorOverride != null) ? generatorOverride : context.componentGenerator();
    List<FilteredPartition> workList = context.workList();
    if (workList.isEmpty()) {
      return;
    }

    for (int idx = 0; idx < workList.size(); idx++) {
      if (context.overBudget()) {
        context.markTerminated(CpqBuilderContext.TERMINATION_BUDGET_DURING_GENERATION);
        break;
      }
      ComponentGenerationResult result = generator.generate(workList.get(idx), idx + 1, context);
      context.recordComponentResult(result);
    }

    if (!context.isTerminated() && context.overBudget()) {
      context.markTerminated(CpqBuilderContext.TERMINATION_BUDGET_FINAL);
    }
  }
}
