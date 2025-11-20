package decomposition.builder;

import decomposition.DecompositionOptions;
import decomposition.builder.stages.AssemblyStage;
import decomposition.builder.stages.ComponentGenerationStage;
import decomposition.builder.stages.ExtractionStage;
import decomposition.builder.stages.PartitioningStage;
import decomposition.builder.stages.VerificationStage;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Pipeline orchestrator that wires together a linear list of {@link BuilderStage} steps operating
 * on a shared {@link CpqBuilderContext}.
 */
public final class CpqBuilder {
  private final List<BuilderStage> stages;

  public CpqBuilder(List<BuilderStage> stages) {
    if (stages == null || stages.isEmpty()) {
      throw new IllegalArgumentException("stages must not be empty");
    }
    this.stages = List.copyOf(stages);
  }

  public CpqBuilder(BuilderStage... stages) {
    this(List.of(stages));
  }

  public List<BuilderStage> stages() {
    return stages;
  }

  public static CpqBuilder defaultBuilder() {
    return new CpqBuilder(
        new ExtractionStage(),
        new PartitioningStage(),
        new ComponentGenerationStage(),
        new VerificationStage(),
        new AssemblyStage());
  }

  public CpqBuilderResult build(
      CQ query, Set<String> explicitFreeVariables, DecompositionOptions options) {
    CpqBuilderContext context = runStages(query, explicitFreeVariables, options);
    return CpqBuilderResult.fromContext(context);
  }

  public CpqBuilderContext buildContext(
      CQ query, Set<String> explicitFreeVariables, DecompositionOptions options) {
    return runStages(query, explicitFreeVariables, options);
  }

  private CpqBuilderContext runStages(
      CQ query, Set<String> explicitFreeVariables, DecompositionOptions options) {
    Objects.requireNonNull(query, "query");
    CpqBuilderContext context = new CpqBuilderContext(query, explicitFreeVariables, options);
    for (BuilderStage stage : stages) {
      if (context.isTerminated()) {
        break;
      }
      stage.execute(context);
    }
    return context;
  }
}
