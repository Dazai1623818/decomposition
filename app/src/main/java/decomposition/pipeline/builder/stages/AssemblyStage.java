package decomposition.pipeline.builder.stages;

import decomposition.pipeline.builder.BuilderStage;
import decomposition.pipeline.builder.CpqBuilderContext;
import decomposition.pipeline.generation.QueryAssembler;
import java.util.Objects;

/** Final stage that deduplicates catalogue entries and computes the global CPQ result. */
public final class AssemblyStage implements BuilderStage {
  private final QueryAssembler assemblerOverride;

  public AssemblyStage() {
    this(null);
  }

  public AssemblyStage(QueryAssembler assemblerOverride) {
    this.assemblerOverride = assemblerOverride;
  }

  @Override
  public void execute(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    if (context.isTerminated()) {
      return;
    }
    if (context.extraction() == null) {
      throw new IllegalStateException("Extraction must run before assembly.");
    }
    QueryAssembler assembler = assemblerOverride != null ? assemblerOverride : new QueryAssembler();
    assembler.assemble(context);
  }
}
