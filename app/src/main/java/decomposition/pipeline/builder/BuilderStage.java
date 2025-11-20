package decomposition.pipeline.builder;

/**
 * Simple pipeline stage contract used by {@link CpqBuilder} to process the mutable {@link
 * CpqBuilderContext}.
 */
public interface BuilderStage {
  void execute(CpqBuilderContext context);
}
