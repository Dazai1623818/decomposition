package decomposition.pipeline.builder.stages;

import decomposition.pipeline.builder.BuilderStage;
import decomposition.pipeline.builder.CpqBuilderContext;
import decomposition.pipeline.extract.CQExtractor;
import java.util.Objects;

/** Extracts the CQ into the neutral graph representation and seeds the builder context. */
public final class ExtractionStage implements BuilderStage {
  private final CQExtractor extractor;

  public ExtractionStage() {
    this(new CQExtractor());
  }

  public ExtractionStage(CQExtractor extractor) {
    this.extractor = Objects.requireNonNull(extractor, "extractor");
  }

  @Override
  public void execute(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    if (context.isTerminated()) {
      return;
    }
    context.applyExtraction(extractor.extract(context.query(), context.explicitFreeVariables()));
  }
}
