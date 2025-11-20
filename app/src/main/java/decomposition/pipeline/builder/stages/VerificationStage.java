package decomposition.pipeline.builder.stages;

import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.pipeline.builder.BuilderStage;
import decomposition.pipeline.builder.CpqBuilderContext;
import decomposition.pipeline.builder.diagnostics.VerificationService;
import java.util.List;
import java.util.Objects;

/** Optionally performs deep verification and records diagnostics. */
public final class VerificationStage implements BuilderStage {
  private final VerificationService verificationService;

  public VerificationStage() {
    this(new VerificationService());
  }

  public VerificationStage(VerificationService verificationService) {
    this.verificationService = Objects.requireNonNull(verificationService, "verificationService");
  }

  @Override
  public void execute(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    if (context.isTerminated()) {
      return;
    }
    if (!context.options().deepVerification()) {
      return;
    }

    List<PartitionDiagnostic> issues = verificationService.verify(context);
    if (!issues.isEmpty()) {
      context.addDiagnostics(issues);
    }
  }
}
