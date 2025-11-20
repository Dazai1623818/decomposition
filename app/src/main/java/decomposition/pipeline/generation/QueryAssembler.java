package decomposition.pipeline.generation;

import decomposition.core.model.Component;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.cpq.ComponentExpressionBuilder;
import decomposition.pipeline.builder.CpqBuilderContext;
import decomposition.util.JoinAnalysisBuilder;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Assembles global CPQ candidates and selects the final expression. */
public final class QueryAssembler {

  public AssemblyResult assemble(CpqBuilderContext context) {
    Objects.requireNonNull(context, "context");
    if (context.extraction() == null) {
      throw new IllegalStateException("Extraction must run before assembly.");
    }

    Set<String> globalJoinNodes =
        JoinAnalysisBuilder.analyzePartition(
                new Partition(List.of(new Component(context.fullBits(), context.vertices()))),
                context.extraction().freeVariables())
            .globalJoinNodes();
    List<CPQExpression> globalCandidates =
        context
            .assembler()
            .synthesizeGlobal(context.fullBits(), globalJoinNodes, context.varToNodeMap());
    CPQExpression finalExpression = selectPreferredFinalComponent(globalCandidates);

    List<CPQExpression> dedupCatalogue =
        ComponentExpressionBuilder.dedupeExpressions(context.recognizedCatalogue());
    context.setRecognizedCatalogue(dedupCatalogue);
    context.setGlobalCatalogue(globalCandidates);
    context.setFinalExpression(finalExpression);

    return new AssemblyResult(globalCandidates, finalExpression);
  }

  private CPQExpression selectPreferredFinalComponent(List<CPQExpression> rules) {
    return (rules == null || rules.isEmpty()) ? null : rules.get(0);
  }

  public record AssemblyResult(
      List<CPQExpression> globalCatalogue, CPQExpression finalExpression) {}
}
