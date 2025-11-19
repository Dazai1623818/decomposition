package decomposition.pipeline;

import decomposition.DecompositionOptions;
import decomposition.cpq.CPQExpression;
import decomposition.diagnostics.PartitionDiagnostic;
import decomposition.partitions.FilteredPartition;
import decomposition.pipeline.DecompositionPipelineState.GlobalResult;
import decomposition.pipeline.DecompositionPipelineState.PipelineContext;
import decomposition.pipeline.DecompositionPipelineState.SynthesisState;
import decomposition.util.Timing;
import java.util.List;

/** Synthesizes CPQ expressions for filtered partitions and global candidates. */
public interface CpqSynthesizer {
  SynthesisState createState(PipelineContext context, DecompositionOptions options);

  PartitionSynthesisResult processPartition(
      FilteredPartition partition,
      PipelineContext context,
      DecompositionOptions options,
      SynthesisState state,
      TupleEnumerator tupleEnumerator,
      List<PartitionDiagnostic> diagnostics,
      int partitionIndex);

  GlobalResult computeGlobalResult(
      PipelineContext context, DecompositionOptions options, SynthesisState state);

  boolean overBudget(DecompositionOptions options, Timing timing);

  boolean overBudget(DecompositionOptions options, long elapsedMillis);

  List<CPQExpression> dedupeCatalogue(List<CPQExpression> expressions);
}
