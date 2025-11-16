package decomposition.pipeline;

import decomposition.cpq.CPQExpression;
import decomposition.pipeline.DecompositionPipelineState.SynthesisState;
import java.util.List;

/** Enumerates tuples of per-component CPQ expressions. */
public interface TupleEnumerator {
  List<List<CPQExpression>> enumerate(List<List<CPQExpression>> perComponent, SynthesisState state);
}
