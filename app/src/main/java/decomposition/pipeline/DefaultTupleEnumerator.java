package decomposition.pipeline;

import decomposition.cpq.CPQExpression;
import decomposition.pipeline.DecompositionPipelineState.SynthesisState;
import java.util.ArrayList;
import java.util.List;

/** Cartesian enumerator used when tuple enumeration is requested. */
public final class DefaultTupleEnumerator implements TupleEnumerator {
  @Override
  public List<List<CPQExpression>> enumerate(
      List<List<CPQExpression>> perComponent, SynthesisState state) {
    if (!state.wantTuples || perComponent == null || perComponent.isEmpty()) {
      return List.of();
    }
    return enumerateTuples(perComponent, state.tupleLimit);
  }

  private List<List<CPQExpression>> enumerateTuples(
      List<List<CPQExpression>> perComponent, int limit) {
    int n = perComponent.size();
    int[] idx = new int[n];
    List<List<CPQExpression>> out = new ArrayList<>();
    while (true) {
      List<CPQExpression> tuple = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        tuple.add(perComponent.get(i).get(idx[i]));
      }
      out.add(tuple);
      if (limit > 0 && out.size() >= limit) {
        break;
      }

      int p = n - 1;
      while (p >= 0) {
        idx[p]++;
        if (idx[p] < perComponent.get(p).size()) {
          break;
        }
        idx[p] = 0;
        p--;
      }
      if (p < 0) {
        break;
      }
    }
    return out;
  }
}
