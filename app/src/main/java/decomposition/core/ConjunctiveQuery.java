package decomposition.core;

import decomposition.decompose.Decomposer;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.List;
import java.util.Objects;

/**
 * Public wrapper for decomposing a CQ.
 *
 * <p>This wrapper intentionally keeps using gmark's {@link CQ} as the underlying representation for
 * now. Construct directly with the gmark {@link CQ}; the goal is to provide a single entry point
 * ({@link #decompose(Decomposer.DecompositionMethod)}) while the internal code is incrementally
 * reorganised.
 */
public final class ConjunctiveQuery {

  private final CQ gmarkCQ;
  private UniqueGraph<VarCQ, AtomCQ> graph;

  public ConjunctiveQuery(CQ gmarkCQ) {
    this.gmarkCQ = Objects.requireNonNull(gmarkCQ, "gmarkCQ");
  }

  /** Returns the underlying gmark CQ (intended for internal use). */
  public CQ gmarkCQ() {
    return gmarkCQ;
  }

  /** Decomposes this CQ using {@link Decomposer.DecompositionMethod#SINGLE_EDGE}. */
  public List<List<CPQExpression>> decompose() {
    return decompose(Decomposer.DecompositionMethod.SINGLE_EDGE);
  }

  public List<List<CPQExpression>> decompose(Decomposer.DecompositionMethod method) {
    Objects.requireNonNull(method, "method");
    return Decomposer.decompose(this, method);
  }

  public List<List<CPQExpression>> decompose(
      Decomposer.DecompositionMethod method, int k, int limit) {
    Objects.requireNonNull(method, "method");
    return Decomposer.decompose(this, method, k, limit);
  }

  // ----- Internal graph access -----

  public UniqueGraph<VarCQ, AtomCQ> graph() {
    if (graph == null) {
      graph = gmarkCQ.toQueryGraph().toUniqueGraph();
    }
    return graph;
  }
}
