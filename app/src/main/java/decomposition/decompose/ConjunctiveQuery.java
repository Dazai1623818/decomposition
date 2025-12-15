package decomposition.decompose;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
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

  public ConjunctiveQuery(CQ gmarkCQ) {
    this.gmarkCQ = Objects.requireNonNull(gmarkCQ, "gmarkCQ");
  }

  /** Returns the underlying gmark CQ (intended for internal use). */
  CQ gmarkCQ() {
    return gmarkCQ;
  }

  /** Decomposes this CQ using {@link Decomposer.DecompositionMethod#SINGLE_EDGE}. */
  public List<CPQ> decompose() {
    return decompose(Decomposer.DecompositionMethod.SINGLE_EDGE);
  }

  public List<CPQ> decompose(Decomposer.DecompositionMethod method) {
    Objects.requireNonNull(method, "method");
    return Decomposer.decompose(gmarkCQ, method);
  }
}
