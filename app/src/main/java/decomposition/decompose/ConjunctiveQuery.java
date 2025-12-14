package decomposition.decompose;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Public wrapper for decomposing a CQ.
 *
 * <p>This wrapper intentionally keeps using gmark's {@link CQ} as the underlying representation for
 * now. Construct directly with the gmark {@link CQ}; the goal is to provide a single entry point
 * ({@link #decompose(DecompositionMethod)}) while the internal code is incrementally reorganised.
 */
public final class ConjunctiveQuery {

  /** High-level decomposition algorithm selection. */
  public enum DecompositionMethod {
    /** Build exactly one decomposition where every CQ edge is its own component. */
    SINGLE_EDGE
    // Add more methods here later.
  }

  private final CQ gmarkCQ;

  public ConjunctiveQuery(CQ gmarkCQ) {
    this.gmarkCQ = Objects.requireNonNull(gmarkCQ, "gmarkCQ");
  }

  /** Returns the underlying gmark CQ (intended for internal use). */
  CQ gmarkCQ() {
    return gmarkCQ;
  }

  /**
   * Decomposes this CQ using {@link DecompositionMethod#SINGLE_EDGE}.
   *
   * <p>The result surface is intentionally minimal: a collection of CPQs (one per CQ edge).
   */
  public List<CPQ> decompose() {
    return decompose(DecompositionMethod.SINGLE_EDGE);
  }

  public List<CPQ> decompose(DecompositionMethod method) {
    Objects.requireNonNull(method, "method");
    return switch (method) {
      case SINGLE_EDGE -> decomposeSingleEdge();
    };
  }

  private List<CPQ> decomposeSingleEdge() {
    UniqueGraph<VarCQ, AtomCQ> graph = gmarkCQ.toQueryGraph().toUniqueGraph();
    List<CPQ> cpqs = new ArrayList<>(graph.getEdges().size());

    for (var edge : graph.getEdges()) {
      AtomCQ atom = edge.getData();
      CPQ cpq = CPQ.label(atom.getLabel());
      VarCQ source = edge.getSourceNode().getData();
      VarCQ target = edge.getTargetNode().getData();
      if (source.equals(target)) {
        cpq = CPQ.intersect(List.of(cpq, CPQ.id()));
      }
      cpqs.add(cpq);
    }

    return List.copyOf(cpqs);
  }
}
