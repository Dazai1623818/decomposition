package decomposition.decompose;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Performs CQ -> CPQ decompositions for gMark queries. */
public final class Decomposer {

  /** High-level decomposition strategy selection. */
  public enum DecompositionMethod {
    SINGLE_EDGE,
    EXHAUSTIVE_ENUMERATION
  }

  private Decomposer() {}

  public static List<List<CPQ>> decompose(CQ cq, DecompositionMethod method) {
    Objects.requireNonNull(cq, "cq");
    Objects.requireNonNull(method, "method");
    return decompose(new ConjunctiveQuery(cq), method);
  }

  static List<List<CPQ>> decompose(ConjunctiveQuery query, DecompositionMethod method) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(method, "method");

    return switch (method) {
      case SINGLE_EDGE -> singleEdgeDecompose(query);
      case EXHAUSTIVE_ENUMERATION -> ExhaustiveEnumerator.decompose(query);
    };
  }

  /** Single-edge decomposition: one CPQ per CQ edge label. */
  private static List<List<CPQ>> singleEdgeDecompose(ConjunctiveQuery query) {
    UniqueGraph<VarCQ, AtomCQ> graph = query.graph();
    List<CPQ> cpqs = new ArrayList<>(graph.getEdges().size());

    for (var edge : graph.getEdges()) {
      CPQ cpq = CPQ.label(edge.getData().getLabel());
      VarCQ source = edge.getSourceNode().getData();
      VarCQ target = edge.getTargetNode().getData();
      if (source.equals(target)) {
        cpq = CPQ.intersect(List.of(cpq, CPQ.id()));
      }
      cpqs.add(cpq);
    }

    return List.of(List.copyOf(cpqs));
  }
}
