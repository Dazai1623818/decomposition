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

  private interface DecompositionStrategy {
    List<CPQ> decompose(CQ cq);
  }

  private Decomposer() {}

  public static List<CPQ> decompose(CQ cq, DecompositionMethod method) {
    Objects.requireNonNull(cq, "cq");
    Objects.requireNonNull(method, "method");
    return strategyFor(method).decompose(cq);
  }

  static List<CPQ> decompose(ConjunctiveQuery query, DecompositionMethod method) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(method, "method");

    return switch (method) {
      case SINGLE_EDGE -> strategyFor(method).decompose(query.gmarkCQ());
      case EXHAUSTIVE_ENUMERATION -> {
        ExhaustiveEnumerator.enumeratePartitions(query);
        yield List.of();
      }
    };
  }

  private static DecompositionStrategy strategyFor(DecompositionMethod method) {
    return switch (method) {
      case SINGLE_EDGE -> SingleEdge.INSTANCE;
      case EXHAUSTIVE_ENUMERATION -> ExhaustiveEnumeration.INSTANCE;
    };
  }

  /** Single-edge decomposition: one CPQ per CQ edge label. */
  private static final class SingleEdge implements DecompositionStrategy {
    private static final SingleEdge INSTANCE = new SingleEdge();

    private SingleEdge() {}

    @Override
    public List<CPQ> decompose(CQ cq) {
      UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
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

      return cpqs;
    }
  }

  /**
   * Exhaustive enumeration decomposition.
   */
  private static final class ExhaustiveEnumeration implements DecompositionStrategy {
    private static final ExhaustiveEnumeration INSTANCE = new ExhaustiveEnumeration();

    private ExhaustiveEnumeration() {}

    @Override
    public List<CPQ> decompose(CQ cq) {
      // Enumerate partitions; CPQ conversion will be layered on top in a later step.
      ExhaustiveEnumerator.enumeratePartitions(cq);
      return List.of();
    }
  }
}
