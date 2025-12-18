package decomposition.decompose;

import decomposition.core.Component;
import decomposition.cpq.CPQExpression;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Performs CQ -> CPQ decompositions for gMark queries. */
public final class Decomposer {

  /** High-level decomposition strategy selection. */
  public enum DecompositionMethod {
    SINGLE_EDGE,
    EXHAUSTIVE_ENUMERATION
  }

  private Decomposer() {}

  public static List<List<CPQExpression>> decompose(CQ cq, DecompositionMethod method) {
    Objects.requireNonNull(cq, "cq");
    Objects.requireNonNull(method, "method");
    return decompose(new ConjunctiveQuery(cq), method);
  }

  static List<List<CPQExpression>> decompose(ConjunctiveQuery query, DecompositionMethod method) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(method, "method");

    return switch (method) {
      case SINGLE_EDGE -> singleEdgeDecompose(query);
      case EXHAUSTIVE_ENUMERATION -> ExhaustiveEnumerator.decompose(query);
    };
  }

  /** Single-edge decomposition: one CPQ per CQ edge label. */
  private static List<List<CPQExpression>> singleEdgeDecompose(ConjunctiveQuery query) {
    UniqueGraph<VarCQ, AtomCQ> graph = query.graph();
    List<CPQExpression> expressions = new ArrayList<>(graph.getEdges().size());

    for (var edge : graph.getEdges()) {
      AtomCQ atom = edge.getData();
      String sourceName = atom.getSource().getName();
      String targetName = atom.getTarget().getName();

      CPQ cpq = CPQ.label(atom.getLabel());
      if (sourceName.equals(targetName)) {
        cpq = CPQ.intersect(List.of(cpq, CPQ.id()));
      }

      // Minimal component metadata for evaluator compatibility.
      BitSet bits = new BitSet(1);
      bits.set(0);
      Set<String> vertices =
          sourceName.equals(targetName) ? Set.of(sourceName) : Set.of(sourceName, targetName);
      Component component = new Component(bits, vertices, Set.of(), null);
      expressions.add(
          new CPQExpression(cpq, component, sourceName, targetName, "Single-edge atom"));
    }

    return List.of(List.copyOf(expressions));
  }
}
