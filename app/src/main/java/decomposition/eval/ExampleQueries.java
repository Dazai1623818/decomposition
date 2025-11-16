package decomposition.eval;

import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Provides sample conjunctive queries expressed with the official gMark CQ API. */
final class ExampleQueries {
  private ExampleQueries() {}

  static ExampleQuery example1() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");

    cq.addAtom(a, new Predicate(1, "1"), b);
    cq.addAtom(b, new Predicate(2, "2"), c);
    // Additional edges can be introduced here for more complex examples.

    QueryDecomposition decomposition = buildLinearDecomposition(cq);
    return ExampleQuery.of(cq, decomposition);
  }

  private static QueryDecomposition buildLinearDecomposition(CQ cq) {
    Map<String, AtomCQ> atoms = atomsBySignature(cq);
    AtomCQ ab = atoms.get(signature("A", "1", "B"));
    AtomCQ bc = atoms.get(signature("B", "2", "C"));
    if (ab == null || bc == null) {
      throw new IllegalStateException("Example 1 expects atoms A-1->B and B-2->C to exist.");
    }
    QueryDecomposition.Bag child = QueryDecomposition.bag(List.of(bc));
    QueryDecomposition.Bag root = QueryDecomposition.bag(List.of(ab), List.of(child));
    return QueryDecomposition.of(root);
  }

  private static Map<String, AtomCQ> atomsBySignature(CQ cq) {
    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
    Map<String, AtomCQ> atoms = new HashMap<>();
    for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
      AtomCQ atom = edge.getData();
      atoms.put(signature(atom), atom);
    }
    return atoms;
  }

  private static String signature(String source, String labelAlias, String target) {
    return source + "-" + labelAlias + "-" + target;
  }

  private static String signature(AtomCQ atom) {
    return signature(
        atom.getSource().getName(), atom.getLabel().getAlias(), atom.getTarget().getName());
  }
}
