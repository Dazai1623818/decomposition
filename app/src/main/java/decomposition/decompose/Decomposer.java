package decomposition.decompose;

import decomposition.core.CPQExpression;
import decomposition.core.Component;
import decomposition.core.ConjunctiveQuery;
import decomposition.decompose.cpqk.CpqkEnumerator;
import decomposition.decompose.exhaustive.ExhaustiveEnumerator;
import decomposition.eval.EvaluationRun;
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
    EXHAUSTIVE_ENUMERATION,
    /** Exhaustive enumeration with parallel processing. */
    EXHAUSTIVE_PARALLEL,
    /** CPQ-k enumeration (no tuple-level dedup). */
    CPQ_K_ENUMERATION
  }

  private static final int UNBOUNDED_K = Integer.MAX_VALUE;
  private static final int UNBOUNDED_LIMIT = 0;

  private Decomposer() {}

  public static List<List<CPQExpression>> decompose(CQ cq, DecompositionMethod method) {
    return decompose(cq, method, UNBOUNDED_K, UNBOUNDED_LIMIT);
  }

  public static List<List<CPQExpression>> decompose(
      CQ cq, DecompositionMethod method, int k, int limit) {
    Objects.requireNonNull(cq, "cq");
    Objects.requireNonNull(method, "method");
    return decompose(new ConjunctiveQuery(cq), method, k, limit);
  }

  public static List<List<CPQExpression>> decompose(
      ConjunctiveQuery query, DecompositionMethod method) {
    return decompose(query, method, UNBOUNDED_K, UNBOUNDED_LIMIT);
  }

  public static List<List<CPQExpression>> decompose(
      ConjunctiveQuery query, DecompositionMethod method, int k, int limit) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(method, "method");

    return switch (method) {
      case SINGLE_EDGE -> singleEdgeDecompose(query);
      case EXHAUSTIVE_ENUMERATION ->
          ExhaustiveEnumerator.decompose(
                  query, ExhaustiveEnumerator.Config.sequential().withDiameterCap(k))
              .decompositions();
      case EXHAUSTIVE_PARALLEL ->
          ExhaustiveEnumerator.decompose(
                  query, ExhaustiveEnumerator.Config.parallel().withDiameterCap(k))
              .decompositions();
      case CPQ_K_ENUMERATION -> CpqkEnumerator.decompose(query, k, limit).decompositions();
    };
  }

  /**
   * Decomposes a CQ and returns a run record with timings and results.
   *
   * <p>Only available for exhaustive methods and CPQ-k enumeration.
   */
  public static EvaluationRun decomposeWithRun(CQ cq, DecompositionMethod method) {
    return decomposeWithRun(cq, method, UNBOUNDED_K, UNBOUNDED_LIMIT);
  }

  public static EvaluationRun decomposeWithRun(
      CQ cq, DecompositionMethod method, int k, int limit) {
    Objects.requireNonNull(cq, "cq");
    Objects.requireNonNull(method, "method");
    return decomposeWithRun(new ConjunctiveQuery(cq), method, k, limit);
  }

  public static EvaluationRun decomposeWithRun(ConjunctiveQuery query, DecompositionMethod method) {
    return decomposeWithRun(query, method, UNBOUNDED_K, UNBOUNDED_LIMIT);
  }

  public static EvaluationRun decomposeWithRun(
      ConjunctiveQuery query, DecompositionMethod method, int k, int limit) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(method, "method");

    return switch (method) {
      case EXHAUSTIVE_ENUMERATION ->
          ExhaustiveEnumerator.decompose(
              query, ExhaustiveEnumerator.Config.sequential().withDiameterCap(k));
      case EXHAUSTIVE_PARALLEL ->
          ExhaustiveEnumerator.decompose(
              query, ExhaustiveEnumerator.Config.parallel().withDiameterCap(k));
      case CPQ_K_ENUMERATION -> CpqkEnumerator.decompose(query, k, limit);
      default ->
          throw new IllegalArgumentException(
              "Run data only available for EXHAUSTIVE_ENUMERATION, EXHAUSTIVE_PARALLEL, or"
                  + " CPQ_K_ENUMERATION");
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
