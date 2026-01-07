package decomposition.eval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.core.CPQExpression;
import decomposition.decompose.Decomposer;
import decomposition.decompose.Decomposer.DecompositionMethod;
import decomposition.eval.leapfrogjoiner.IntAccumulator;
import decomposition.eval.leapfrogjoiner.LeapfrogJoiner;
import decomposition.eval.leapfrogjoiner.RelationBinding;
import decomposition.eval.leapfrogjoiner.RelationBinding.RelationProjection;
import decomposition.testing.TestDefaults;
import dev.roanh.cpqindex.IndexUtil;
import dev.roanh.gmark.eval.PathQuery;
import dev.roanh.gmark.eval.ReachabilityQueryEvaluator;
import dev.roanh.gmark.eval.ResultGraph;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.IntGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class EvaluatorComparisonTest {

  private static final String GRAPH_PATH_PROPERTY = "decomposition.graphPath";
  private static final Path DEFAULT_GRAPH_PATH = Path.of("graphs", "robotssmall.edge");
  private static final int INDEX_K = 2;
  private static final int REQUIRED_LABEL_MAX = 7;
  private static final int RANDOM_CPQ_OPERATIONS = 10;
  private static final int RANDOM_CPQ_COUNT = 20;

  @Test
  void decompositionMatchesGmark() throws Exception {
    Path graphPath = resolvePath(GRAPH_PATH_PROPERTY, DEFAULT_GRAPH_PATH);
    assertTrue(Files.exists(graphPath), "Missing graph file: " + graphPath);
    UniqueGraph<Integer, Predicate> graph = IndexUtil.readGraph(graphPath);
    IntGraph intGraph = toIntGraph(graph, REQUIRED_LABEL_MAX);
    ReachabilityQueryEvaluator gmarkEvaluator = new ReachabilityQueryEvaluator(intGraph);

    int decompositionLimit = TestDefaults.tupleLimit();

    try (EvaluationIndex index = new EvaluationIndex(graphPath, INDEX_K)) {
      LeapfrogJoiner joiner = new LeapfrogJoiner();
      List<CQ> queries = queries();
      for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
        CQ query = queries.get(queryIndex);
        int queryNumber = queryIndex + 1;
        List<List<CPQExpression>> decompositions =
            Decomposer.decompose(
                query, DecompositionMethod.CPQ_K_ENUMERATION, INDEX_K, decompositionLimit);
        assertFalse(decompositions.isEmpty(), () -> "No decompositions for query " + queryNumber);

        List<String> freeVars = normalizedFreeVars(query);
        int ordinal = 1;
        for (List<CPQExpression> tuple : decompositions) {
          int decompositionOrdinal = ordinal;
          int componentOrdinal = 1;
          for (CPQExpression expression : tuple) {
            if (expression == null) {
              componentOrdinal++;
              continue;
            }
            Set<Map<String, Integer>> indexPairs =
                evaluateExpressionPairsWithIndex(index, expression);
            Set<Map<String, Integer>> gmarkPairs =
                evaluateExpressionPairsWithGmark(expression, gmarkEvaluator);

            int finalComponentOrdinal = componentOrdinal;
            int finalDecompositionOrdinal = decompositionOrdinal;
            assertEquals(
                gmarkPairs,
                indexPairs,
                () ->
                    "Mismatch for query "
                        + queryNumber
                        + " decomposition "
                        + finalDecompositionOrdinal
                        + "/"
                        + decompositions.size()
                        + " component "
                        + finalComponentOrdinal
                        + "/"
                        + tuple.size()
                        + ": "
                        + expression.cpq());
            componentOrdinal++;
          }

          List<Map<String, Integer>> indexRows = index.evaluateDecomposition(tuple);
          List<Map<String, Integer>> gmarkRows = evaluateWithGmark(tuple, gmarkEvaluator, joiner);
          Set<Map<String, Integer>> indexProjected = projectResults(indexRows, freeVars);
          Set<Map<String, Integer>> gmarkProjected = projectResults(gmarkRows, freeVars);

          int finalOrdinal = decompositionOrdinal;
          assertEquals(
              gmarkProjected,
              indexProjected,
              () ->
                  "Mismatch for "
                      + "query "
                      + queryNumber
                      + " decomposition "
                      + finalOrdinal
                      + "/"
                      + decompositions.size());
          ordinal++;
        }
      }
    }
  }

  @Test
  void randomCpqDecompositionMatchesGmarkEvaluation() throws Exception {
    Path graphPath = resolvePath(GRAPH_PATH_PROPERTY, DEFAULT_GRAPH_PATH);
    assertTrue(Files.exists(graphPath), "Missing graph file: " + graphPath);
    UniqueGraph<Integer, Predicate> graph = IndexUtil.readGraph(graphPath);
    IntGraph intGraph = toIntGraph(graph, REQUIRED_LABEL_MAX);
    ReachabilityQueryEvaluator gmarkEvaluator = new ReachabilityQueryEvaluator(intGraph);
    int labelCount = Math.max(1, maxLabelInGraph(graph) + 1);

    int decompositionLimit = TestDefaults.tupleLimit();
    LeapfrogJoiner joiner = new LeapfrogJoiner();
    for (int cpqIndex = 0; cpqIndex < RANDOM_CPQ_COUNT; cpqIndex++) {
      CPQ cpq = CPQ.generateRandomCPQ(RANDOM_CPQ_OPERATIONS, labelCount);
      CQ cq = cpq.toCQ();
      List<String> freeVars = normalizedFreeVars(cq);
      Set<Map<String, Integer>> baseline = evaluateCpqWithGmark(cpq, gmarkEvaluator, freeVars);

      List<List<CPQExpression>> decompositions =
          Decomposer.decompose(
              cq, DecompositionMethod.CPQ_K_ENUMERATION, INDEX_K, decompositionLimit);
      int cpqNumber = cpqIndex + 1;
      assertFalse(decompositions.isEmpty(), () -> "No decompositions for random CPQ " + cpqNumber);

      int ordinal = 1;
      for (List<CPQExpression> tuple : decompositions) {
        List<Map<String, Integer>> joinedRows = evaluateWithGmark(tuple, gmarkEvaluator, joiner);
        Set<Map<String, Integer>> joinedProjected = projectResults(joinedRows, freeVars);

        int finalOrdinal = ordinal;
        assertEquals(
            baseline,
            joinedProjected,
            () ->
                "Mismatch for random CPQ "
                    + cpqNumber
                    + " decomposition "
                    + finalOrdinal
                    + "/"
                    + decompositions.size()
                    + ": "
                    + cpq);
        ordinal++;
      }
    }
  }

  private static List<CQ> queries() {
    return List.of(
        starOut5(),
        diamondBranch(),
        pathBranchMid(),
        twoStarsBridge(),
        cycleWithBranches(),
        multiEdgeHub());
  }

  // ---- Query definitions copied from HardQueries ----

  private static CQ starOut5() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(a, label(3), d);
    cq.addAtom(a, label(4), e);
    cq.addAtom(a, label(5), f);

    return cq;
  }

  private static CQ diamondBranch() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(b, label(3), d);
    cq.addAtom(c, label(4), d);
    cq.addAtom(d, label(5), e);
    cq.addAtom(d, label(6), f);

    return cq;
  }

  private static CQ pathBranchMid() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(c, label(4), e);
    cq.addAtom(c, label(5), f);

    return cq;
  }

  private static CQ twoStarsBridge() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");
    VarCQ g = cq.addBoundVariable("G");
    VarCQ h = cq.addBoundVariable("H");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), c);
    cq.addAtom(b, label(3), d);
    cq.addAtom(d, label(4), e);
    cq.addAtom(d, label(5), f);
    cq.addAtom(e, label(6), g);
    cq.addAtom(f, label(7), h);

    return cq;
  }

  private static CQ cycleWithBranches() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");
    VarCQ e = cq.addBoundVariable("E");
    VarCQ f = cq.addBoundVariable("F");

    cq.addAtom(a, label(1), b);
    cq.addAtom(b, label(2), c);
    cq.addAtom(c, label(3), d);
    cq.addAtom(d, label(4), a);
    cq.addAtom(a, label(5), e);
    cq.addAtom(c, label(6), f);

    return cq;
  }

  private static CQ multiEdgeHub() {
    CQ cq = CQ.empty();
    VarCQ a = cq.addFreeVariable("A");
    VarCQ b = cq.addBoundVariable("B");
    VarCQ c = cq.addBoundVariable("C");
    VarCQ d = cq.addBoundVariable("D");

    cq.addAtom(a, label(1), b);
    cq.addAtom(a, label(2), b);
    cq.addAtom(a, label(3), b);
    cq.addAtom(b, label(4), c);
    cq.addAtom(b, label(5), d);

    return cq;
  }

  private static Predicate label(int id) {
    return new Predicate(id, String.valueOf(id));
  }

  private static Path resolvePath(String property, Path defaultValue) {
    String raw = System.getProperty(property);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return Path.of(raw);
  }

  private static IntGraph toIntGraph(UniqueGraph<Integer, Predicate> graph, int requiredLabelMax) {
    int nodeCount = graph.getNodeCount();
    int maxLabel = Math.max(requiredLabelMax, maxLabelInGraph(graph));
    IntGraph intGraph = new IntGraph(nodeCount, maxLabel + 1);
    for (GraphEdge<Integer, Predicate> edge : graph.getEdges()) {
      intGraph.addEdge(edge.getSource(), edge.getTarget(), edge.getData().getID());
    }
    return intGraph;
  }

  private static int maxLabelInGraph(UniqueGraph<Integer, Predicate> graph) {
    int maxLabel = -1;
    for (GraphEdge<Integer, Predicate> edge : graph.getEdges()) {
      maxLabel = Math.max(maxLabel, edge.getData().getID());
    }
    return maxLabel;
  }

  private static Set<Map<String, Integer>> evaluateCpqWithGmark(
      CPQ cpq, ReachabilityQueryEvaluator evaluator, List<String> freeVars) {
    ResultGraph result = evaluator.evaluate(PathQuery.of(cpq));
    List<dev.roanh.gmark.data.SourceTargetPair> pairs = result.getSourceTargetPairs();
    if (pairs.isEmpty()) {
      return Set.of();
    }
    if (freeVars.isEmpty()) {
      return Set.of(Map.of());
    }
    if (freeVars.size() > 2) {
      throw new IllegalArgumentException(
          "Expected at most 2 free variables for CPQ evaluation, got " + freeVars);
    }

    Set<Map<String, Integer>> deduped = new LinkedHashSet<>();
    String sourceVar = freeVars.get(0);
    String targetVar = freeVars.size() == 1 ? null : freeVars.get(1);
    for (var pair : pairs) {
      LinkedHashMap<String, Integer> row = new LinkedHashMap<>();
      row.put(sourceVar, pair.source());
      if (targetVar != null) {
        row.put(targetVar, pair.target());
      }
      deduped.add(Map.copyOf(row));
    }
    return Set.copyOf(deduped);
  }

  private static Set<Map<String, Integer>> evaluateExpressionPairsWithGmark(
      CPQExpression expression, ReachabilityQueryEvaluator evaluator) {
    ResultGraph result = evaluator.evaluate(PathQuery.of(expression.cpq()));
    List<dev.roanh.gmark.data.SourceTargetPair> pairs = result.getSourceTargetPairs();
    if (pairs.isEmpty()) {
      return Set.of();
    }

    List<String> endpointVars = normalizedEndpointVars(expression);
    String left = endpointVars.get(0);
    String right = endpointVars.size() == 1 ? null : endpointVars.get(1);

    Set<Map<String, Integer>> deduped = new LinkedHashSet<>();
    for (var pair : pairs) {
      if (right == null) {
        if (pair.source() != pair.target()) {
          continue;
        }
        deduped.add(Map.of(left, pair.source()));
        continue;
      }
      LinkedHashMap<String, Integer> row = new LinkedHashMap<>();
      row.put(left, pair.source());
      row.put(right, pair.target());
      deduped.add(Map.copyOf(row));
    }
    return Set.copyOf(deduped);
  }

  private static Set<Map<String, Integer>> evaluateExpressionPairsWithIndex(
      EvaluationIndex index, CPQExpression expression) {
    List<Map<String, Integer>> rows = index.evaluateDecomposition(List.of(expression));
    return projectResults(rows, normalizedEndpointVars(expression));
  }

  private static List<Map<String, Integer>> evaluateWithGmark(
      List<CPQExpression> tuple, ReachabilityQueryEvaluator evaluator, LeapfrogJoiner joiner) {
    Objects.requireNonNull(tuple, "tuple");
    List<RelationBinding> relations = new ArrayList<>(tuple.size());

    for (CPQExpression expression : tuple) {
      if (expression == null) {
        continue;
      }
      RelationBinding binding = relationFromGmark(expression, evaluator);
      if (binding == null) {
        return List.of();
      }
      relations.add(binding);
    }

    if (relations.isEmpty()) {
      return List.of();
    }
    return joiner.join(relations);
  }

  private static RelationBinding relationFromGmark(
      CPQExpression expression, ReachabilityQueryEvaluator evaluator) {
    ResultGraph result = evaluator.evaluate(PathQuery.of(expression.cpq()));
    String left = normalizeVar(endpointVariable(expression, expression.source()));
    String right = normalizeVar(endpointVariable(expression, expression.target()));

    if (left.equals(right)) {
      Set<Integer> values = new LinkedHashSet<>();
      for (var pair : result.getSourceTargetPairs()) {
        if (pair.source() == pair.target()) {
          values.add(pair.source());
        }
      }
      if (values.isEmpty()) {
        return null;
      }
      int[] domain = values.stream().mapToInt(Integer::intValue).sorted().toArray();
      return RelationBinding.unary(left, expression.derivation(), domain);
    }

    Map<Integer, IntAccumulator> forward = new HashMap<>();
    Map<Integer, IntAccumulator> reverse = new HashMap<>();

    for (var pair : result.getSourceTargetPairs()) {
      forward.computeIfAbsent(pair.source(), ignored -> new IntAccumulator()).add(pair.target());
      reverse.computeIfAbsent(pair.target(), ignored -> new IntAccumulator()).add(pair.source());
    }

    if (forward.isEmpty() || reverse.isEmpty()) {
      return null;
    }

    Map<Integer, int[]> forwardMap = toIntArrayMap(forward);
    Map<Integer, int[]> reverseMap = toIntArrayMap(reverse);
    RelationProjection projection =
        new RelationProjection(
            sortedKeys(forward.keySet()), sortedKeys(reverse.keySet()), forwardMap, reverseMap);
    if (projection.isEmpty()) {
      return null;
    }

    return RelationBinding.binary(left, right, expression.derivation(), projection);
  }

  private static String endpointVariable(CPQExpression expression, String node) {
    if (expression.component() != null) {
      String mapped = expression.component().getVarForNode(node);
      if (mapped != null) {
        return mapped;
      }
    }
    return node;
  }

  private static String normalizeVar(String raw) {
    if (raw == null) {
      throw new IllegalArgumentException("Variable name must be non-null");
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("Variable name must be non-empty");
    }
    return trimmed.startsWith("?") ? trimmed : ("?" + trimmed);
  }

  private static List<String> normalizedFreeVars(CQ cq) {
    return cq.getFreeVariables().stream().map(var -> normalizeVar(var.getName())).toList();
  }

  private static List<String> normalizedEndpointVars(CPQExpression expression) {
    String left = normalizeVar(endpointVariable(expression, expression.source()));
    String right = normalizeVar(endpointVariable(expression, expression.target()));
    if (left.equals(right)) {
      return List.of(left);
    }
    return List.of(left, right);
  }

  private static Set<Map<String, Integer>> projectResults(
      List<Map<String, Integer>> rows, List<String> freeVars) {
    if (rows.isEmpty()) {
      return Set.of();
    }
    if (freeVars.isEmpty()) {
      return Set.of(Map.of());
    }
    Set<Map<String, Integer>> deduped = new LinkedHashSet<>();
    for (Map<String, Integer> row : rows) {
      Map<String, Integer> projected = projectRow(row, freeVars);
      if (projected != null) {
        deduped.add(projected);
      }
    }
    return Set.copyOf(deduped);
  }

  private static Map<String, Integer> projectRow(Map<String, Integer> row, List<String> freeVars) {
    LinkedHashMap<String, Integer> projected = new LinkedHashMap<>();
    for (String var : freeVars) {
      Integer value = row.get(var);
      if (value == null) {
        return null;
      }
      projected.put(var, value);
    }
    return Map.copyOf(projected);
  }

  private static int[] sortedKeys(Set<Integer> keys) {
    if (keys.isEmpty()) {
      return new int[0];
    }
    int[] sorted = keys.stream().mapToInt(Integer::intValue).toArray();
    Arrays.sort(sorted);
    return sorted;
  }

  private static Map<Integer, int[]> toIntArrayMap(Map<Integer, IntAccumulator> input) {
    Map<Integer, int[]> result = new HashMap<>(input.size());
    for (Map.Entry<Integer, IntAccumulator> entry : input.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toSortedDistinctArray());
    }
    return result;
  }
}
