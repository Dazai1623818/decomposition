package decomposition.eval;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.IndexUtil;
import dev.roanh.cpqindex.Pair;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.eval.PathQuery;
import dev.roanh.gmark.eval.ReachabilityQueryEvaluator;
import dev.roanh.gmark.eval.ResultGraph;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.IntGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Verifies composed CPQ components by comparing against manual single-edge joins. */
public class SimpleComponentVerifier {

  private static final int REQUIRED_LABEL_MAX = 4;
  private static final int INDEX_DIAMETER = 3;

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: SimpleComponentVerifier <graph-file>");
      System.exit(1);
    }

    Path graphPath = Path.of(args[0]);
    System.out.println("Loading graph from: " + graphPath);
    Path libPath = Path.of("").toAbsolutePath().resolve("lib/libnauty.so");
    System.out.println("Loading nauty from: " + libPath);
    System.load(libPath.toString());

    UniqueGraph<Integer, Predicate> graph = IndexUtil.readGraph(graphPath);
    List<Predicate> alphabet = buildAlphabet(graph, REQUIRED_LABEL_MAX);
    Index index =
        new Index(
            graph,
            INDEX_DIAMETER,
            true,
            true,
            Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
            2,
            ProgressListener.LOG);
    index.sort();
    IntGraph intGraph = toIntGraph(graph);
    ReachabilityQueryEvaluator rqEval = new ReachabilityQueryEvaluator(intGraph);
    System.out.println("Index ready\n");

    System.out.println("=== Test 0: Single edges vs gMark ===");
    for (int labelId = 1; labelId <= 4; labelId++) {
      CPQ edge = label(labelId, alphabet);
      List<Pair> edgeResults = index.query(edge);
      ResultGraph edgeRg = rqEval.evaluate(PathQuery.of(edge));
      Set<ResultPair> gmarkPairs = fromResultGraph(edgeRg);
      System.out.println("Label " + labelId + " index result count: " + edgeResults.size());
      printSample("  Index results", edgeResults, 5);
      System.out.println("Label " + labelId + " gMark result count: " + gmarkPairs.size());
      printSample("  gMark results", new ArrayList<>(gmarkPairs), 5);
      compareResultPairs(
          "Label " + labelId + " index vs gMark", toResultPairSet(edgeResults), gmarkPairs);
      System.out.println();
    }

    // Test: (3◦4) vs querying 3 and 4 separately and joining
    System.out.println("=== Test 1: (3◦4) ===");
    CPQ composed34 = CPQ.concat(label(3, alphabet), label(4, alphabet));
    System.out.println("Composed CPQ: " + composed34);
    List<Pair> result34 = index.query(composed34);
    System.out.println("Composed result count: " + result34.size());
    printSample("Composed results", result34, 10);

    // Now query 3 and 4 separately
    List<Pair> result3 = index.query(label(3, alphabet));
    List<Pair> result4 = index.query(label(4, alphabet));
    System.out.println("\nLabel 3 result count: " + result3.size());
    System.out.println("Label 4 result count: " + result4.size());

    // Manual join: find pairs where 3's target matches 4's source
    List<ResultPair> manual34 = manualJoin(result3, result4);
    System.out.println("Manual join result count: " + manual34.size());
    printSample("Manual join results", manual34, 10);

    // Compare
    compare("(3◦4)", result34, manual34);

    System.out.println("\n=== Test 2: (1◦2) ===");
    CPQ composed12 = CPQ.concat(label(1, alphabet), label(2, alphabet));
    List<Pair> result12 = index.query(composed12);
    System.out.println("Composed result count: " + result12.size());

    List<Pair> result1 = index.query(label(1, alphabet));
    List<Pair> result2 = index.query(label(2, alphabet));
    List<ResultPair> manual12 = manualJoin(result1, result2);
    System.out.println("Manual join result count: " + manual12.size());

    compare("(1◦2)", result12, manual12);

    System.out.println("\n=== Test 3: (2◦4⁻) with INVERSE ===");
    String expr24inv = alphabet.get(2).getAlias() + "◦" + alphabet.get(4).getInverse().getAlias();
    CPQ composed24inv = CPQ.parse(expr24inv, alphabet);
    System.out.println("Composed CPQ: " + composed24inv);
    List<Pair> result24inv = index.query(composed24inv);
    System.out.println("Composed result count: " + result24inv.size());
    printSample("Composed (2◦4⁻) results", result24inv, 10);

    // Manual: 2(x,y) AND 4(z,y) should give us (x,z)
    List<ResultPair> manual24inv = manualJoinWithInverse(result2, result4);
    System.out.println("Manual join result count: " + manual24inv.size());
    printSample("Manual (2◦4⁻) results", manual24inv, 10);

    compare("(2◦4⁻)", result24inv, manual24inv);

    System.out.println("\n=== Test 4: Intersection of (1◦2) and (3◦4) ===");
    // This simulates Partition 6
    // (1◦2)(A,C) AND (3◦4)(A,C)

    System.out.println("Set 1: (1◦2) size " + result12.size());
    System.out.println("Set 2: (3◦4) size " + result34.size());

    Set<ResultPair> set1 = new HashSet<>();
    for (Pair p : result12) set1.add(new ResultPair(p.getSource(), p.getTarget()));

    Set<ResultPair> set2 = new HashSet<>();
    for (Pair p : result34) set2.add(new ResultPair(p.getSource(), p.getTarget()));

    Set<ResultPair> intersection = new HashSet<>(set1);
    intersection.retainAll(set2);

    System.out.println("Intersection size: " + intersection.size());
    printSample("Intersection results", new ArrayList<>(intersection), 10);

    // Direct CPQ intersection via the native index
    CPQ intersectionCpq = CPQ.intersect(List.of(composed12, composed34));
    List<Pair> intersectionCpqResults = index.query(intersectionCpq);
    System.out.println("CPQ intersection result count: " + intersectionCpqResults.size());
    printSample("CPQ intersection results", intersectionCpqResults, 10);

    // Compare with full baseline manual join
    // 1(A,B) & 2(B,C) & 3(A,D) & 4(D,C)
    // We already have manual12 = 1(A,B) & 2(B,C) -> (A,C)
    // We already have manual34 = 3(A,D) & 4(D,C) -> (A,C) (Wait, manual34 was 3 &
    // 4. 3 is A->D, 4 is D->C. Yes.)

    Set<ResultPair> manualIntersection = new HashSet<>(manual12);
    manualIntersection.retainAll(manual34);

    System.out.println("Manual Baseline Intersection size: " + manualIntersection.size());

    if (intersection.size() == manualIntersection.size()) {
      System.out.println("✅ MATCH: CPQ intersection matches Manual Baseline intersection!");
    } else {
      System.out.println(
          "❌ MISMATCH: CPQ intersection ("
              + intersection.size()
              + ") != Manual Baseline ("
              + manualIntersection.size()
              + ")");
    }

    compare("(1◦2) ∩ (3◦4) via CPQ", intersectionCpqResults, new ArrayList<>(manualIntersection));

    // Evaluate the same intersection via gMark's reachability evaluator
    ResultGraph rg = rqEval.evaluate(PathQuery.of(intersectionCpq));
    List<ResultPair> reachabilityPairs = new ArrayList<>();
    for (dev.roanh.gmark.data.SourceTargetPair st : rg.getSourceTargetPairs()) {
      reachabilityPairs.add(new ResultPair(st.source(), st.target()));
    }
    System.out.println("gMark reachability intersection size: " + reachabilityPairs.size());
    printSample("gMark intersection results", reachabilityPairs, 10);
    compareResultPairs(
        "(1◦2) ∩ (3◦4) via gMark",
        new HashSet<>(reachabilityPairs),
        new HashSet<>(manualIntersection));

    System.out.println("\n=== Test 5: ((1◦2◦3◦4) ∩ id) ===");
    CPQ concat1234 =
        CPQ.concat(
            List.of(
                label(1, alphabet), label(2, alphabet), label(3, alphabet), label(4, alphabet)));
    CPQ closedWalk = CPQ.intersect(List.of(concat1234, CPQ.id()));
    ResultGraph closedRg = rqEval.evaluate(PathQuery.of(closedWalk));
    List<ResultPair> closedReachability = new ArrayList<>();
    for (dev.roanh.gmark.data.SourceTargetPair st : closedRg.getSourceTargetPairs()) {
      closedReachability.add(new ResultPair(st.source(), st.target()));
    }
    System.out.println("gMark closed-walk result count: " + closedReachability.size());
    printSample("gMark closed-walk results", closedReachability, 10);
    if (closedWalk.getDiameter() > INDEX_DIAMETER) {
      System.out.println(
          "Skipping native index eval: diameter "
              + closedWalk.getDiameter()
              + " exceeds k="
              + INDEX_DIAMETER);
    } else {
      List<Pair> closedResults = index.query(closedWalk);
      System.out.println("Closed-walk CPQ result count: " + closedResults.size());
      printSample("Closed-walk CPQ results", closedResults, 10);
      compareResultPairs(
          "((1◦2◦3◦4) ∩ id) via CPQ vs gMark",
          toResultPairSet(closedResults),
          new HashSet<>(closedReachability));
    }
  }

  private static List<Predicate> buildAlphabet(
      UniqueGraph<Integer, Predicate> graph, int requiredMaxLabel) {
    int maxLabel = Math.max(requiredMaxLabel, maxLabelInGraph(graph));
    Map<Integer, Predicate> forwardPredicates = new HashMap<>();
    for (GraphEdge<Integer, Predicate> edge : graph.getEdges()) {
      Predicate predicate = edge.getData();
      Predicate forward = predicate.isInverse() ? predicate.getInverse() : predicate;
      forwardPredicates.putIfAbsent(
          forward.getID(), new Predicate(forward.getID(), forward.getAlias()));
    }

    List<Predicate> alphabet = new ArrayList<>(maxLabel + 1);
    for (int i = 0; i <= maxLabel; i++) {
      alphabet.add(null);
    }
    for (Map.Entry<Integer, Predicate> entry : forwardPredicates.entrySet()) {
      alphabet.set(entry.getKey(), entry.getValue());
    }
    for (int i = 0; i <= maxLabel; i++) {
      if (alphabet.get(i) == null) {
        alphabet.set(i, new Predicate(i, String.valueOf(i)));
      }
    }
    return alphabet;
  }

  private static int maxLabelInGraph(UniqueGraph<Integer, Predicate> graph) {
    int maxLabel = -1;
    for (GraphEdge<Integer, Predicate> edge : graph.getEdges()) {
      maxLabel = Math.max(maxLabel, edge.getData().getID());
    }
    return maxLabel;
  }

  private static List<ResultPair> manualJoin(List<Pair> left, List<Pair> right) {
    // Build index: right.source -> [right.target, ...]
    Map<Integer, List<Integer>> rightBySource = new HashMap<>();
    for (Pair p : right) {
      rightBySource.computeIfAbsent(p.getSource(), k -> new ArrayList<>()).add(p.getTarget());
    }

    // Join with projection: deduplicate to mirror CPQ reachability semantics
    Set<ResultPair> results = new HashSet<>();
    for (Pair lp : left) {
      List<Integer> targets = rightBySource.get(lp.getTarget());
      if (targets != null) {
        for (int target : targets) {
          results.add(new ResultPair(lp.getSource(), target));
        }
      }
    }
    return new ArrayList<>(results);
  }

  private static IntGraph toIntGraph(UniqueGraph<Integer, Predicate> graph) {
    int nodeCount = graph.getNodeCount();
    int maxLabel = Math.max(REQUIRED_LABEL_MAX, maxLabelInGraph(graph));
    IntGraph intGraph = new IntGraph(nodeCount, maxLabel + 1);
    for (GraphEdge<Integer, Predicate> edge : graph.getEdges()) {
      intGraph.addEdge(edge.getSource(), edge.getTarget(), edge.getData().getID());
    }
    return intGraph;
  }

  private static List<ResultPair> manualJoinWithInverse(List<Pair> left, List<Pair> right) {
    // left: 2(x,y) right: 4(z,y) => output (x,z)
    // Build index: right.target -> [right.source, ...]
    Map<Integer, List<Integer>> rightByTarget = new HashMap<>();
    for (Pair p : right) {
      rightByTarget.computeIfAbsent(p.getTarget(), k -> new ArrayList<>()).add(p.getSource());
    }

    Set<ResultPair> results = new HashSet<>();
    for (Pair lp : left) {
      List<Integer> sources = rightByTarget.get(lp.getTarget());
      if (sources != null) {
        for (int source : sources) {
          results.add(new ResultPair(lp.getSource(), source));
        }
      }
    }
    return new ArrayList<>(results);
  }

  private static void compare(String label, List<Pair> composed, List<ResultPair> manual) {
    Set<ResultPair> composedSet = new HashSet<>();
    for (Pair p : composed) {
      composedSet.add(new ResultPair(p.getSource(), p.getTarget()));
    }
    Set<ResultPair> manualSet = new HashSet<>(manual);

    boolean matches = composedSet.equals(manualSet);

    System.out.println("\n" + label + " Comparison:");
    if (matches) {
      System.out.println("  ✅ MATCH: CPQ composition and manual join agree!");
    } else {
      Set<ResultPair> onlyComposed = new HashSet<>(composedSet);
      onlyComposed.removeAll(manualSet);
      Set<ResultPair> onlyManual = new HashSet<>(manualSet);
      onlyManual.removeAll(composedSet);

      System.out.println("  ❌ MISMATCH!");
      System.out.println("  Pairs only in composed: " + onlyComposed.size());
      System.out.println("  Pairs only in manual: " + onlyManual.size());

      if (!onlyComposed.isEmpty()) {
        System.out.println("  Sample pairs only in composed:");
        onlyComposed.stream().limit(5).forEach(p -> System.out.println("    " + p));
      }
      if (!onlyManual.isEmpty()) {
        System.out.println("  Sample pairs only in manual:");
        onlyManual.stream().limit(5).forEach(p -> System.out.println("    " + p));
      }
    }
  }

  private static void printSample(String label, List<?> items, int limit) {
    System.out.println("  " + label + " (showing up to " + limit + "):");
    for (int i = 0; i < Math.min(limit, items.size()); i++) {
      System.out.println("    " + items.get(i));
    }
    if (items.size() > limit) {
      System.out.println("    ... and " + (items.size() - limit) + " more");
    }
  }

  private static void compareResultPairs(
      String label, Set<ResultPair> left, Set<ResultPair> right) {
    boolean matches = left.equals(right);
    System.out.println("\n" + label + " Comparison (ResultPair sets):");
    if (matches) {
      System.out.println("  ✅ MATCH");
      return;
    }
    Set<ResultPair> onlyLeft = new HashSet<>(left);
    onlyLeft.removeAll(right);
    Set<ResultPair> onlyRight = new HashSet<>(right);
    onlyRight.removeAll(left);
    System.out.println("  ❌ MISMATCH!");
    System.out.println("  Pairs only in first: " + onlyLeft.size());
    System.out.println("  Pairs only in second: " + onlyRight.size());
    if (!onlyLeft.isEmpty()) {
      System.out.println("  Sample only in first:");
      onlyLeft.stream().limit(5).forEach(p -> System.out.println("    " + p));
    }
    if (!onlyRight.isEmpty()) {
      System.out.println("  Sample only in second:");
      onlyRight.stream().limit(5).forEach(p -> System.out.println("    " + p));
    }
  }

  private static Set<ResultPair> toResultPairSet(List<Pair> pairs) {
    Set<ResultPair> result = new HashSet<>();
    for (Pair p : pairs) {
      result.add(new ResultPair(p.getSource(), p.getTarget()));
    }
    return result;
  }

  private static Set<ResultPair> fromResultGraph(ResultGraph rg) {
    Set<ResultPair> result = new HashSet<>();
    for (dev.roanh.gmark.data.SourceTargetPair st : rg.getSourceTargetPairs()) {
      result.add(new ResultPair(st.source(), st.target()));
    }
    return result;
  }

  private static CPQ label(int id, List<Predicate> alphabet) {
    if (id < 0 || id >= alphabet.size() || alphabet.get(id) == null) {
      throw new IllegalArgumentException("Label id " + id + " not available in alphabet");
    }
    return CPQ.label(alphabet.get(id));
  }

  static class ResultPair {
    final int source;
    final int target;

    ResultPair(int source, int target) {
      this.source = source;
      this.target = target;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ResultPair)) return false;
      ResultPair that = (ResultPair) o;
      return source == that.source && target == that.target;
    }

    @Override
    public int hashCode() {
      return Objects.hash(source, target);
    }

    @Override
    public String toString() {
      return "(" + source + " → " + target + ")";
    }
  }
}
