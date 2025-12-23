package decomposition.eval;

import decomposition.core.CPQExpression;
import decomposition.eval.leapfrogjoiner.IntAccumulator;
import decomposition.eval.leapfrogjoiner.LeapfrogJoiner;
import decomposition.eval.leapfrogjoiner.RelationBinding;
import decomposition.eval.leapfrogjoiner.RelationBinding.RelationProjection;
import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.IndexUtil;
// import dev.roanh.cpqindex.OneTimeProgressListener; // Remove
import dev.roanh.cpqindex.Pair;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.lang.cpq.CPQ;
// import dev.roanh.gmark.util.core.IntAccumulator; // Remove
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class EvaluationIndex implements AutoCloseable {

  private static final String INDEX_DIRNAME = "indices";
  private static final AtomicBoolean NATIVES_LOADED = new AtomicBoolean(false);
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  private final Index index;
  private final LeapfrogJoiner joiner;
  private final int k;

  public EvaluationIndex(Path graphPath, int k) throws IOException {
    ensureNativesLoaded();
    this.k = k;

    Path indexPath = findExistingIndexPath(graphPath, k);
    if (indexPath == null) {
      indexPath = resolveIndexPath(graphPath, k);
      System.out.println("Building CPQ index (k=" + k + ") for " + graphPath + "...");
      long start = System.currentTimeMillis();
      this.index = build(graphPath, k);
      long end = System.currentTimeMillis();
      System.out.println("Index built in " + (end - start) + " ms.");
      System.out.println("Saving index to " + indexPath + "...");
      save(index, indexPath);
    } else {
      System.out.println("Loading existing CPQ index from " + indexPath + "...");
      long start = System.currentTimeMillis();
      this.index = load(indexPath);
      long end = System.currentTimeMillis();
      System.out.println("Index loaded in " + (end - start) + " ms.");
    }
    this.joiner = new LeapfrogJoiner();
  }

  @Override
  public void close() throws Exception {
    if (index != null) {
      // index.close(); // Index likely doesn't support close
    }
  }

  public int k() {
    return k;
  }

  public EvaluationRun evaluate(
      dev.roanh.gmark.lang.cq.CQ cq,
      decomposition.decompose.Decomposer.DecompositionMethod method) {
    long start = System.currentTimeMillis();

    // 1. Decompose (this returns a run with decomposition phases already timed)
    EvaluationRun run =
        decomposition.decompose.Decomposer.decomposeWithRun(
            new decomposition.core.ConjunctiveQuery(cq), method, k(), 0);

    // 2. Evaluate each decomposition
    List<List<CPQExpression>> decompositions = run.decompositions();
    List<Map<String, Integer>> results = new ArrayList<>(decompositions.size());

    // Evaluate all decompositions for performance benchmarking
    for (List<CPQExpression> tuple : decompositions) {
      List<Map<String, Integer>> tupleResult = evaluateDecomposition(tuple, run);
      if (!tupleResult.isEmpty()) {
        results.addAll(tupleResult);
      }
    }
    List<String> freeVars = normalizedFreeVars(cq);
    run.setEvaluationResults(projectResults(results, freeVars));

    // Update total time to include evaluation
    long end = System.currentTimeMillis();
    run.recordPhaseMs(EvaluationRun.Phase.TOTAL, end - start);

    return run;
  }

  public List<Map<String, Integer>> evaluateDecomposition(
      List<CPQExpression> tuple, EvaluationRun run) {
    Objects.requireNonNull(tuple, "tuple");
    if (tuple.isEmpty()) {
      return List.of();
    }

    List<RelationBinding> relations = new ArrayList<>(tuple.size());
    try (var timer = run.startTimer(EvaluationRun.Phase.CPQ_EVALUATION)) {
      for (CPQExpression expression : tuple) {
        if (expression == null) {
          continue;
        }
        RelationBinding binding = evaluateExpression(expression);
        if (binding == null) {
          return List.of();
        }
        relations.add(binding);
      }
    }
    if (relations.isEmpty()) {
      return List.of();
    }

    try (var timer = run.startTimer(EvaluationRun.Phase.JOIN)) {
      return joiner.join(relations);
    }
  }

  /** Legacy method for backward compatibility if needed, delegates with dummy run or throws. */
  public List<Map<String, Integer>> evaluateDecomposition(List<CPQExpression> tuple) {
    // For now, create a dummy run just to satisfy the method signature if called
    // directly
    return evaluateDecomposition(tuple, new EvaluationRun());
  }

  public List<Map<String, Integer>> evaluateDecompositionInternal(List<CPQExpression> tuple) {
    Objects.requireNonNull(tuple, "tuple");
    // ... logic moved to evaluateDecomposition(tuple, run) ...
    return evaluateDecomposition(tuple);
  }

  private RelationBinding evaluateExpression(CPQExpression expression) {
    CPQ cpq = expression.cpq();
    List<Pair> matches;
    try {
      matches = index.query(cpq);
    } catch (IllegalArgumentException ex) {
      throw new IllegalStateException("Failed to evaluate CPQ: " + cpq, ex);
    }

    String left = normalizeVar(endpointVariable(expression, expression.source()));
    String right = normalizeVar(endpointVariable(expression, expression.target()));

    if (left.equals(right)) {
      Set<Integer> values = new HashSet<>();
      for (Pair pair : matches) {
        if (pair.getSource() == pair.getTarget()) {
          values.add(pair.getSource());
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
    for (Pair pair : matches) {
      forward
          .computeIfAbsent(pair.getSource(), ignored -> new IntAccumulator())
          .add(pair.getTarget());
      reverse
          .computeIfAbsent(pair.getTarget(), ignored -> new IntAccumulator())
          .add(pair.getSource());
    }
    if (forward.isEmpty() || reverse.isEmpty()) {
      return null;
    }

    RelationProjection projection =
        new RelationProjection(
            sortedKeys(forward.keySet()),
            sortedKeys(reverse.keySet()),
            toIntArrayMap(forward),
            toIntArrayMap(reverse));
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

  private static int[] sortedKeys(Set<Integer> keys) {
    if (keys.isEmpty()) {
      return EMPTY_INT_ARRAY;
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

  private static List<String> normalizedFreeVars(dev.roanh.gmark.lang.cq.CQ cq) {
    return cq.getFreeVariables().stream().map(var -> normalizeVar(var.getName())).toList();
  }

  private static List<Map<String, Integer>> projectResults(
      List<Map<String, Integer>> rows, List<String> freeVars) {
    if (rows.isEmpty()) {
      return List.of();
    }
    if (freeVars.isEmpty()) {
      return List.of(Map.of());
    }
    Set<Map<String, Integer>> deduped = new LinkedHashSet<>();
    for (Map<String, Integer> row : rows) {
      Map<String, Integer> projected = projectRow(row, freeVars);
      if (projected != null) {
        deduped.add(projected);
      }
    }
    return List.copyOf(deduped);
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

  private static void ensureNativesLoaded() throws IOException {
    if (!NATIVES_LOADED.compareAndSet(false, true)) {
      return;
    }
    String mappedName = System.mapLibraryName("nauty");
    Path[] candidates = {
      Path.of("lib", mappedName),
      Path.of(mappedName),
      Path.of("libnauty.so"),
      Path.of("libnauty.dll")
    };

    for (Path candidate : candidates) {
      Path absolute = candidate.toAbsolutePath();
      if (Files.exists(absolute)) {
        System.out.println("Loading nauty native library from: " + absolute);
        System.load(absolute.toString());
        return;
      }
    }

    try {
      System.loadLibrary("nauty");
    } catch (UnsatisfiedLinkError e) {
      throw new IOException(
          "Failed to load nauty native library. Searched " + Arrays.toString(candidates), e);
    }
  }

  private static Index build(Path graphPath, int k) throws IOException {
    try {
      return new Index(
          IndexUtil.readGraph(graphPath),
          k,
          true,
          true,
          Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
          2,
          ProgressListener.LOG);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
  }

  private static Index load(Path path) throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return new Index(in);
    }
  }

  private static void save(Index index, Path path) throws IOException {
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    try (OutputStream out = Files.newOutputStream(path)) {
      index.write(out, false);
    }
  }

  private static Path resolveIndexPath(Path graphPath, int k) {
    String name = graphPath.getFileName().toString();
    String stem = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
    Path parent = graphPath.getParent();
    if (parent == null) {
      return Path.of(stem + ".k" + k + ".idx");
    }
    return parent.resolve(INDEX_DIRNAME).resolve(stem + ".k" + k + ".idx");
  }

  private static Path findExistingIndexPath(Path graphPath, int k) {
    String name = graphPath.getFileName().toString();
    String stem = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;

    Path preferred = resolveIndexPath(graphPath, k);
    if (Files.exists(preferred)) {
      return preferred;
    }

    // Backwards-compatible fallback: older builds stored index files alongside the
    // graph.
    Path legacyKSpecific = graphPath.resolveSibling(stem + ".k" + k + ".idx");
    if (Files.exists(legacyKSpecific)) {
      return legacyKSpecific;
    }

    return null;
  }
}
