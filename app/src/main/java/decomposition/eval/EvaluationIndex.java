package decomposition.eval;

import decomposition.core.CPQExpression;
import decomposition.eval.leapfrogjoiner.IntAccumulator;
import decomposition.eval.leapfrogjoiner.LeapfrogJoiner;
import decomposition.eval.leapfrogjoiner.RelationBinding;
import decomposition.eval.leapfrogjoiner.RelationBinding.RelationProjection;
import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.IndexUtil;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.Pair;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal facade around the native CPQ index.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>load/build the native {@link Index}
 *   <li>evaluate a full decomposition tuple via native queries + {@link LeapfrogJoiner}
 * </ul>
 *
 * <p>Intentionally holds no query-to-query cache.
 */
public final class EvaluationIndex {
  private static final AtomicBoolean NATIVES_LOADED = new AtomicBoolean();
  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private static final String INDEX_DIRNAME = ".cpqindex";

  private final Index index;
  private final int k;
  private final LeapfrogJoiner joiner = new LeapfrogJoiner();

  private EvaluationIndex(Index index, int k) {
    this.index = Objects.requireNonNull(index, "index");
    this.k = k;
  }

  public static EvaluationIndex loadOrBuild(Path graphPath, int k) throws IOException {
    Objects.requireNonNull(graphPath, "graphPath");
    if (k <= 0) {
      throw new IllegalArgumentException("k must be > 0");
    }
    ensureNativesLoaded();

    Path preferredIndexPath = resolveIndexPath(graphPath, k);
    Path existingIndexPath = findExistingIndexPath(graphPath, k);
    Index idx;
    if (existingIndexPath != null) {
      idx = load(existingIndexPath);
    } else {
      idx = build(graphPath, k);
      try {
        save(idx, preferredIndexPath);
      } catch (IOException ignored) {
        // Best-effort persistence only.
      }
    }
    idx.sort();
    return new EvaluationIndex(idx, k);
  }

  public int k() {
    return k;
  }

  public List<Map<String, Integer>> evaluateDecomposition(List<CPQExpression> tuple) {
    Objects.requireNonNull(tuple, "tuple");
    if (tuple.isEmpty()) {
      return List.of();
    }

    List<RelationBinding> relations = new ArrayList<>(tuple.size());
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
    if (relations.isEmpty()) {
      return List.of();
    }
    return joiner.join(relations);
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

  private static void ensureNativesLoaded() throws IOException {
    if (!NATIVES_LOADED.compareAndSet(false, true)) {
      return;
    }
    try {
      Main.loadNatives();
    } catch (UnsatisfiedLinkError e) {
      NATIVES_LOADED.set(false);
      throw new IOException(
          "Failed to load native nauty bindings required by CPQ index: " + e.getMessage(), e);
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

    // Backwards-compatible fallback: older builds stored index files alongside the graph.
    Path legacyKSpecific = graphPath.resolveSibling(stem + ".k" + k + ".idx");
    if (Files.exists(legacyKSpecific)) {
      return legacyKSpecific;
    }

    return null;
  }
}
