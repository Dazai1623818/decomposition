package decomposition.eval;

import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.CpqNativeIndex.Block;
import decomposition.nativeindex.CpqNativeIndex.LabelSequence;
import decomposition.nativeindex.CpqNativeIndex.Pair;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Provides lazily materialised projections of the CPQ-native index for arbitrary label sequences.
 *
 * <p>The underlying {@link Index} stores {@code (source,target)} pairs grouped in blocks that are
 * indexed by label sequences. This class builds a lookup structure keyed by path signatures (lists
 * of label aliases) and exposes the pairs as primitive arrays optimised for Leapfrog Triejoin.
 */
final class CpqIndex {
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  private final CpqNativeIndex index;
  private final Map<List<String>, RelationProjection> cache = new HashMap<>();
  private final Map<List<String>, List<Block>> blocksPerSignature = new HashMap<>();
  private final Set<String> canonicalSingleEdgeLabels = new HashSet<>();
  private final int maxSignatureLength;

  private CpqIndex(CpqNativeIndex index) {
    this.index = Objects.requireNonNull(index, "index");
    this.maxSignatureLength = initialiseSignatures();
  }

  static CpqIndex from(CpqNativeIndex index) {
    return new CpqIndex(index);
  }

  /**
   * Returns all distinct label aliases that appear in single-edge (length-1) label sequences. This
   * is primarily used for diagnostics / logging.
   */
  Set<String> singleEdgeLabels() {
    return Collections.unmodifiableSet(canonicalSingleEdgeLabels);
  }

  /** Returns the maximum path length (number of labels) observed in the underlying index. */
  int maxSignatureLength() {
    return maxSignatureLength;
  }

  /** Checks whether the index contains at least one block for the given path signature. */
  boolean hasSignature(List<String> signature) {
    Objects.requireNonNull(signature, "signature");
    if (signature.isEmpty()) {
      return false;
    }
    return blocksPerSignature.containsKey(List.copyOf(signature));
  }

  /**
   * Returns a relation projection for the given path signature. The signature must be a non-empty
   * list of label aliases.
   *
   * @throws IllegalArgumentException if the signature is unknown to the index.
   */
  RelationProjection projectionFor(List<String> signature) {
    Objects.requireNonNull(signature, "signature");
    if (signature.isEmpty()) {
      throw new IllegalArgumentException("Path signature must contain at least one label.");
    }
    List<String> canonical = List.copyOf(signature);
    List<Block> blocks = blocksPerSignature.get(canonical);
    if (blocks == null || blocks.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown path signature "
              + canonical
              + ". Known single-edge labels: "
              + canonicalSingleEdgeLabels);
    }
    return cache.computeIfAbsent(canonical, ignored -> materialise(blocks));
  }

  private int initialiseSignatures() {
    int maxLength = 0;
    for (Block block : index.getBlocks()) {
      List<LabelSequence> sequences = block.getLabels();
      if (sequences == null || sequences.isEmpty()) {
        continue;
      }
      for (LabelSequence sequence : sequences) {
        Predicate[] predicates = sequence.getLabels();
        if (predicates == null || predicates.length == 0) {
          continue;
        }
        List<String> signature = toSignature(predicates);
        maxLength = Math.max(maxLength, signature.size());
        blocksPerSignature.computeIfAbsent(signature, ignored -> new ArrayList<>()).add(block);
        if (predicates.length == 1) {
          canonicalSingleEdgeLabels.add(predicates[0].getAlias());
        }
      }
    }
    return maxLength;
  }

  private static List<String> toSignature(Predicate[] predicates) {
    List<String> labels = new ArrayList<>(predicates.length);
    for (Predicate predicate : predicates) {
      labels.add(predicate.getAlias());
    }
    return List.copyOf(labels);
  }

  private static RelationProjection materialise(List<Block> blocks) {
    if (blocks.isEmpty()) {
      return new RelationProjection(
          EMPTY_INT_ARRAY, EMPTY_INT_ARRAY, Collections.emptyMap(), Collections.emptyMap());
    }

    Map<Integer, IntAccumulator> forward = new HashMap<>();
    Map<Integer, IntAccumulator> reverse = new HashMap<>();

    for (Block block : blocks) {
      List<Pair> paths = block.getPaths();
      if (paths == null || paths.isEmpty()) {
        continue;
      }
      for (Pair pair : paths) {
        forward
            .computeIfAbsent(pair.getSource(), ignored -> new IntAccumulator())
            .add(pair.getTarget());
        reverse
            .computeIfAbsent(pair.getTarget(), ignored -> new IntAccumulator())
            .add(pair.getSource());
      }
    }

    if (forward.isEmpty() || reverse.isEmpty()) {
      return new RelationProjection(
          EMPTY_INT_ARRAY, EMPTY_INT_ARRAY, Collections.emptyMap(), Collections.emptyMap());
    }

    Map<Integer, int[]> forwardArrays = toIntArrayMap(forward);
    Map<Integer, int[]> reverseArrays = toIntArrayMap(reverse);
    int[] allSources = sortedKeys(forwardArrays.keySet());
    int[] allTargets = sortedKeys(reverseArrays.keySet());

    return new RelationProjection(allSources, allTargets, forwardArrays, reverseArrays);
  }

  private static Map<Integer, int[]> toIntArrayMap(Map<Integer, IntAccumulator> input) {
    if (input.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<Integer, int[]> result = new HashMap<>(input.size());
    for (Map.Entry<Integer, IntAccumulator> entry : input.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toSortedDistinctArray());
    }
    return result;
  }

  private static int[] sortedKeys(Collection<Integer> keys) {
    if (keys.isEmpty()) {
      return EMPTY_INT_ARRAY;
    }
    int[] result = new int[keys.size()];
    int index = 0;
    for (Integer key : keys) {
      result[index++] = key;
    }
    java.util.Arrays.sort(result);
    return result;
  }

  /** Lightweight accumulator for primitive {@code int} values. */
  private static final class IntAccumulator {
    private int[] buffer = new int[16];
    private int size = 0;

    void add(int value) {
      if (size == buffer.length) {
        int[] expanded = new int[buffer.length * 2];
        System.arraycopy(buffer, 0, expanded, 0, buffer.length);
        buffer = expanded;
      }
      buffer[size++] = value;
    }

    int[] toSortedDistinctArray() {
      if (size == 0) {
        return EMPTY_INT_ARRAY;
      }
      int[] result = new int[size];
      System.arraycopy(buffer, 0, result, 0, size);
      java.util.Arrays.sort(result);
      int uniqueCount = 1;
      for (int i = 1; i < result.length; i++) {
        if (result[i] != result[uniqueCount - 1]) {
          result[uniqueCount++] = result[i];
        }
      }
      if (uniqueCount == result.length) {
        return result;
      }
      int[] trimmed = new int[uniqueCount];
      System.arraycopy(result, 0, trimmed, 0, uniqueCount);
      return trimmed;
    }
  }

  /**
   * A bidirectional projection of a relation, exposing distinct sources/targets and per-vertex
   * adjacency lists backed by primitive arrays.
   */
  static final class RelationProjection {
    private final int[] allSources;
    private final int[] allTargets;
    private final Map<Integer, int[]> forward;
    private final Map<Integer, int[]> reverse;

    RelationProjection(
        int[] allSources,
        int[] allTargets,
        Map<Integer, int[]> forward,
        Map<Integer, int[]> reverse) {
      this.allSources = allSources;
      this.allTargets = allTargets;
      this.forward = forward;
      this.reverse = reverse;
    }

    int[] allSources() {
      return allSources;
    }

    int[] allTargets() {
      return allTargets;
    }

    int[] targetsForSource(int source) {
      return forward.getOrDefault(source, EMPTY_INT_ARRAY);
    }

    int[] sourcesForTarget(int target) {
      return reverse.getOrDefault(target, EMPTY_INT_ARRAY);
    }
  }
}
