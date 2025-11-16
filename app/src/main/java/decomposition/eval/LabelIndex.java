package decomposition.eval;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.LabelSequence;
import dev.roanh.cpqindex.Pair;
import dev.roanh.gmark.core.graph.Predicate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Provides lazily materialised projections of the CPQ index for individual labels. The index itself
 * keeps {@code (source,target)} pairs sorted; we only lift those pairs into search-friendly
 * structures when a query references the label.
 */
final class LabelIndex {
  private static final int[] EMPTY_INT_ARRAY = new int[0];

  private final Index index;
  private final Map<String, LabelProjection> cache = new HashMap<>();
  private final Set<String> canonicalLabels = new TreeSet<>();
  private final Map<String, List<Index.Block>> blocksPerLabel = new HashMap<>();

  private LabelIndex(Index index) {
    this.index = Objects.requireNonNull(index, "index");
    initialiseLabels();
  }

  static LabelIndex from(Index index) {
    return new LabelIndex(index);
  }

  Set<String> labels() {
    return Collections.unmodifiableSet(canonicalLabels);
  }

  LabelProjection projectionFor(String requestedLabel) {
    Objects.requireNonNull(requestedLabel, "requestedLabel");
    if (!canonicalLabels.contains(requestedLabel)) {
      throw new IllegalArgumentException(
          "Unknown label '" + requestedLabel + "'. Known labels: " + canonicalLabels);
    }
    return cache.computeIfAbsent(requestedLabel, this::materialise);
  }

  private void initialiseLabels() {
    for (Index.Block block : index.getBlocks()) {
      List<LabelSequence> sequences = block.getLabels();
      if (sequences == null || sequences.isEmpty()) {
        continue;
      }
      for (LabelSequence sequence : sequences) {
        Predicate[] predicates = sequence.getLabels();
        if (predicates == null || predicates.length != 1) {
          continue;
        }
        String alias = predicates[0].getAlias();
        canonicalLabels.add(alias);
        blocksPerLabel.computeIfAbsent(alias, ignored -> new ArrayList<>()).add(block);
      }
    }
  }

  private LabelProjection materialise(String canonicalLabel) {
    Map<Integer, NavigableSet<Integer>> srcToTargets = new TreeMap<>();
    Map<Integer, NavigableSet<Integer>> tgtToSources = new TreeMap<>();
    List<Index.Block> relevantBlocks = blocksPerLabel.getOrDefault(canonicalLabel, List.of());
    for (Index.Block block : relevantBlocks) {
      List<Pair> paths = block.getPaths();
      if (paths == null || paths.isEmpty()) {
        continue;
      }
      for (Pair pair : paths) {
        srcToTargets
            .computeIfAbsent(pair.getSource(), ignored -> new TreeSet<>())
            .add(pair.getTarget());
        tgtToSources
            .computeIfAbsent(pair.getTarget(), ignored -> new TreeSet<>())
            .add(pair.getSource());
      }
    }
    return new LabelProjection(
        toIntArray(srcToTargets.keySet()),
        toIntArray(tgtToSources.keySet()),
        toIntArrayMap(srcToTargets),
        toIntArrayMap(tgtToSources));
  }

  private static Map<Integer, int[]> toIntArrayMap(Map<Integer, NavigableSet<Integer>> input) {
    if (input.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<Integer, int[]> result = new HashMap<>(input.size());
    for (Map.Entry<Integer, NavigableSet<Integer>> entry : input.entrySet()) {
      result.put(entry.getKey(), toIntArray(entry.getValue()));
    }
    return result;
  }

  private static int[] toIntArray(Collection<Integer> values) {
    if (values.isEmpty()) {
      return EMPTY_INT_ARRAY;
    }
    int[] result = new int[values.size()];
    int index = 0;
    for (Integer value : values) {
      result[index++] = value;
    }
    return result;
  }

  static final class LabelProjection {
    private final int[] allSources;
    private final int[] allTargets;
    private final Map<Integer, int[]> forward;
    private final Map<Integer, int[]> reverse;

    LabelProjection(
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
