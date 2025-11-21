package decomposition.eval;

import decomposition.nativeindex.CpqNativeIndex;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Executes conjunctive queries using Leapfrog Triejoin over the CPQ-native index. */
public final class LeapfrogCpqJoiner {
  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private final RelationIndex index;

  private LeapfrogCpqJoiner(RelationIndex index) {
    this.index = Objects.requireNonNull(index, "index");
  }

  public static LeapfrogCpqJoiner fromIndex(CpqNativeIndex cpqIndex) {
    return new LeapfrogCpqJoiner(RelationIndex.from(cpqIndex));
  }

  /** Baseline execution that only uses single-edge relations (k = 1 behaviour). */
  public List<Map<String, Integer>> executeBaseline(CQ cq) {
    Objects.requireNonNull(cq, "cq");
    List<AtomCQ> atoms = atomsOf(cq);
    if (atoms.isEmpty()) {
      return List.of();
    }
    return executeBaseline(atoms);
  }

  /** Baseline execution over a fixed list of atoms using only length-1 label sequences. */
  public List<Map<String, Integer>> executeBaseline(List<AtomCQ> atoms) {
    Objects.requireNonNull(atoms, "atoms");
    if (atoms.isEmpty()) {
      return List.of();
    }
    System.out.println("Baseline join CPQs (single-edge):");
    for (AtomCQ atom : atoms) {
      String cpq = atom.getLabel().getAlias();
      String src = varName(atom.getSource());
      String tgt = varName(atom.getTarget());
      System.out.println("  " + cpq + " (" + src + " → " + tgt + ")");
    }
    List<RelationBinding> relations = new ArrayList<>(atoms.size());
    for (AtomCQ atom : atoms) {
      List<String> signature = List.of(atom.getLabel().getAlias());
      RelationProjection projection = index.projectionFor(signature);
      relations.add(RelationBinding.from(projection, atom));
    }
    return runLeapfrog(relations);
  }

  /**
   * Optimised execution that groups maximal contiguous paths for which a pre-computed relation is
   * available in the CPQ index (up to the maximum signature length observed in the native index).
   *
   * <p>Each such path is treated as a single binary relation between the path endpoints.
   */
  public List<Map<String, Integer>> executeOptimized(List<AtomCQ> atoms) {
    Objects.requireNonNull(atoms, "atoms");
    if (atoms.isEmpty()) {
      return List.of();
    }
    List<PathFragment> fragments = groupPathFragments(atoms);
    System.out.println("Optimized join CPQs:");
    for (PathFragment fragment : fragments) {
      String cpq = formatSignature(fragment.signature());
      AtomCQ first = fragment.atoms().get(0);
      AtomCQ last = fragment.atoms().get(fragment.atoms().size() - 1);
      String src = varName(first.getSource());
      String tgt = varName(last.getTarget());
      System.out.println("  " + cpq + " (" + src + " → " + tgt + ")");
    }
    List<RelationBinding> relations = new ArrayList<>(fragments.size());
    for (PathFragment fragment : fragments) {
      RelationProjection projection = index.projectionFor(fragment.signature());
      relations.add(RelationBinding.from(projection, fragment));
    }
    return runLeapfrog(relations);
  }

  private List<Map<String, Integer>> runLeapfrog(List<RelationBinding> relations) {
    Map<String, List<RelationBinding>> relationsPerVariable = new HashMap<>();
    for (RelationBinding relation : relations) {
      relationsPerVariable
          .computeIfAbsent(relation.sourceVar, ignored -> new ArrayList<>())
          .add(relation);
      relationsPerVariable
          .computeIfAbsent(relation.targetVar, ignored -> new ArrayList<>())
          .add(relation);
    }

    List<String> variableOrder =
        relationsPerVariable.keySet().stream()
            .sorted(
                Comparator.comparingInt((String var) -> relationsPerVariable.get(var).size())
                    .thenComparing(Comparator.naturalOrder()))
            .collect(Collectors.toCollection(ArrayList::new));

    List<Map<String, Integer>> results = new ArrayList<>();
    search(variableOrder, 0, relationsPerVariable, new LinkedHashMap<>(), results);
    return results;
  }

  private void search(
      List<String> variableOrder,
      int depth,
      Map<String, List<RelationBinding>> relationsPerVariable,
      Map<String, Integer> assignment,
      List<Map<String, Integer>> results) {
    if (depth == variableOrder.size()) {
      results.add(new LinkedHashMap<>(assignment));
      return;
    }
    String variable = variableOrder.get(depth);
    List<RelationBinding> bindings = relationsPerVariable.getOrDefault(variable, List.of());
    if (bindings.isEmpty()) {
      // Variable unconstrained; no tuples can satisfy the query.
      return;
    }

    List<LeapfrogIterator.IntCursor> cursors = new ArrayList<>(bindings.size());
    for (RelationBinding binding : bindings) {
      int[] domain = binding.domainFor(variable, assignment);
      if (domain.length == 0) {
        return;
      }
      cursors.add(new LeapfrogIterator.IntCursor(domain));
    }

    LeapfrogIterator join = new LeapfrogIterator(cursors);
    join.init();
    while (!join.atEnd()) {
      assignment.put(variable, join.key());
      search(variableOrder, depth + 1, relationsPerVariable, assignment, results);
      assignment.remove(variable);
      join.next();
    }
  }

  public Set<String> singleEdgeLabels() {
    return index.singleEdgeLabels();
  }

  private static String formatSignature(List<String> signature) {
    if (signature.isEmpty()) {
      return "";
    }
    if (signature.size() == 1) {
      return signature.get(0);
    }
    return String.join(" ◦ ", signature);
  }

  private static List<AtomCQ> atomsOf(CQ cq) {
    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
    if (graph.getEdgeCount() == 0) {
      return List.of();
    }
    List<AtomCQ> atoms = new ArrayList<>(graph.getEdgeCount());
    for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
      atoms.add(edge.getData());
    }
    return atoms;
  }

  private List<PathFragment> groupPathFragments(List<AtomCQ> atoms) {
    List<PathFragment> fragments = new ArrayList<>();
    int maxLength = index.maxSignatureLength();
    if (maxLength <= 1) {
      // Index only contains single-edge information; fall back to baseline grouping.
      for (AtomCQ atom : atoms) {
        fragments.add(PathFragment.singleton(atom));
      }
      return fragments;
    }

    int i = 0;
    while (i < atoms.size()) {
      AtomCQ start = atoms.get(i);
      List<AtomCQ> pathAtoms = new ArrayList<>();
      pathAtoms.add(start);
      List<String> labels = new ArrayList<>();
      labels.add(start.getLabel().getAlias());

      String currentTarget = varName(start.getTarget());
      List<String> bestSignature = null;
      int bestEndExclusive = i + 1;

      // Try to extend the path greedily up to maxLength.
      int j = i + 1;
      while (j < atoms.size() && pathAtoms.size() < maxLength) {
        AtomCQ next = atoms.get(j);
        if (!varName(next.getSource()).equals(currentTarget)) {
          break;
        }
        pathAtoms.add(next);
        labels.add(next.getLabel().getAlias());
        currentTarget = varName(next.getTarget());

        if (index.hasSignature(labels)) {
          bestSignature = List.copyOf(labels);
          bestEndExclusive = j + 1;
        }
        j++;
      }

      if (bestSignature != null && bestEndExclusive - i > 1) {
        List<AtomCQ> grouped = atoms.subList(i, bestEndExclusive);
        fragments.add(PathFragment.of(grouped, bestSignature));
        i = bestEndExclusive;
      } else {
        fragments.add(PathFragment.singleton(start));
        i++;
      }
    }

    return fragments;
  }

  private static String varName(VarCQ variable) {
    return "?" + variable.getName();
  }

  private static final class RelationBinding {
    private final RelationProjection projection;
    private final String sourceVar;
    private final String targetVar;
    private final String description;

    private RelationBinding(
        RelationProjection projection, String sourceVar, String targetVar, String description) {
      this.projection = projection;
      this.sourceVar = sourceVar;
      this.targetVar = targetVar;
      this.description = description;
    }

    static RelationBinding from(RelationProjection projection, AtomCQ atom) {
      String sourceVar = varName(atom.getSource());
      String targetVar = varName(atom.getTarget());
      String desc = atom.getLabel().getAlias();
      return new RelationBinding(projection, sourceVar, targetVar, desc);
    }

    static RelationBinding from(RelationProjection projection, PathFragment fragment) {
      String sourceVar = varName(fragment.atoms().get(0).getSource());
      String targetVar = varName(fragment.atoms().get(fragment.atoms().size() - 1).getTarget());
      String desc = fragment.signature().toString();
      return new RelationBinding(projection, sourceVar, targetVar, desc);
    }

    int[] domainFor(String variable, Map<String, Integer> assignment) {
      if (variable.equals(sourceVar)) {
        if (assignment.containsKey(targetVar)) {
          int target = assignment.get(targetVar);
          return projection.sourcesForTarget(target);
        }
        return projection.allSources();
      }
      if (variable.equals(targetVar)) {
        if (assignment.containsKey(sourceVar)) {
          int source = assignment.get(sourceVar);
          return projection.targetsForSource(source);
        }
        return projection.allTargets();
      }
      throw new IllegalArgumentException(
          "Variable " + variable + " not part of relation " + description);
    }
  }

  private static final class PathFragment {
    private final List<AtomCQ> atoms;
    private final List<String> signature;

    private PathFragment(List<AtomCQ> atoms, List<String> signature) {
      this.atoms = List.copyOf(atoms);
      this.signature = List.copyOf(signature);
    }

    static PathFragment singleton(AtomCQ atom) {
      return new PathFragment(List.of(atom), List.of(atom.getLabel().getAlias()));
    }

    static PathFragment of(List<AtomCQ> atoms, List<String> signature) {
      return new PathFragment(atoms, signature);
    }

    List<AtomCQ> atoms() {
      return atoms;
    }

    List<String> signature() {
      return signature;
    }
  }

  private static final class RelationIndex {
    private final CpqNativeIndex index;
    private final Map<List<String>, RelationProjection> cache = new HashMap<>();
    private final Map<List<String>, List<CpqNativeIndex.Block>> blocksPerSignature =
        new HashMap<>();
    private final Set<String> canonicalSingleEdgeLabels = new HashSet<>();
    private final int maxSignatureLength;

    private RelationIndex(CpqNativeIndex index) {
      this.index = Objects.requireNonNull(index, "index");
      this.maxSignatureLength = initialiseSignatures();
    }

    static RelationIndex from(CpqNativeIndex index) {
      return new RelationIndex(index);
    }

    Set<String> singleEdgeLabels() {
      return Collections.unmodifiableSet(canonicalSingleEdgeLabels);
    }

    int maxSignatureLength() {
      return maxSignatureLength;
    }

    boolean hasSignature(List<String> signature) {
      Objects.requireNonNull(signature, "signature");
      if (signature.isEmpty()) {
        return false;
      }
      return blocksPerSignature.containsKey(List.copyOf(signature));
    }

    RelationProjection projectionFor(List<String> signature) {
      Objects.requireNonNull(signature, "signature");
      if (signature.isEmpty()) {
        throw new IllegalArgumentException("Path signature must contain at least one label.");
      }
      List<String> canonical = List.copyOf(signature);
      List<CpqNativeIndex.Block> blocks = blocksPerSignature.get(canonical);
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
      for (CpqNativeIndex.Block block : index.getBlocks()) {
        List<CpqNativeIndex.LabelSequence> sequences = block.getLabels();
        if (sequences == null || sequences.isEmpty()) {
          continue;
        }
        for (CpqNativeIndex.LabelSequence sequence : sequences) {
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

    private static RelationProjection materialise(List<CpqNativeIndex.Block> blocks) {
      if (blocks.isEmpty()) {
        return new RelationProjection(
            EMPTY_INT_ARRAY, EMPTY_INT_ARRAY, Collections.emptyMap(), Collections.emptyMap());
      }

      Map<Integer, IntAccumulator> forward = new HashMap<>();
      Map<Integer, IntAccumulator> reverse = new HashMap<>();

      for (CpqNativeIndex.Block block : blocks) {
        List<CpqNativeIndex.Pair> paths = block.getPaths();
        if (paths == null || paths.isEmpty()) {
          continue;
        }
        for (CpqNativeIndex.Pair pair : paths) {
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
  }

  /** A bidirectional projection of a relation with primitive adjacency arrays. */
  private static final class RelationProjection {
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
