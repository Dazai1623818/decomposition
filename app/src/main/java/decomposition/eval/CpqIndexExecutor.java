package decomposition.eval;

import decomposition.cpq.CPQExpression;
import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.CpqNativeIndex.Pair;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Executes CPQ components against the native index and joins their assignments. */
final class CpqIndexExecutor {
  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private final CpqNativeIndex index;

  CpqIndexExecutor(CpqNativeIndex index) {
    this.index = Objects.requireNonNull(index, "index");
  }

  List<Map<String, Integer>> execute(List<Component> components) {
    Objects.requireNonNull(components, "components");
    if (components.isEmpty()) {
      return List.of();
    }
    List<RelationBinding> bindings = new ArrayList<>(components.size());
    for (Component component : components) {
      RelationBinding binding = evaluateComponent(component);
      if (binding == null) {
        return List.of();
      }
      bindings.add(binding);
    }
    if (bindings.isEmpty()) {
      return List.of();
    }
    return runLeapfrog(bindings);
  }

  private RelationBinding evaluateComponent(Component component) {
    List<Pair> matches;
    try {
      matches = index.query(component.cpq());
    } catch (IllegalArgumentException ex) {
      throw new IllegalStateException(
          "Failed to evaluate component '" + component.description() + "'", ex);
    }
    String left = component.leftVar();
    String right = component.rightVar();
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
      return RelationBinding.unary(left, component.description(), domain);
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
    return RelationBinding.binary(left, right, component.description(), projection);
  }

  private List<Map<String, Integer>> runLeapfrog(List<RelationBinding> bindings) {
    Map<String, List<RelationBinding>> relationsPerVariable = new HashMap<>();
    for (RelationBinding binding : bindings) {
      binding.register(relationsPerVariable);
    }
    if (relationsPerVariable.isEmpty()) {
      return List.of();
    }

    List<String> variableOrder =
        relationsPerVariable.keySet().stream()
            .sorted(
                Comparator.comparingInt((String var) -> relationsPerVariable.get(var).size())
                    .thenComparing(Comparator.naturalOrder()))
            .toList();
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

  static Component oversizedComponent(List<Component> components, int maxDiameter) {
    for (Component component : components) {
      if (component.cpq().getDiameter() > maxDiameter) {
        return component;
      }
    }
    return null;
  }

  static Component fromExpression(CPQExpression expression) {
    Objects.requireNonNull(expression, "expression");
    String left = variableName(expression.getVarForNode(expression.source()));
    if (left == null) {
      left = variableName(expression.source());
    }
    String right = variableName(expression.getVarForNode(expression.target()));
    if (right == null) {
      right = variableName(expression.target());
    }
    if (left == null || right == null) {
      throw new IllegalArgumentException(
          "Expression must supply variables for both endpoints: " + expression);
    }
    return new Component(
        left,
        right,
        expression.cpq(),
        expression.cpq().toString() + " (" + left + "→" + right + ")");
  }

  private static String variableName(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    return raw.startsWith("?") ? raw : ("?" + raw);
  }

  record Component(String leftVar, String rightVar, CPQ cpq, String description) {
    Component {
      Objects.requireNonNull(leftVar, "leftVar");
      Objects.requireNonNull(rightVar, "rightVar");
      Objects.requireNonNull(cpq, "cpq");
      Objects.requireNonNull(description, "description");
      if (leftVar.isBlank() || rightVar.isBlank()) {
        throw new IllegalArgumentException("Variables cannot be blank");
      }
    }
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

  private static final class RelationBinding {
    private final String sourceVar;
    private final String targetVar;
    private final String description;
    private final RelationProjection projection;
    private final int[] unaryDomain;

    private RelationBinding(
        String sourceVar, String targetVar, String description, RelationProjection projection) {
      this.sourceVar = Objects.requireNonNull(sourceVar, "sourceVar");
      this.targetVar = Objects.requireNonNull(targetVar, "targetVar");
      this.description = Objects.requireNonNull(description, "description");
      this.projection = Objects.requireNonNull(projection, "projection");
      this.unaryDomain = null;
    }

    private RelationBinding(String variable, String description, int[] unaryDomain) {
      this.sourceVar = Objects.requireNonNull(variable, "variable");
      this.targetVar = null;
      this.description = Objects.requireNonNull(description, "description");
      this.projection = null;
      this.unaryDomain = Objects.requireNonNull(unaryDomain, "unaryDomain");
    }

    static RelationBinding binary(
        String sourceVar, String targetVar, String description, RelationProjection projection) {
      return new RelationBinding(sourceVar, targetVar, description, projection);
    }

    static RelationBinding unary(String variable, String description, int[] domain) {
      return new RelationBinding(variable, description, domain);
    }

    void register(Map<String, List<RelationBinding>> relationsPerVariable) {
      relationsPerVariable.computeIfAbsent(sourceVar, ignored -> new ArrayList<>()).add(this);
      if (targetVar != null) {
        relationsPerVariable.computeIfAbsent(targetVar, ignored -> new ArrayList<>()).add(this);
      }
    }

    int[] domainFor(String variable, Map<String, Integer> assignment) {
      if (unaryDomain != null) {
        return unaryDomain;
      }
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
      this.allSources = Objects.requireNonNull(allSources, "allSources");
      this.allTargets = Objects.requireNonNull(allTargets, "allTargets");
      this.forward = Objects.requireNonNull(forward, "forward");
      this.reverse = Objects.requireNonNull(reverse, "reverse");
    }

    boolean isEmpty() {
      return allSources.length == 0 || allTargets.length == 0;
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
      Arrays.sort(result);
      int unique = 1;
      for (int i = 1; i < result.length; i++) {
        if (result[i] != result[unique - 1]) {
          result[unique++] = result[i];
        }
      }
      if (unique == result.length) {
        return result;
      }
      int[] trimmed = new int[unique];
      System.arraycopy(result, 0, trimmed, 0, unique);
      return trimmed;
    }
  }

  private static final class LeapfrogIterator {
    private final List<IntCursor> cursors;
    private int index;

    LeapfrogIterator(List<IntCursor> cursors) {
      this.cursors = cursors;
      this.index = 0;
    }

    void init() {
      if (cursors.isEmpty()) {
        return;
      }
      for (IntCursor cursor : cursors) {
        cursor.seekToStart();
      }
      index = 0;
      leapfrogSearch();
    }

    boolean atEnd() {
      return cursors.isEmpty() || cursors.get(index).atEnd();
    }

    int key() {
      return cursors.get(index).key();
    }

    void next() {
      if (atEnd()) {
        return;
      }
      cursors.get(index).next();
      leapfrogSearch();
    }

    private void leapfrogSearch() {
      if (cursors.isEmpty()) {
        return;
      }
      while (true) {
        IntCursor current = cursors.get(index);
        IntCursor prev = cursors.get((index + cursors.size() - 1) % cursors.size());
        if (current.atEnd() || prev.atEnd()) {
          return;
        }
        if (current.key() == prev.key()) {
          return;
        }
        current.seek(prev.key());
        index = (index + 1) % cursors.size();
      }
    }

    static final class IntCursor {
      private final int[] values;
      private int position;

      IntCursor(int[] values) {
        this.values = values;
        this.position = 0;
      }

      void seekToStart() {
        position = 0;
      }

      boolean atEnd() {
        return position >= values.length;
      }

      int key() {
        return values[position];
      }

      void next() {
        position++;
      }

      void seek(int target) {
        int low = position;
        int high = values.length - 1;
        while (low <= high) {
          int mid = (low + high) >>> 1;
          if (values[mid] < target) {
            low = mid + 1;
          } else {
            high = mid - 1;
          }
        }
        position = low;
      }
    }
  }
}
