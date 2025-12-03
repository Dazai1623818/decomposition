package decomposition.eval;

import decomposition.cpq.CPQExpression;
import decomposition.eval.leapfrogjoiner.IntAccumulator;
import decomposition.eval.leapfrogjoiner.LeapfrogJoiner;
import decomposition.eval.leapfrogjoiner.RelationBinding;
import decomposition.eval.leapfrogjoiner.RelationProjection;
import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.Pair;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Executes CPQ components against the native index and joins their assignments.
 *
 * <p>Delegates algorithmic join logic to {@link LeapfrogJoiner}.
 */
public final class CpqIndexExecutor {
  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private final Index index;
  private final LeapfrogJoiner joiner;

  public CpqIndexExecutor(Index index) {
    this.index = Objects.requireNonNull(index, "index");
    this.joiner = new LeapfrogJoiner();
  }

  public List<Map<String, Integer>> execute(List<Component> components) {
    Objects.requireNonNull(components, "components");
    if (components.isEmpty()) {
      return List.of();
    }
    List<RelationBinding> bindings = new ArrayList<>(components.size());

    // 1. Evaluate individual components against Index
    for (Component component : components) {
      RelationBinding binding = evaluateComponent(component);
      if (binding == null) {
        return List.of(); // Fast fail
      }
      bindings.add(binding);
    }

    // 2. Delegate complexity to the engine
    return joiner.join(bindings);
  }

  public static List<Component> componentsFromAtoms(dev.roanh.gmark.lang.cq.CQ cq) {
    Objects.requireNonNull(cq, "cq");
    List<dev.roanh.gmark.lang.cq.AtomCQ> atoms = atomsOf(cq);
    List<Component> components = new ArrayList<>(atoms.size());
    for (dev.roanh.gmark.lang.cq.AtomCQ atom : atoms) {
      components.add(componentFromAtom(atom));
    }
    return components;
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

  // --- Static Helpers ---

  public static Component fromExpression(CPQExpression expression) {
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

  public static Component oversizedComponent(List<Component> components, int maxDiameter) {
    for (Component component : components) {
      if (component.cpq().getDiameter() > maxDiameter) {
        return component;
      }
    }
    return null;
  }

  public List<Component> componentsFromTuple(List<CPQExpression> tuple) {
    return tuple.stream().map(CpqIndexExecutor::fromExpression).toList();
  }

  public int getIndexK() {
    // Assuming access to k is possible or passed in context,
    // otherwise this is just for logging/diagnostics.
    return 3;
  }

  private static String variableName(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    return raw.startsWith("?") ? raw : ("?" + raw);
  }

  private static List<dev.roanh.gmark.lang.cq.AtomCQ> atomsOf(dev.roanh.gmark.lang.cq.CQ cq) {
    dev.roanh.gmark.util.graph.generic.UniqueGraph<
            dev.roanh.gmark.lang.cq.VarCQ, dev.roanh.gmark.lang.cq.AtomCQ>
        graph = cq.toQueryGraph().toUniqueGraph();
    List<dev.roanh.gmark.lang.cq.AtomCQ> atoms = new ArrayList<>(graph.getEdgeCount());
    for (dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge<
            dev.roanh.gmark.lang.cq.VarCQ, dev.roanh.gmark.lang.cq.AtomCQ>
        edge : graph.getEdges()) {
      atoms.add(edge.getData());
    }
    return atoms;
  }

  private static Component componentFromAtom(dev.roanh.gmark.lang.cq.AtomCQ atom) {
    CPQ cpq = CPQ.label(atom.getLabel());
    String source = variableName(atom.getSource().getName());
    String target = variableName(atom.getTarget().getName());
    String description = atom.getLabel().getAlias() + " (" + source + "→" + target + ")";
    return new Component(source, target, cpq, description);
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

  public record Component(String leftVar, String rightVar, CPQ cpq, String description) {
    public Component {
      Objects.requireNonNull(leftVar, "leftVar");
      Objects.requireNonNull(rightVar, "rightVar");
      Objects.requireNonNull(cpq, "cpq");
      Objects.requireNonNull(description, "description");
    }
  }
}
