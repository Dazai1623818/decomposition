package decomposition.eval;

import decomposition.cpq.CPQExpression;
import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.CpqNativeIndex.Pair;
import dev.roanh.gmark.lang.cpq.CPQ;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Executes CPQ components against the native index and joins their assignments. */
final class CpqIndexExecutor {
  private final CpqNativeIndex index;

  CpqIndexExecutor(CpqNativeIndex index) {
    this.index = Objects.requireNonNull(index, "index");
  }

  List<Map<String, Integer>> execute(List<Component> components) {
    Objects.requireNonNull(components, "components");
    if (components.isEmpty()) {
      return List.of();
    }
    List<Map<String, Integer>> joined = null;
    for (Component component : components) {
      List<Map<String, Integer>> tuples = evaluateComponent(component);
      if (joined == null) {
        joined = tuples;
      } else {
        joined = naturalJoin(joined, tuples);
      }
      if (joined.isEmpty()) {
        return List.of();
      }
    }
    return joined == null ? List.of() : joined;
  }

  private List<Map<String, Integer>> evaluateComponent(Component component) {
    List<Pair> matches;
    try {
      matches = index.query(component.cpq());
    } catch (IllegalArgumentException ex) {
      throw new IllegalStateException(
          "Failed to evaluate component '" + component.description() + "'", ex);
    }
    if (matches.isEmpty()) {
      return List.of();
    }
    List<Map<String, Integer>> assignments = new ArrayList<>(matches.size());
    for (Pair pair : matches) {
      Map<String, Integer> assignment = new LinkedHashMap<>(2);
      if (component.leftVar() != null) {
        assignment.put(component.leftVar(), pair.getSource());
      }
      if (component.rightVar() != null) {
        if (component.leftVar() != null
            && component.leftVar().equals(component.rightVar())
            && pair.getSource() != pair.getTarget()) {
          // Component expects the same variable on both endpoints; skip inconsistent tuples.
          continue;
        }
        assignment.put(component.rightVar(), pair.getTarget());
      }
      assignments.add(assignment);
    }
    return assignments;
  }

  private static List<Map<String, Integer>> naturalJoin(
      List<Map<String, Integer>> left, List<Map<String, Integer>> right) {
    if (left.isEmpty() || right.isEmpty()) {
      return List.of();
    }
    List<Map<String, Integer>> result = new ArrayList<>();
    for (Map<String, Integer> leftRow : left) {
      for (Map<String, Integer> rightRow : right) {
        if (!compatible(leftRow, rightRow)) {
          continue;
        }
        Map<String, Integer> joined = new LinkedHashMap<>(leftRow);
        for (Map.Entry<String, Integer> entry : rightRow.entrySet()) {
          joined.put(entry.getKey(), entry.getValue());
        }
        result.add(joined);
      }
    }
    return result;
  }

  private static boolean compatible(Map<String, Integer> left, Map<String, Integer> right) {
    for (Map.Entry<String, Integer> entry : left.entrySet()) {
      Integer other = right.get(entry.getKey());
      if (other != null && !Objects.equals(entry.getValue(), other)) {
        return false;
      }
    }
    return true;
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
      Objects.requireNonNull(cpq, "cpq");
      Objects.requireNonNull(description, "description");
    }
  }
}
