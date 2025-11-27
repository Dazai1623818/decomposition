package decomposition.eval;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reporting helpers shared by the evaluation runner and the new pipeline. */
final class DecompositionComparisonReporter {
  private DecompositionComparisonReporter() {}

  static void report(
      String label,
      List<Map<String, Integer>> baseline,
      List<Map<String, Integer>> decomposition,
      Set<String> freeVariables) {
    Set<Map<String, Integer>> baselineSet = project(baseline, freeVariables, decomposition);
    Set<Map<String, Integer>> decompositionSet =
        project(decomposition, freeVariables, decomposition);
    System.out.println(
        "Projected (free vars) result count - baseline: "
            + baselineSet.size()
            + ", decomposition: "
            + decompositionSet.size());
    if (baselineSet.equals(decompositionSet)) {
      System.out.println("Joined decomposition '" + label + "' matches single-edge evaluation.");
      return;
    }
    System.out.println(
        "Joined decomposition '" + label + "' does NOT match single-edge evaluation.");
    if (!decompositionSet.isEmpty()) {
      System.out.println("Sample joined assignments:");
      decompositionSet.stream()
          .limit(5)
          .forEach(assignment -> System.out.println("  " + formatAssignment(assignment)));
    } else {
      System.out.println("Joined assignments empty.");
    }
    printDifference("Missing assignments (baseline only):", baselineSet, decompositionSet);
    printDifference("Extra assignments (decomposition only):", decompositionSet, baselineSet);
  }

  static boolean compare(
      List<Map<String, Integer>> baseline,
      List<Map<String, Integer>> decomposition,
      Set<String> freeVariables) {
    Set<Map<String, Integer>> baselineSet = project(baseline, freeVariables, decomposition);
    Set<Map<String, Integer>> decompositionSet =
        project(decomposition, freeVariables, decomposition);
    return baselineSet.equals(decompositionSet);
  }

  static String formatAssignment(Map<String, Integer> assignment) {
    Map<String, Integer> ordered = new LinkedHashMap<>(assignment);
    return ordered.toString();
  }

  static Set<Map<String, Integer>> project(
      List<Map<String, Integer>> assignments, Set<String> declaredVariables) {
    return project(assignments, declaredVariables, assignments);
  }

  static Set<Map<String, Integer>> project(
      List<Map<String, Integer>> assignments,
      Set<String> declaredVariables,
      List<Map<String, Integer>> decomposition) {
    List<String> variableOrder = orderedVariables(declaredVariables, decomposition);
    return projectAssignments(assignments, variableOrder);
  }

  private static List<String> orderedVariables(
      Set<String> declaredVariables, List<Map<String, Integer>> fallbackAssignments) {
    List<String> ordered = new ArrayList<>();
    if (declaredVariables != null) {
      for (String variable : declaredVariables) {
        if (variable != null && !variable.isBlank()) {
          ordered.add(variable);
        }
      }
    }
    if (!ordered.isEmpty()) {
      return ordered;
    }
    // Fallback: infer from decomposition output if no declared variables provided.
    for (Map<String, Integer> assignment : fallbackAssignments) {
      for (String variable : assignment.keySet()) {
        if (variable != null && !variable.isBlank()) {
          ordered.add(variable);
        }
      }
      if (!ordered.isEmpty()) {
        break;
      }
    }
    return ordered;
  }

  private static Set<Map<String, Integer>> projectAssignments(
      List<Map<String, Integer>> assignments, List<String> variableOrder) {
    Set<Map<String, Integer>> projected = new LinkedHashSet<>();
    for (Map<String, Integer> assignment : assignments) {
      Map<String, Integer> view = new LinkedHashMap<>();
      for (String variable : variableOrder) {
        if (assignment.containsKey(variable)) {
          view.put(variable, assignment.get(variable));
        }
      }
      projected.add(view);
    }
    return projected;
  }

  private static void printDifference(
      String header, Set<Map<String, Integer>> first, Set<Map<String, Integer>> second) {
    Set<Map<String, Integer>> difference = new LinkedHashSet<>(first);
    difference.removeAll(second);
    if (difference.isEmpty()) {
      return;
    }
    System.out.println(header);
    difference.stream()
        .limit(5)
        .forEach(assignment -> System.out.println("  " + formatAssignment(assignment)));
    if (difference.size() > 5) {
      System.out.println("  ... truncated ...");
    }
  }
}
