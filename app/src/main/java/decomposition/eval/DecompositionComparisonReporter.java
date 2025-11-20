package decomposition.eval;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reporting helpers shared by the evaluation runner and the new pipeline. */
final class DecompositionComparisonReporter {
  private DecompositionComparisonReporter() {}

  static void report(
      String label, List<Map<String, Integer>> baseline, List<Map<String, Integer>> decomposition) {
    Set<Map<String, Integer>> baselineSet = new LinkedHashSet<>(baseline);
    Set<Map<String, Integer>> decompositionSet = new LinkedHashSet<>(decomposition);
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

  static String formatAssignment(Map<String, Integer> assignment) {
    Map<String, Integer> ordered = new LinkedHashMap<>(assignment);
    return ordered.toString();
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
