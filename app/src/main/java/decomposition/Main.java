package decomposition;

import decomposition.core.CPQExpression;
import decomposition.decompose.Decomposer;
import decomposition.eval.EvaluationIndex;
import decomposition.eval.EvaluationRun;
import dev.roanh.gmark.lang.cq.CQ;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Minimal entrypoint for the project.
 *
 * <p>Runs all decomposition modes on a built-in example CQ and prints the resulting CPQs with
 * performance metrics.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>{@code Main} — runs all examples (1..8)
 *   <li>{@code Main all} — runs all examples (1..8)
 *   <li>{@code Main 3} — runs example3 only
 * </ul>
 */
public final class Main {

  private static final int[] ALL_EXAMPLES = {1, 2, 3, 4, 5, 6, 7, 8};

  private record DecompositionTiming(
      int index,
      long evaluationMs,
      int answerCount,
      boolean matchesBaseline,
      List<Integer> relationSizes) {}

  private record EvaluationComparison(
      int decompositionCount,
      Set<Map<String, Integer>> baselineResults,
      Set<Map<String, Integer>> unionResults,
      boolean allAgree,
      int mismatches,
      long evaluationMs,
      List<DecompositionTiming> timings) {}

  private record SummaryEntry(EvaluationRun run, EvaluationComparison evaluation) {}

  private Main() {}

  public static void main(String[] args) {
    // Initialize EvaluationIndex
    // Use the larger 2kv50keall graph by default.
    Path graphPath = Path.of("graphs/2kv50keall.edge");
    int k = 2;

    try (EvaluationIndex index = new EvaluationIndex(graphPath, k)) {
      int[] exampleIds = resolveExampleIds(args);
      for (int exampleId : exampleIds) {
        CQ cq = pickExample(exampleId);
        System.out.println("\n" + "=".repeat(60));
        System.out.println("Running decomposition on example " + exampleId);
        System.out.println("=".repeat(60));

        // 1. Single-edge baseline
        SummaryEntry single = runSingleEdge(cq, index);

        // 2. Exhaustive (sequential)
        SummaryEntry seq = runExhaustiveSequential(cq, index);

        // 3. CPQ-k Enumeration
        SummaryEntry cpqk = runCpqk(cq, index);

        // 4. Summary comparison
        printSummary(exampleId, single, seq, cpqk);
      }
    } catch (Exception e) {
      System.err.println("Error during execution:");
      e.printStackTrace();
    }
  }

  private static SummaryEntry runSingleEdge(CQ cq, EvaluationIndex index) {
    System.out.println("\n[1] SINGLE-EDGE DECOMPOSITION");

    long startDecomp = System.nanoTime();
    List<List<CPQExpression>> single =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.SINGLE_EDGE);
    long endDecomp = System.nanoTime();
    long decompMs = (endDecomp - startDecomp) / 1_000_000;
    EvaluationRun run = new EvaluationRun();
    run.setDecompositions(single);
    run.recordPhaseMs(EvaluationRun.Phase.TOTAL, decompMs);
    EvaluationComparison evaluation = evaluateAllDecompositions(cq, single, index, run);

    System.out.printf("Result: %d decomposition(s)%n", single.size());
    printDecompositions(single, evaluation.timings());
    System.out.printf("Decomp Time: %.2f ms%n", (double) decompMs);
    System.out.printf("Eval Time: %.2f ms%n", (double) evaluation.evaluationMs());
    System.out.printf("Total Time: %.2f ms%n", (double) (decompMs + evaluation.evaluationMs()));
    printEvaluationSummary(evaluation);
    return new SummaryEntry(run, evaluation);
  }

  private static SummaryEntry runExhaustiveSequential(CQ cq, EvaluationIndex index) {
    System.out.println("\n[2] EXHAUSTIVE (Sequential)");

    EvaluationRun run =
        Decomposer.decomposeWithRun(
            cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION, index.k(), 0);
    EvaluationComparison evaluation =
        evaluateAllDecompositions(cq, run.decompositions(), index, run);

    System.out.printf("Result: %d decomposition(s)%n", run.size());
    printDecompositions(run.decompositions(), evaluation.timings());
    printMetrics(run);
    printEvaluationSummary(evaluation);
    return new SummaryEntry(run, evaluation);
  }

  private static SummaryEntry runExhaustiveParallel(CQ cq, EvaluationIndex index) {
    System.out.println("\n[3] EXHAUSTIVE (Parallel)");

    EvaluationRun run =
        Decomposer.decomposeWithRun(
            cq, Decomposer.DecompositionMethod.EXHAUSTIVE_PARALLEL, index.k(), 0);
    EvaluationComparison evaluation =
        evaluateAllDecompositions(cq, run.decompositions(), index, run);

    System.out.printf("Result: %d decomposition(s)%n", run.size());
    printDecompositions(run.decompositions(), evaluation.timings());
    printMetrics(run);
    printEvaluationSummary(evaluation);
    return new SummaryEntry(run, evaluation);
  }

  private static SummaryEntry runCpqk(CQ cq, EvaluationIndex index) {
    System.out.println("\n[3] CPQ_K ENUMERATION");

    EvaluationRun run =
        Decomposer.decomposeWithRun(
            cq, Decomposer.DecompositionMethod.CPQ_K_ENUMERATION, index.k(), 0);
    EvaluationComparison evaluation =
        evaluateAllDecompositions(cq, run.decompositions(), index, run);

    System.out.printf("Result: %d decomposition(s)%n", run.size());
    printDecompositions(run.decompositions(), evaluation.timings());
    printMetrics(run);
    printEvaluationSummary(evaluation);
    return new SummaryEntry(run, evaluation);
  }

  private static void printMetrics(EvaluationRun run) {
    System.out.printf(
        "Components: %d (%.2f ms)%n", run.componentCount(), (double) run.componentEnumerationMs());
    System.out.printf(
        "Partitions: %d (%.2f ms)%n",
        run.partitionsExplored(), (double) run.partitionGenerationMs());
    System.out.printf("Expression: %.2f ms%n", (double) run.expressionBuildingMs());
    System.out.printf("Dedup: %.2f ms%n", (double) run.deduplicationMs());
    System.out.printf("CPQ Eval: %.2f ms%n", (double) run.cpqEvaluationMs());
    System.out.printf("Join: %.2f ms%n", (double) run.joinMs());
    System.out.printf("TOTAL: %.2f ms%n", (double) run.totalMs());
    System.out.println();
    System.out.println(run.percentageBreakdown());
  }

  private static void printDecompositions(
      List<List<CPQExpression>> decompositions, List<DecompositionTiming> timings) {
    if (decompositions.isEmpty()) {
      System.out.println("Decompositions: (none)");
      return;
    }

    System.out.println("Decompositions:");
    for (int i = 0; i < decompositions.size(); i++) {
      List<CPQExpression> tuple = decompositions.get(i);
      DecompositionTiming timing = timings != null && i < timings.size() ? timings.get(i) : null;
      long evalMs = timing != null ? timing.evaluationMs() : -1;
      int joins = Math.max(0, tuple.size() - 1);
      List<Integer> sizes = timing != null ? timing.relationSizes() : null;
      String tupleText = formatTuple(tuple, sizes);
      if (evalMs >= 0) {
        System.out.printf("#%2d joins=%d eval=%3d ms %s%n", i + 1, joins, evalMs, tupleText);
      } else {
        System.out.printf("#%2d joins=%d %s%n", i + 1, joins, tupleText);
      }
    }
  }

  private static String formatTuple(List<CPQExpression> tuple, List<Integer> relationSizes) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tuple.size(); i++) {
      CPQExpression expr = tuple.get(i);
      if (i > 0) {
        sb.append(" | ");
      }
      int size = 0;
      if (relationSizes != null && i < relationSizes.size()) {
        size = relationSizes.get(i);
      }
      sb.append(expr.cpq());
      sb.append("[");
      sb.append(size);
      sb.append("]");
    }
    return sb.toString();
  }

  private static void printSummary(
      int exampleId,
      SummaryEntry singleSummary,
      SummaryEntry seqSummary,
      SummaryEntry cpqkSummary) {
    System.out.println("\n" + "=".repeat(60));
    System.out.println("PERFORMANCE COMPARISON SUMMARY (example " + exampleId + ")");
    System.out.println("=".repeat(60));

    long singleTime = singleSummary.run().totalMs();
    long seqTime = seqSummary.run().totalMs();
    long cpqkTime = cpqkSummary.run().totalMs();

    System.out.printf("%-25s %8s %14s%n", "Method", "Time (ms)", "vs Sequential");
    System.out.println("-".repeat(45));
    System.out.printf("%-25s %8d %10s%n", "Single-edge", singleTime, "-");
    System.out.printf("%-25s %8d %14s%n", "Exhaustive Sequential", seqTime, "1.00x");
    System.out.printf(
        "%-25s %8d %14.2fx%n",
        "CPQ-k Enumeration", cpqkTime, seqTime > 0 ? (double) seqTime / cpqkTime : 0);

    System.out.println();
    System.out.println("Answer comparison (projected to free vars):");
    printAnswerComparison("Single-edge", singleSummary.evaluation(), singleSummary.evaluation());
    printAnswerComparison(
        "Exhaustive Sequential", seqSummary.evaluation(), singleSummary.evaluation());
    printAnswerComparison(
        "CPQ-k Enumeration", cpqkSummary.evaluation(), singleSummary.evaluation());
  }

  private static CQ pickExample(int id) {
    return switch (id) {
      case 2 -> Example.example2();
      case 3 -> Example.example3();
      case 4 -> Example.example4();
      case 5 -> Example.example5();
      case 6 -> Example.example6();
      case 7 -> Example.example7();
      case 8 -> Example.example8();
      default -> Example.example1();
    };
  }

  private static int parseExampleId(String raw) {
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException ex) {
      return -1;
    }
  }

  private static int[] resolveExampleIds(String[] args) {
    if (args == null || args.length == 0) {
      return ALL_EXAMPLES;
    }
    if (args.length == 1 && "all".equalsIgnoreCase(args[0].trim())) {
      return ALL_EXAMPLES;
    }

    Set<Integer> ids = new LinkedHashSet<>();
    for (String arg : args) {
      if (arg == null) {
        continue;
      }
      String trimmed = arg.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      if ("all".equalsIgnoreCase(trimmed)) {
        return ALL_EXAMPLES;
      }
      int id = parseExampleId(trimmed);
      if (id > 0) {
        ids.add(id);
      }
    }

    if (ids.isEmpty()) {
      return ALL_EXAMPLES;
    }
    return ids.stream().mapToInt(Integer::intValue).toArray();
  }

  private static EvaluationComparison evaluateAllDecompositions(
      CQ cq, List<List<CPQExpression>> decompositions, EvaluationIndex index, EvaluationRun run) {
    List<String> freeVars = normalizedFreeVars(cq);
    Set<Map<String, Integer>> union = new LinkedHashSet<>();
    Set<Map<String, Integer>> baseline = null;
    boolean allAgree = true;
    int mismatches = 0;
    long evalMs = 0L;
    List<DecompositionTiming> timings = new ArrayList<>();

    int ordinal = 1;
    for (List<CPQExpression> tuple : decompositions) {
      long start = System.nanoTime();
      EvaluationIndex.DecompositionEval evaluation =
          index.evaluateDecompositionWithStats(tuple, run);
      long end = System.nanoTime();
      long elapsedMs = (end - start) / 1_000_000;
      evalMs += elapsedMs;
      List<Map<String, Integer>> raw = evaluation.results();
      Set<Map<String, Integer>> projected = projectResults(raw, freeVars);
      union.addAll(projected);
      if (baseline == null) {
        baseline = projected;
      }
      boolean matchesBaseline = baseline.equals(projected);
      if (!matchesBaseline) {
        allAgree = false;
        mismatches++;
      }
      timings.add(
          new DecompositionTiming(
              ordinal, elapsedMs, projected.size(), matchesBaseline, evaluation.relationSizes()));
      ordinal++;
    }

    if (baseline == null) {
      baseline = Set.of();
    }

    if (run != null) {
      run.setEvaluationResults(List.copyOf(union));
      run.recordPhaseMs(EvaluationRun.Phase.TOTAL, run.totalMs() + evalMs);
    }

    return new EvaluationComparison(
        decompositions.size(),
        Set.copyOf(baseline),
        Set.copyOf(union),
        allAgree,
        mismatches,
        evalMs,
        List.copyOf(timings));
  }

  private static List<String> normalizedFreeVars(CQ cq) {
    return cq.getFreeVariables().stream().map(var -> normalizeVar(var.getName())).toList();
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

  private static Set<Map<String, Integer>> projectResults(
      List<Map<String, Integer>> rows, List<String> freeVars) {
    if (rows.isEmpty()) {
      return Set.of();
    }
    if (freeVars.isEmpty()) {
      return Set.of(Map.of());
    }
    Set<Map<String, Integer>> result = new LinkedHashSet<>();
    for (Map<String, Integer> row : rows) {
      Map<String, Integer> projected = projectRow(row, freeVars);
      if (projected != null) {
        result.add(projected);
      }
    }
    return result;
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

  private static void printEvaluationSummary(EvaluationComparison evaluation) {
    System.out.printf("Answers: %d%n", evaluation.baselineResults().size());
    if (!evaluation.allAgree()) {
      System.out.printf("Union: %d%n", evaluation.unionResults().size());
    }
    if (evaluation.decompositionCount() > 1) {
      String status =
          evaluation.allAgree()
              ? "yes"
              : ("no ("
                  + evaluation.mismatches()
                  + " mismatch"
                  + (evaluation.mismatches() == 1 ? "" : "es")
                  + ")");
      System.out.printf("Consistent: %s%n", status);
    }
  }

  private static void printAnswerComparison(
      String label, EvaluationComparison evaluation, EvaluationComparison baseline) {
    boolean matches = evaluation.baselineResults().equals(baseline.baselineResults());
    String status = matches ? "matches" : "DIFFERS";
    if (!evaluation.allAgree()) {
      status = status + ", inconsistent";
    }
    System.out.printf(
        "  %-23s %8d answers (%s)%n", label, evaluation.baselineResults().size(), status);
  }

  private static void printDecompositionTimings(List<DecompositionTiming> timings) {
    if (timings.isEmpty()) {
      return;
    }
    System.out.println("  Decomposition eval times:");
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long total = 0L;
    for (DecompositionTiming timing : timings) {
      min = Math.min(min, timing.evaluationMs());
      max = Math.max(max, timing.evaluationMs());
      total += timing.evaluationMs();
      String status = timing.matchesBaseline() ? "ok" : "DIFF";
      System.out.printf(
          "    #%d %6d ms (%d answers, %s)%n",
          timing.index(), timing.evaluationMs(), timing.answerCount(), status);
    }
    if (timings.size() > 1) {
      long avg = total / timings.size();
      System.out.printf("    min/avg/max: %d/%d/%d ms%n", min, avg, max);
    }
  }
}
