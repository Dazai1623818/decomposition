package decomposition;

import decomposition.core.CPQExpression;
import decomposition.decompose.Decomposer;
import decomposition.eval.EvaluationRun;
import dev.roanh.gmark.lang.cq.CQ;
import java.util.List;

/**
 * Minimal entrypoint for the project.
 *
 * <p>Runs all decomposition modes on a built-in example CQ and prints the resulting CPQs with
 * performance metrics.
 *
 * <p>Usage:
 *
 * <ul>
 *   <li>{@code Main} — uses example1
 *   <li>{@code Main 3} — uses example3
 * </ul>
 */
public final class Main {

  private Main() {}

  public static void main(String[] args) {
    int exampleId = args != null && args.length > 0 ? parseExampleId(args[0]) : 1;
    CQ cq = pickExample(exampleId);

    System.out.println("Running decomposition on example" + exampleId);
    System.out.println("=".repeat(60));

    // 1. Single-edge baseline
    runSingleEdge(cq);

    // 2. Exhaustive (sequential)
    runExhaustiveSequential(cq);

    // 3. Exhaustive (parallel)
    runExhaustiveParallel(cq);

    // 4. CPQ-k Enumeration
    runCpqk(cq);

    // 5. Summary comparison
    printSummary(cq);
  }

  private static void runSingleEdge(CQ cq) {
    System.out.println("\n[1] SINGLE-EDGE DECOMPOSITION");
    System.out.println("-".repeat(40));

    long start = System.nanoTime();
    List<List<CPQExpression>> single =
        Decomposer.decompose(cq, Decomposer.DecompositionMethod.SINGLE_EDGE);
    long end = System.nanoTime();

    System.out.printf("  Result: %d decomposition(s)%n", single.size());
    printDecompositions(single);
    System.out.printf("  Time:   %.2f ms%n", (end - start) / 1_000_000.0);
  }

  private static void runExhaustiveSequential(CQ cq) {
    System.out.println("\n[2] EXHAUSTIVE (Sequential)");
    System.out.println("-".repeat(40));

    EvaluationRun run =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);

    System.out.printf("  Result: %d decomposition(s)%n", run.size());
    printDecompositions(run.decompositions());
    printMetrics(run);
  }

  private static void runExhaustiveParallel(CQ cq) {
    System.out.println("\n[3] EXHAUSTIVE (Parallel)");
    System.out.println("-".repeat(40));

    EvaluationRun run =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_PARALLEL);

    System.out.printf("  Result: %d decomposition(s)%n", run.size());
    printDecompositions(run.decompositions());
    printMetrics(run);
  }

  private static void runCpqk(CQ cq) {
    System.out.println("\n[4] CPQ_K ENUMERATION");
    System.out.println("-".repeat(40));

    EvaluationRun run =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.CPQ_K_ENUMERATION);

    System.out.printf("  Result: %d decomposition(s)%n", run.size());
    printDecompositions(run.decompositions());
    printMetrics(run);
  }

  private static void printMetrics(EvaluationRun run) {
    System.out.printf(
        "  Components:  %d (%.2f ms)%n",
        run.componentCount(), (double) run.componentEnumerationMs());
    System.out.printf(
        "  Partitions:  %d (%.2f ms)%n",
        run.partitionsExplored(), (double) run.partitionGenerationMs());
    System.out.printf("  Expression:  %.2f ms%n", (double) run.expressionBuildingMs());
    System.out.printf("  Dedup:       %.2f ms%n", (double) run.deduplicationMs());
    System.out.printf("  TOTAL:       %.2f ms%n", (double) run.totalMs());
    System.out.println();
    System.out.println(run.percentageBreakdown());
  }

  private static void printDecompositions(List<List<CPQExpression>> decompositions) {
    if (decompositions.isEmpty()) {
      System.out.println("  Decompositions: (none)");
      return;
    }

    System.out.println("  Decompositions:");
    for (int i = 0; i < decompositions.size(); i++) {
      List<CPQExpression> tuple = decompositions.get(i);
      System.out.printf("    #%d %s%n", i + 1, formatTuple(tuple));
    }
  }

  private static String formatTuple(List<CPQExpression> tuple) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < tuple.size(); i++) {
      CPQExpression expr = tuple.get(i);
      if (i > 0) {
        sb.append(" | ");
      }
      sb.append(expr.cpq())
          .append(" [")
          .append(expr.source())
          .append(" -> ")
          .append(expr.target())
          .append("]");
    }
    return sb.toString();
  }

  private static void printSummary(CQ cq) {
    System.out.println("\n" + "=".repeat(60));
    System.out.println("PERFORMANCE COMPARISON SUMMARY");
    System.out.println("=".repeat(60));

    // Run all methods and collect times
    long singleStart = System.nanoTime();
    Decomposer.decompose(cq, Decomposer.DecompositionMethod.SINGLE_EDGE);
    long singleTime = (System.nanoTime() - singleStart) / 1_000_000;

    EvaluationRun seqRun =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);
    long seqTime = seqRun.totalMs();

    EvaluationRun parRun =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_PARALLEL);
    long parTime = parRun.totalMs();

    EvaluationRun cpqkRun =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.CPQ_K_ENUMERATION);
    long cpqkTime = cpqkRun.totalMs();

    System.out.printf("%-25s %8s %14s%n", "Method", "Time (ms)", "vs Sequential");
    System.out.println("-".repeat(45));
    System.out.printf("%-25s %8d %10s%n", "Single-edge", singleTime, "-");
    System.out.printf("%-25s %8d %14s%n", "Exhaustive Sequential", seqTime, "1.00x");
    System.out.printf(
        "%-25s %8d %14.2fx%n",
        "Exhaustive Parallel", parTime, seqTime > 0 ? (double) seqTime / parTime : 0);
    System.out.printf(
        "%-25s %8d %14.2fx%n",
        "CPQ-k Enumeration", cpqkTime, seqTime > 0 ? (double) seqTime / cpqkTime : 0);
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
      return 1;
    }
  }
}
