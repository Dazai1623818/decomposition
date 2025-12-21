package decomposition.cli;

import decomposition.cpq.CPQExpression;
import decomposition.decompose.Decomposer;
import decomposition.eval.EvaluationRun;
import decomposition.examples.Example;
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

    // 4. Summary comparison
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
    System.out.printf("  Time:   %.2f ms%n", (end - start) / 1_000_000.0);
  }

  private static void runExhaustiveSequential(CQ cq) {
    System.out.println("\n[2] EXHAUSTIVE (Sequential)");
    System.out.println("-".repeat(40));

    EvaluationRun run =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_ENUMERATION);

    System.out.printf("  Result: %d decomposition(s)%n", run.size());
    printMetrics(run);
  }

  private static void runExhaustiveParallel(CQ cq) {
    System.out.println("\n[3] EXHAUSTIVE (Parallel)");
    System.out.println("-".repeat(40));

    EvaluationRun run =
        Decomposer.decomposeWithRun(cq, Decomposer.DecompositionMethod.EXHAUSTIVE_PARALLEL);

    System.out.printf("  Result: %d decomposition(s)%n", run.size());
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

    System.out.printf("%-25s %8s %14s%n", "Method", "Time (ms)", "vs Sequential");
    System.out.println("-".repeat(45));
    System.out.printf("%-25s %8d %10s%n", "Single-edge", singleTime, "-");
    System.out.printf("%-25s %8d %14s%n", "Exhaustive Sequential", seqTime, "1.00x");
    System.out.printf(
        "%-25s %8d %14.2fx%n",
        "Exhaustive Parallel", parTime, seqTime > 0 ? (double) seqTime / parTime : 0);
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
