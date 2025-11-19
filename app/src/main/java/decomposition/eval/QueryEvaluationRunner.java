package decomposition.eval;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.util.UniqueGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Executes the Leapfrog evaluation workflow over a graph index and optional decompositions. */
public final class QueryEvaluationRunner {
  private static final int MAX_RESULTS_TO_PRINT = 99_999;

  public void run(EvaluateOptions options) throws IOException {
    System.load(options.nativeLibrary().toAbsolutePath().toString());

    Index index;
    try {
      UniqueGraph<Integer, dev.roanh.gmark.core.graph.Predicate> graph =
          GraphLoader.load(options.graphPath());
      index =
          new Index(
              graph,
              1,
              false,
              true,
              Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
              -1,
              ProgressListener.NONE);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
    index.sort();

    LeapfrogEdgeJoiner joiner = LeapfrogEdgeJoiner.fromIndex(index);
    System.out.println("Loaded labels: " + joiner.labels());

    ExampleQuery example = selectExample(options.exampleName());
    CQ cq = example.cq();
    List<Map<String, Integer>> results = joiner.execute(cq);

    System.out.println(
        "Executing example '" + options.exampleName() + "' expressed in gMark notation.");
    System.out.println("gMark CQ: " + cq.toFormalSyntax());
    System.out.println("Result count: " + results.size());
    results.stream()
        .limit(MAX_RESULTS_TO_PRINT)
        .forEach(
            assignment ->
                System.out.println(DecompositionComparisonReporter.formatAssignment(assignment)));
    if (results.size() > MAX_RESULTS_TO_PRINT) {
      System.out.println("... truncated ...");
    }

    List<DecompositionTask> tasks = new ArrayList<>();
    example
        .decomposition()
        .ifPresent(
            decomposition ->
                tasks.add(
                    new DecompositionTask("example-" + options.exampleName(), decomposition)));

    if (!options.decompositionInputs().isEmpty()) {
      PartitionDecompositionLoader loader = PartitionDecompositionLoader.forQuery(cq);
      for (Path input : options.decompositionInputs()) {
        List<PartitionDecompositionLoader.NamedDecomposition> loaded = loader.load(input);
        if (loaded.isEmpty()) {
          System.out.println("No decompositions found in " + input);
          continue;
        }
        boolean directory = Files.isDirectory(input);
        for (PartitionDecompositionLoader.NamedDecomposition named : loaded) {
          String label = directory ? input + "/" + named.name() : input.toString();
          tasks.add(new DecompositionTask(label, named.decomposition()));
        }
      }
    }

    if (!tasks.isEmpty()) {
      JoinedDecompositionExecutor executor = new JoinedDecompositionExecutor(joiner);
      for (DecompositionTask task : tasks) {
        List<Map<String, Integer>> joinedResults = executor.execute(task.decomposition());
        System.out.println(
            "Decomposition '" + task.label() + "' result count: " + joinedResults.size());
        DecompositionComparisonReporter.report(task.label(), results, joinedResults);
      }
    }
  }

  private ExampleQuery selectExample(String exampleName) {
    if ("example1".equalsIgnoreCase(exampleName)) {
      return ExampleQueries.example1();
    }
    throw new IllegalArgumentException(
        "Unknown example '" + exampleName + "'. Available examples: example1");
  }

  /** Options forwarded from the CLI for evaluate command runs. */
  public record EvaluateOptions(
      String exampleName, Path graphPath, Path nativeLibrary, List<Path> decompositionInputs) {
    public EvaluateOptions {
      Objects.requireNonNull(exampleName, "exampleName");
      Objects.requireNonNull(graphPath, "graphPath");
      Objects.requireNonNull(nativeLibrary, "nativeLibrary");
      decompositionInputs =
          decompositionInputs == null ? List.of() : List.copyOf(decompositionInputs);
    }
  }

  private record DecompositionTask(String label, QueryDecomposition decomposition) {}
}
