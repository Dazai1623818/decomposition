package decomposition.eval;

import decomposition.core.DecompositionResult;
import decomposition.core.model.Partition;
import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.ProgressListener;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DecompositionComparisonPipeline {
  private final Path graphPath;
  private final Path nativeLibrary;
  private final List<Path> decompositionInputs;

  public DecompositionComparisonPipeline(
      Path graphPath, Path nativeLibrary, List<Path> decompositionInputs) {
    this.graphPath = Objects.requireNonNull(graphPath, "graphPath");
    this.nativeLibrary = Objects.requireNonNull(nativeLibrary, "nativeLibrary");
    this.decompositionInputs = decompositionInputs == null ? List.of() : List.copyOf(decompositionInputs);
  }

  public void evaluate(CQ query, DecompositionResult result) throws IOException {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");

    System.load(nativeLibrary.toAbsolutePath().toString());
    CpqNativeIndex index = buildIndex(graphPath);
    LeapfrogCpqJoiner joiner = LeapfrogCpqJoiner.fromIndex(index);

    System.out.println("Loaded single-edge labels: " + joiner.singleEdgeLabels());
    System.out.println("Executing query: " + query.toFormalSyntax());
    List<Map<String, Integer>> baseline = joiner.executeBaseline(query);
    System.out.println("Single-edge result count: " + baseline.size());
    for (int i = 0; i < Math.min(10, baseline.size()); i++) {
      System.out.println(
          "  [baseline] " + DecompositionComparisonReporter.formatAssignment(baseline.get(i)));
    }

    JoinedDecompositionExecutor executor = new JoinedDecompositionExecutor(joiner);
    boolean executed = false;

    List<Partition> partitions = result.cpqPartitions();
    for (int i = 0; i < partitions.size(); i++) {
      Partition partition = partitions.get(i);
      String label = "pipeline-partition-" + (i + 1);
      executor.setDebugLabel(label);
      List<Map<String, Integer>> joinedResults = executor.execute(partition, query, result.edges());
      System.out.println("Decomposition '" + label + "' result count: " + joinedResults.size());
      DecompositionComparisonReporter.report(label, baseline, joinedResults);
      executed = true;
    }

    List<PartitionDecompositionLoader.NamedDecomposition> external = loadExternalDecompositions(query);
    for (PartitionDecompositionLoader.NamedDecomposition task : external) {
      List<Map<String, Integer>> joinedResults = executor.execute(task.decomposition());
      System.out.println(
          "Decomposition '" + task.name() + "' result count: " + joinedResults.size());
      DecompositionComparisonReporter.report(task.name(), baseline, joinedResults);
      executed = true;
    }

    if (!executed) {
      System.out.println("No decompositions available for comparison.");
    }
  }

  private CpqNativeIndex buildIndex(Path graphPath) throws IOException {
    try {
      UniqueGraph<Integer, Predicate> graph = GraphLoader.load(graphPath);
      CpqNativeIndex index = new CpqNativeIndex(
          graph,
          3,
          false,
          true,
          Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
          -1,
          ProgressListener.none());
      index.sort();
      return index;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
  }

  private List<PartitionDecompositionLoader.NamedDecomposition> loadExternalDecompositions(CQ query)
      throws IOException {
    List<PartitionDecompositionLoader.NamedDecomposition> tasks = new ArrayList<>();
    if (decompositionInputs.isEmpty()) {
      return tasks;
    }
    PartitionDecompositionLoader loader = PartitionDecompositionLoader.forQuery(query);
    for (Path input : decompositionInputs) {
      List<PartitionDecompositionLoader.NamedDecomposition> loaded = loader.load(input);
      if (loaded.isEmpty()) {
        System.out.println("No decompositions found in " + input);
        continue;
      }
      boolean directory = Files.isDirectory(input);
      for (PartitionDecompositionLoader.NamedDecomposition named : loaded) {
        String label = directory ? input + "/" + named.name() : input.toString();
        tasks.add(
            new PartitionDecompositionLoader.NamedDecomposition(label, named.decomposition()));
      }
    }
    return tasks;
  }
}
