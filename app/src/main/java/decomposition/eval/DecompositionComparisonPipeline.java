package decomposition.eval;

import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.ProgressListener;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DecompositionComparisonPipeline {
  private static final int INDEX_DIAMETER = 3;
  private final Path graphPath;
  private final Path nativeLibrary;
  private final List<Path> decompositionInputs;

  public DecompositionComparisonPipeline(
      Path graphPath, Path nativeLibrary, List<Path> decompositionInputs) {
    this.graphPath = Objects.requireNonNull(graphPath, "graphPath");
    this.nativeLibrary = Objects.requireNonNull(nativeLibrary, "nativeLibrary");
    this.decompositionInputs =
        decompositionInputs == null ? List.of() : List.copyOf(decompositionInputs);
  }

  public void evaluate(CQ query, DecompositionResult result) throws IOException {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");

    System.load(nativeLibrary.toAbsolutePath().toString());
    CpqNativeIndex index = buildIndex(graphPath);
    CpqIndexExecutor executor = new CpqIndexExecutor(index);

    System.out.println("Executing query: " + query.toFormalSyntax());
    List<CpqIndexExecutor.Component> baselineComponents = baselineComponents(result.edges());
    CpqIndexExecutor.Component oversizedBaseline =
        CpqIndexExecutor.oversizedComponent(baselineComponents, INDEX_DIAMETER);
    if (oversizedBaseline != null) {
      System.out.println(
          "Baseline exceeds index diameter k="
              + INDEX_DIAMETER
              + " because of component '"
              + oversizedBaseline.description()
              + "'. Comparison aborted.");
      return;
    }
    List<Map<String, Integer>> baseline = executor.execute(baselineComponents);
    System.out.println("Baseline (single-edge) result count: " + baseline.size());
    for (int i = 0; i < Math.min(10, baseline.size()); i++) {
      System.out.println(
          "  [baseline] " + DecompositionComparisonReporter.formatAssignment(baseline.get(i)));
    }

    Map<Partition, PartitionEvaluation> evaluations = mapEvaluations(result.partitionEvaluations());
    boolean executed = false;

    List<Partition> partitions = result.cpqPartitions();
    for (int i = 0; i < partitions.size(); i++) {
      Partition partition = partitions.get(i);
      PartitionEvaluation evaluation = evaluations.get(partition);
      if (evaluation == null || evaluation.decompositionTuples().isEmpty()) {
        System.out.println(
            "Skipping partition "
                + (i + 1)
                + " (no component tuples available; ensure enumeration mode is enabled).");
        continue;
      }

      int tupleIndex = 1;
      for (List<CPQExpression> tuple : evaluation.decompositionTuples()) {
        String label = "pipeline-partition-" + (i + 1) + "/tuple-" + tupleIndex++;
        List<CpqIndexExecutor.Component> components = componentsFromTuple(tuple);
        CpqIndexExecutor.Component oversized =
            CpqIndexExecutor.oversizedComponent(components, INDEX_DIAMETER);
        if (oversized != null) {
          System.out.println(
              "Skipping '"
                  + label
                  + "' because component '"
                  + oversized.description()
                  + "' (diameter="
                  + oversized.cpq().getDiameter()
                  + ") exceeds index k="
                  + INDEX_DIAMETER);
          continue;
        }
        List<Map<String, Integer>> joinedResults = executor.execute(components);
        System.out.println("Decomposition '" + label + "' result count: " + joinedResults.size());
        DecompositionComparisonReporter.report(label, baseline, joinedResults);
        executed = true;
      }
    }

    if (!decompositionInputs.isEmpty()) {
      System.out.println(
          "External decomposition inputs are no longer evaluated because CPQ cores are required.");
    }

    if (!executed) {
      System.out.println("No decompositions available for comparison.");
    }
  }

  private CpqNativeIndex buildIndex(Path graphPath) throws IOException {
    try {
      UniqueGraph<Integer, Predicate> graph = GraphLoader.load(graphPath);
      CpqNativeIndex index =
          new CpqNativeIndex(
              graph,
              INDEX_DIAMETER,
              true,
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

  private Map<Partition, PartitionEvaluation> mapEvaluations(
      List<PartitionEvaluation> evaluations) {
    Map<Partition, PartitionEvaluation> map = new LinkedHashMap<>();
    for (PartitionEvaluation evaluation : evaluations) {
      map.put(evaluation.partition(), evaluation);
    }
    return map;
  }

  private List<CpqIndexExecutor.Component> baselineComponents(List<Edge> edges) {
    List<CpqIndexExecutor.Component> components = new ArrayList<>(edges.size());
    for (Edge edge : edges) {
      CPQ cpq = CPQ.label(edge.predicate());
      String left = normalizeVariable(edge.source());
      String right = normalizeVariable(edge.target());
      String description = edge.label() + " (" + left + "→" + right + ")";
      components.add(new CpqIndexExecutor.Component(left, right, cpq, description));
    }
    return components;
  }

  private List<CpqIndexExecutor.Component> componentsFromTuple(List<CPQExpression> tuple) {
    List<CpqIndexExecutor.Component> components = new ArrayList<>(tuple.size());
    for (CPQExpression expression : tuple) {
      components.add(CpqIndexExecutor.fromExpression(expression));
    }
    return components;
  }

  private static String normalizeVariable(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    return raw.startsWith("?") ? raw : ("?" + raw);
  }
}
