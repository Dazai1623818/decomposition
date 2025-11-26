package decomposition.eval;

import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.nativeindex.CpqNativeIndex;
import decomposition.util.Timing;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
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

public final class EvaluationPipeline {
  private static final int INDEX_DIAMETER = 3;
  private static final Path NAUTY_LIBRARY = Path.of("lib", "libnauty.so");
  private final Path graphPath;

  public EvaluationPipeline(Path graphPath) {
    this.graphPath = Objects.requireNonNull(graphPath, "graphPath");
  }

  public void evaluate(CQ query, DecompositionResult result) throws IOException {
    evaluate(query, result, null);
  }

  public void evaluate(CQ query, DecompositionResult result, CpqNativeIndex prebuiltIndex)
      throws IOException {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(result, "result");

    Set<String> freeVariables = normalizeVariables(result.freeVariables());
    CpqNativeIndex index = prebuiltIndex != null ? prebuiltIndex : buildIndexOnly();
    CpqIndexExecutor executor = new CpqIndexExecutor(index);

    System.out.println("Executing query: " + query.toFormalSyntax());

    // 1. Baseline: Single Edge Decomposition
    Timing baselineTiming = Timing.start();
    List<CpqIndexExecutor.Component> baselineComponents = baselineComponents(result.edges());

    // Check if baseline components fit in index
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

    List<Map<String, Integer>> baseline =
        executeWithTiming("Baseline (single-edge)", executor, baselineComponents, baselineTiming);
    System.out.println("Baseline (single-edge) result count: " + baseline.size());
    Set<Map<String, Integer>> projectedBaseline =
        DecompositionComparisonReporter.project(baseline, freeVariables);
    System.out.println(
        "Baseline projected (free vars) result count: "
            + projectedBaseline.size()
            + " over "
            + freeVariables);
    for (int i = 0; i < Math.min(10, baseline.size()); i++) {
      System.out.println(
          "  [baseline] " + DecompositionComparisonReporter.formatAssignment(baseline.get(i)));
    }

    // 2. Decompositions
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
        System.out.println("Tuple '" + label + "': " + formatTuple(tuple));

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

        List<Map<String, Integer>> joinedResults = executeWithTiming(label, executor, components);
        System.out.println("Decomposition '" + label + "' result count: " + joinedResults.size());
        printSampleAssignments("  [" + label + "]", joinedResults, 10);
        DecompositionComparisonReporter.report(label, baseline, joinedResults, freeVariables);
        executed = true;
      }
    }

    if (!executed) {
      System.out.println("No decompositions available for comparison.");
    }
  }

  public CpqNativeIndex buildIndexOnly() throws IOException {
    System.load(NAUTY_LIBRARY.toAbsolutePath().toString());
    long startNanos = System.nanoTime();
    CpqNativeIndex index = buildIndex(graphPath);
    long durationMs = Math.round((System.nanoTime() - startNanos) / 1_000_000.0);
    System.out.println("Index construction took " + durationMs + " ms");
    return index;
  }

  private CpqNativeIndex buildIndex(Path graphPath) throws IOException {
    Path indexPath = indexPathFor(graphPath);
    if (Files.exists(indexPath)) {
      System.out.println("Loading saved index from " + indexPath.toAbsolutePath());
      CpqNativeIndex cached = CpqNativeIndex.load(indexPath);
      cached.sort();
      return cached;
    }
    try {
      CpqNativeIndex index =
          new CpqNativeIndex(
              CpqNativeIndex.readGraph(graphPath),
              INDEX_DIAMETER,
              Math.max(1, Runtime.getRuntime().availableProcessors() - 1));
      index.sort();
      try {
        index.save(indexPath);
        System.out.println("Saved index to " + indexPath.toAbsolutePath());
      } catch (IOException ex) {
        System.out.println("Warning: failed to save index to " + indexPath + ": " + ex.getMessage());
      }
      return index;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
  }

  private Path indexPathFor(Path graphPath) {
    Path directory = graphPath.getParent() != null ? graphPath.getParent() : Path.of("");
    String fileName = graphPath.getFileName().toString();
    int dot = fileName.lastIndexOf('.');
    String stem = dot >= 0 ? fileName.substring(0, dot) : fileName;
    String indexName = stem + ".k" + INDEX_DIAMETER + ".idx";
    return directory.resolve(indexName);
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
      if (Objects.equals(edge.source(), edge.target())) {
        // Self-loop: intersect with identity so both endpoints stay anchored at the
        // same vertex
        cpq = CPQ.intersect(List.of(cpq, CPQ.id()));
      }
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

  private static void printSampleAssignments(
      String prefix, List<Map<String, Integer>> results, int limit) {
    for (int i = 0; i < Math.min(limit, results.size()); i++) {
      System.out.println(
          prefix + " " + DecompositionComparisonReporter.formatAssignment(results.get(i)));
    }
    if (results.size() > limit) {
      System.out.println(prefix + " ... truncated ...");
    }
  }

  private static String formatTuple(List<CPQExpression> tuple) {
    List<String> parts = new ArrayList<>(tuple.size());
    for (CPQExpression component : tuple) {
      String left = normalizeVariable(component.getVarForNode(component.source()));
      String right = normalizeVariable(component.getVarForNode(component.target()));
      parts.add(component.cpq().toString() + " (" + left + "→" + right + ")");
    }
    return String.join(" | ", parts);
  }

  private static List<Map<String, Integer>> executeWithTiming(
      String label, CpqIndexExecutor executor, List<CpqIndexExecutor.Component> components) {
    return executeWithTiming(label, executor, components, null);
  }

  private static List<Map<String, Integer>> executeWithTiming(
      String label,
      CpqIndexExecutor executor,
      List<CpqIndexExecutor.Component> components,
      Timing timing) {
    Timing effectiveTiming = timing != null ? timing : Timing.start();
    List<Map<String, Integer>> results = executor.execute(components);
    System.out.println(label + " execution took " + effectiveTiming.elapsedMillis() + " ms");
    return results;
  }

  private static Set<String> normalizeVariables(Set<String> variables) {
    Set<String> normalized = new LinkedHashSet<>();
    for (String variable : variables) {
      String candidate = normalizeVariable(variable);
      if (candidate != null && !candidate.isBlank()) {
        normalized.add(candidate);
      }
    }
    return normalized;
  }
}
