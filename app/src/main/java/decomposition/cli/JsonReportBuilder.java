package decomposition.cli;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import decomposition.core.DecompositionResult;
import decomposition.core.PartitionEvaluation;
import decomposition.core.diagnostics.PartitionDiagnostic;
import decomposition.core.model.Component;
import decomposition.core.model.Partition;
import decomposition.cpq.CPQExpression;
import decomposition.util.BitsetUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class JsonReportBuilder {
  private static final String VERSION = "1.0.0";
  private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

  String build(DecompositionResult result) {
    Map<String, Object> root = new LinkedHashMap<>();
    root.put("meta", meta(result));
    root.put("candidates", candidates(result));
    if (!result.recognizedCatalogue().isEmpty()) {
      root.put("component_cpq_catalog", catalogue(result.recognizedCatalogue()));
    }
    root.put(
        "final_expression",
        result.hasFinalExpression() ? result.finalExpression().cpqRule() : null);
    root.put(
        "final_expression_derivation",
        result.hasFinalExpression() ? result.finalExpression().derivation() : null);
    if (!result.globalCatalogue().isEmpty()) {
      root.put("global_cpq_catalog", catalogue(result.globalCatalogue()));
    }
    if (!result.diagnostics().isEmpty()) {
      root.put("diagnostics", diagnosticSummaries(result.diagnostics()));
    }
    if (!result.filteredPartitionList().isEmpty()) {
      root.put("partitions", partitionSummaries(result.filteredPartitionList()));
    }
    if (!result.cpqPartitions().isEmpty()) {
      root.put("valid_partitions", partitionSummaries(result.cpqPartitions()));
    }
    if (!result.partitionEvaluations().isEmpty()) {
      root.put("partition_evaluations", evaluationSummaries(result));
    }
    if (result.terminationReason() != null) {
      root.put("termination_reason", result.terminationReason());
    }
    return gson.toJson(root);
  }

  private Map<String, Object> meta(DecompositionResult result) {
    Map<String, Object> meta = new LinkedHashMap<>();
    meta.put("version", VERSION);
    meta.put("time_ms", result.elapsedMillis());
    meta.put("edge_count", result.edges().size());
    meta.put("vertex_count", result.vertexCount());
    meta.put("free_vars", sortedList(result.freeVariables()));
    return meta;
  }

  private Map<String, Object> candidates(DecompositionResult result) {
    Map<String, Object> candidates = new LinkedHashMap<>();
    candidates.put("partitions_considered", result.totalPartitions());
    candidates.put("filtered", result.filteredPartitions());
    candidates.put("sorting_metric", "L_then_M");
    candidates.put("valid", result.cpqPartitions().size());
    return candidates;
  }

  private List<Map<String, Object>> catalogue(List<CPQExpression> components) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (CPQExpression component : components) {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("edges", BitsetUtils.toIndexList(component.edges()));
      map.put("source", component.source());
      map.put("target", component.target());
      map.put("expression", component.cpqRule());
      map.put("derivation", component.derivation());
      list.add(map);
    }
    return list;
  }

  private List<Map<String, Object>> partitionSummaries(List<Partition> partitions) {
    List<Map<String, Object>> summaries = new ArrayList<>();
    for (Partition partition : partitions) {
      Map<String, Object> map = new LinkedHashMap<>();
      int L = largestComponentSize(partition);
      int M = frequencyOfLargest(partition, L);
      map.put("L", L);
      map.put("M", M);
      map.put("components", componentSummaries(partition));
      summaries.add(map);
    }
    return summaries;
  }

  private List<Map<String, Object>> evaluationSummaries(DecompositionResult result) {
    List<Map<String, Object>> summaries = new ArrayList<>();
    for (PartitionEvaluation evaluation : result.partitionEvaluations()) {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("index", evaluation.partitionIndex());
      map.put("component_expressions", evaluation.componentExpressionCounts());
      map.put("components", componentSummaries(evaluation.partition()));
      if (!evaluation.decompositionTuples().isEmpty()) {
        map.put("tuples", tupleSummaries(evaluation.decompositionTuples()));
      }
      summaries.add(map);
    }
    return summaries;
  }

  private List<Map<String, Object>> diagnosticSummaries(List<PartitionDiagnostic> diagnostics) {
    List<Map<String, Object>> summaries = new ArrayList<>();
    for (PartitionDiagnostic diagnostic : diagnostics) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("reason", diagnostic.reason().name().toLowerCase(Locale.ROOT));
      if (diagnostic.partitionIndex() != null) {
        entry.put("partition_index", diagnostic.partitionIndex());
      }
      if (diagnostic.componentIndex() != null) {
        entry.put("component_index", diagnostic.componentIndex());
      }
      entry.put("message", diagnosticMessage(diagnostic));
      if (!diagnostic.attributes().isEmpty()) {
        entry.put("attributes", diagnostic.attributes());
      }
      summaries.add(entry);
    }
    return summaries;
  }

  private String diagnosticMessage(PartitionDiagnostic diagnostic) {
    return switch (diagnostic.reason()) {
      case FREE_VARIABLE_ABSENT ->
          String.format(
              Locale.ROOT,
              "%s rejected: free var %s absent from partition",
              partitionLabel(diagnostic.partitionIndex()),
              diagnostic.attributes().get(PartitionDiagnostic.ATTR_FREE_VARIABLE));
      case EXCESS_JOIN_NODES -> {
        int limit =
            ((Number)
                    diagnostic
                        .attributes()
                        .getOrDefault(PartitionDiagnostic.ATTR_MAX_JOIN_NODES, 0))
                .intValue();
        int count =
            ((Number)
                    diagnostic
                        .attributes()
                        .getOrDefault(PartitionDiagnostic.ATTR_JOIN_NODE_COUNT, 0))
                .intValue();
        yield String.format(
            Locale.ROOT,
            "%s component#%d rejected: component had >%d join nodes (found %d)",
            partitionLabel(diagnostic.partitionIndex()),
            diagnostic.componentIndex(),
            limit,
            count);
      }
      case COMPONENT_EXPRESSIONS_MISSING ->
          String.format(
              Locale.ROOT,
              "%s rejected: no CPQ expressions for bits %s",
              componentLabel(diagnostic),
              diagnostic.attributes().get(PartitionDiagnostic.ATTR_SIGNATURE));
      case COMPONENT_ENDPOINTS_INVALID ->
          String.format(
              Locale.ROOT,
              "%s rejected: endpoints not on join nodes for bits %s",
              componentLabel(diagnostic),
              diagnostic.attributes().get(PartitionDiagnostic.ATTR_SIGNATURE));
      case COMPONENT_DIAGNOSTICS_UNAVAILABLE ->
          partitionLabel(diagnostic.partitionIndex())
              + " rejected but component diagnostics were unavailable.";
      case VERIFICATION_FAILED ->
          partitionLabel(diagnostic.partitionIndex())
              + " verification failed: "
              + diagnostic.attributes().getOrDefault("message", "verification failure");
    };
  }

  private String partitionLabel(Integer partitionIndex) {
    return partitionIndex != null ? "Partition#" + partitionIndex : "Partition";
  }

  private String componentLabel(PartitionDiagnostic diagnostic) {
    String base = partitionLabel(diagnostic.partitionIndex()) + " component";
    return diagnostic.componentIndex() != null ? base + "#" + diagnostic.componentIndex() : base;
  }

  private List<List<Map<String, Object>>> tupleSummaries(List<List<CPQExpression>> tuples) {
    List<List<Map<String, Object>>> list = new ArrayList<>();
    for (List<CPQExpression> tuple : tuples) {
      List<Map<String, Object>> tupleEntries = new ArrayList<>();
      for (CPQExpression component : tuple) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("edges", BitsetUtils.toIndexList(component.edges()));
        map.put("source", component.source());
        map.put("target", component.target());
        map.put("rule", component.cpqRule());
        tupleEntries.add(map);
      }
      list.add(tupleEntries);
    }
    return list;
  }

  private List<Map<String, Object>> componentSummaries(Partition partition) {
    List<Map<String, Object>> list = new ArrayList<>();
    for (Component component : partition.components()) {
      Map<String, Object> comp = new LinkedHashMap<>();
      comp.put(
          "edge_bits",
          String.join(
              ",",
              BitsetUtils.toIndexList(component.edgeBits()).stream()
                  .map(String::valueOf)
                  .toList()));
      comp.put("vertices", sortedList(component.vertices()));
      list.add(comp);
    }
    return list;
  }

  private int largestComponentSize(Partition partition) {
    int max = 0;
    for (Component component : partition.components()) {
      max = Math.max(max, component.edgeCount());
    }
    return max;
  }

  private int frequencyOfLargest(Partition partition, int largest) {
    int count = 0;
    for (Component component : partition.components()) {
      if (component.edgeCount() == largest) {
        count++;
      }
    }
    return count;
  }

  private List<String> sortedList(Set<String> values) {
    return values.stream().sorted().toList();
  }
}
