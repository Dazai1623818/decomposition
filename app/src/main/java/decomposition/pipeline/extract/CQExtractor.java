package decomposition.pipeline.extract;

import decomposition.core.model.Edge;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphEdge;
import dev.roanh.gmark.util.graph.generic.UniqueGraph.GraphNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Extracts a neutral intermediate representation from a gMark CQ. */
public final class CQExtractor {

  public record ExtractionResult(
      List<Edge> edges, Set<String> freeVariables, Map<String, String> variableNodeMap) {
    public ExtractionResult {
      edges = List.copyOf(edges);
      freeVariables = Set.copyOf(freeVariables);
      variableNodeMap =
          Collections.unmodifiableMap(
              new LinkedHashMap<>(Objects.requireNonNull(variableNodeMap, "variableNodeMap")));
    }
  }

  public ExtractionResult extract(CQ cq, Set<String> explicitFreeVariables) {
    Objects.requireNonNull(cq, "cq");

    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();

    Map<String, String> variableNodeMap = extractVariableNodeMap(graph);
    List<Edge> edges = new ArrayList<>();
    long nextSyntheticId = 0L;
    for (GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
      AtomCQ atom = edge.getData();
      if (atom.getSource() == null || atom.getTarget() == null) {
        throw new IllegalArgumentException("Non-binary CQ atom encountered: " + atom);
      }
      String sourceVar = atom.getSource().getName();
      String targetVar = atom.getTarget().getName();
      edges.add(new Edge(sourceVar, targetVar, atom.getLabel(), nextSyntheticId++));
    }

    Set<String> freeVariables =
        explicitFreeVariables != null && !explicitFreeVariables.isEmpty()
            ? validateFreeVariables(explicitFreeVariables, cq)
            : deriveFreeVariables(cq);

    return new ExtractionResult(edges, freeVariables, variableNodeMap);
  }

  private Set<String> deriveFreeVariables(CQ cq) {
    return cq.getFreeVariables().stream()
        .map(VarCQ::getName)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private Set<String> validateFreeVariables(Set<String> explicit, CQ cq) {
    Set<String> extracted = deriveAllVariables(cq);
    for (String candidate : explicit) {
      if (!extracted.contains(candidate)) {
        throw new IllegalArgumentException("Unknown free variable: " + candidate);
      }
    }
    return Set.copyOf(explicit);
  }

  private Set<String> deriveAllVariables(CQ cq) {
    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
    return graph.getNodes().stream()
        .map(GraphNode::getData)
        .map(VarCQ::getName)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private Map<String, String> extractVariableNodeMap(UniqueGraph<VarCQ, AtomCQ> graph) {
    Map<String, String> mapping = new LinkedHashMap<>();
    for (GraphNode<VarCQ, AtomCQ> node : graph.getNodes()) {
      String name = node.getData().getName();
      mapping.putIfAbsent(name, name);
    }
    return mapping;
  }
}
