package decomposition.eval;

import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Executes conjunctive edge queries using Leapfrog Triejoin. */
public final class LeapfrogEdgeJoiner {
  private final LabelIndex labelIndex;

  public LeapfrogEdgeJoiner(LabelIndex labelIndex) {
    this.labelIndex = Objects.requireNonNull(labelIndex, "labelIndex");
  }

  public static LeapfrogEdgeJoiner fromIndex(dev.roanh.cpqindex.Index index) {
    return new LeapfrogEdgeJoiner(LabelIndex.from(index));
  }

  public List<Map<String, Integer>> execute(CQ cq) {
    Objects.requireNonNull(cq, "cq");
    List<AtomCQ> atoms = atomsOf(cq);
    if (atoms.isEmpty()) {
      return List.of();
    }
    return executeAtoms(atoms);
  }

  public List<Map<String, Integer>> executeAtoms(List<AtomCQ> atoms) {
    Objects.requireNonNull(atoms, "atoms");
    if (atoms.isEmpty()) {
      return List.of();
    }
    List<RelationBinding> relations = new ArrayList<>(atoms.size());
    for (AtomCQ atom : atoms) {
      LabelIndex.LabelProjection projection = labelIndex.projectionFor(atom.getLabel().getAlias());
      relations.add(new RelationBinding(projection, atom));
    }
    return runLeapfrog(relations);
  }

  private List<Map<String, Integer>> runLeapfrog(List<RelationBinding> relations) {
    Map<String, List<RelationBinding>> relationsPerVariable = new HashMap<>();
    for (RelationBinding relation : relations) {
      relationsPerVariable
          .computeIfAbsent(relation.sourceVar, ignored -> new ArrayList<>())
          .add(relation);
      relationsPerVariable
          .computeIfAbsent(relation.targetVar, ignored -> new ArrayList<>())
          .add(relation);
    }

    List<String> variableOrder =
        relationsPerVariable.keySet().stream()
            .sorted(
                Comparator.comparingInt((String var) -> relationsPerVariable.get(var).size())
                    .thenComparing(Comparator.naturalOrder()))
            .collect(Collectors.toCollection(ArrayList::new));

    List<Map<String, Integer>> results = new ArrayList<>();
    search(variableOrder, 0, relationsPerVariable, new LinkedHashMap<>(), results);
    return results;
  }

  private void search(
      List<String> variableOrder,
      int depth,
      Map<String, List<RelationBinding>> relationsPerVariable,
      Map<String, Integer> assignment,
      List<Map<String, Integer>> results) {
    if (depth == variableOrder.size()) {
      results.add(new LinkedHashMap<>(assignment));
      return;
    }
    String variable = variableOrder.get(depth);
    List<RelationBinding> bindings = relationsPerVariable.getOrDefault(variable, List.of());
    if (bindings.isEmpty()) {
      // Variable unconstrained; no tuples can satisfy the query.
      return;
    }

    List<LeapfrogIterator.IntCursor> cursors = new ArrayList<>(bindings.size());
    for (RelationBinding binding : bindings) {
      int[] domain = binding.domainFor(variable, assignment);
      if (domain.length == 0) {
        return;
      }
      cursors.add(new LeapfrogIterator.IntCursor(domain));
    }

    LeapfrogIterator join = new LeapfrogIterator(cursors);
    join.init();
    while (!join.atEnd()) {
      assignment.put(variable, join.key());
      search(variableOrder, depth + 1, relationsPerVariable, assignment, results);
      assignment.remove(variable);
      join.next();
    }
  }

  private static final class RelationBinding {
    private final LabelIndex.LabelProjection projection;
    private final String sourceVar;
    private final String targetVar;
    private final String label;

    RelationBinding(LabelIndex.LabelProjection projection, AtomCQ atom) {
      this.projection = projection;
      this.sourceVar = varName(atom.getSource());
      this.targetVar = varName(atom.getTarget());
      this.label = atom.getLabel().getAlias();
    }

    int[] domainFor(String variable, Map<String, Integer> assignment) {
      if (variable.equals(sourceVar)) {
        if (assignment.containsKey(targetVar)) {
          int target = assignment.get(targetVar);
          return projection.sourcesForTarget(target);
        }
        return projection.allSources();
      }
      if (variable.equals(targetVar)) {
        if (assignment.containsKey(sourceVar)) {
          int source = assignment.get(sourceVar);
          return projection.targetsForSource(source);
        }
        return projection.allTargets();
      }
      throw new IllegalArgumentException("Variable " + variable + " not part of relation " + label);
    }
  }

  public Set<String> labels() {
    return labelIndex.labels();
  }

  private static List<AtomCQ> atomsOf(CQ cq) {
    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
    if (graph.getEdgeCount() == 0) {
      return List.of();
    }
    List<AtomCQ> atoms = new ArrayList<>(graph.getEdgeCount());
    for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
      atoms.add(edge.getData());
    }
    return atoms;
  }

  private static String varName(VarCQ variable) {
    return "?" + variable.getName();
  }
}
