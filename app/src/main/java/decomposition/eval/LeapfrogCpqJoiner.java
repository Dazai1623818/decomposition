package decomposition.eval;

import decomposition.eval.CpqIndex.RelationProjection;
import decomposition.nativeindex.CpqNativeIndex;
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

/** Executes conjunctive queries using Leapfrog Triejoin over the CPQ-native index. */
public final class LeapfrogCpqJoiner {
  private final CpqIndex index;

  public LeapfrogCpqJoiner(CpqIndex index) {
    this.index = Objects.requireNonNull(index, "index");
  }

  public static LeapfrogCpqJoiner fromIndex(CpqNativeIndex cpqIndex) {
    return new LeapfrogCpqJoiner(CpqIndex.from(cpqIndex));
  }

  /** Baseline execution that only uses single-edge relations (k = 1 behaviour). */
  public List<Map<String, Integer>> executeBaseline(CQ cq) {
    Objects.requireNonNull(cq, "cq");
    List<AtomCQ> atoms = atomsOf(cq);
    if (atoms.isEmpty()) {
      return List.of();
    }
    return executeBaseline(atoms);
  }

  /** Baseline execution over a fixed list of atoms using only length-1 label sequences. */
  public List<Map<String, Integer>> executeBaseline(List<AtomCQ> atoms) {
    Objects.requireNonNull(atoms, "atoms");
    if (atoms.isEmpty()) {
      return List.of();
    }
    System.out.println("Baseline join CPQs (single-edge):");
    for (AtomCQ atom : atoms) {
      String cpq = atom.getLabel().getAlias();
      String src = varName(atom.getSource());
      String tgt = varName(atom.getTarget());
      System.out.println("  " + cpq + " (" + src + " → " + tgt + ")");
    }
    List<RelationBinding> relations = new ArrayList<>(atoms.size());
    for (AtomCQ atom : atoms) {
      List<String> signature = List.of(atom.getLabel().getAlias());
      RelationProjection projection = index.projectionFor(signature);
      relations.add(RelationBinding.from(projection, atom));
    }
    return runLeapfrog(relations);
  }

  /**
   * Optimised execution that groups maximal contiguous paths for which a pre-computed relation is
   * available in the CPQ index (up to {@link CpqIndex#maxSignatureLength()}).
   *
   * <p>Each such path is treated as a single binary relation between the path endpoints.
   */
  public List<Map<String, Integer>> executeOptimized(List<AtomCQ> atoms) {
    Objects.requireNonNull(atoms, "atoms");
    if (atoms.isEmpty()) {
      return List.of();
    }
    List<PathFragment> fragments = groupPathFragments(atoms);
    System.out.println("Optimized join CPQs:");
    for (PathFragment fragment : fragments) {
      String cpq = formatSignature(fragment.signature());
      AtomCQ first = fragment.atoms().get(0);
      AtomCQ last = fragment.atoms().get(fragment.atoms().size() - 1);
      String src = varName(first.getSource());
      String tgt = varName(last.getTarget());
      System.out.println("  " + cpq + " (" + src + " → " + tgt + ")");
    }
    List<RelationBinding> relations = new ArrayList<>(fragments.size());
    for (PathFragment fragment : fragments) {
      RelationProjection projection = index.projectionFor(fragment.signature());
      relations.add(RelationBinding.from(projection, fragment));
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

  public Set<String> singleEdgeLabels() {
    return index.singleEdgeLabels();
  }

  private static String formatSignature(List<String> signature) {
    if (signature.isEmpty()) {
      return "";
    }
    if (signature.size() == 1) {
      return signature.get(0);
    }
    return String.join(" ◦ ", signature);
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

  private List<PathFragment> groupPathFragments(List<AtomCQ> atoms) {
    List<PathFragment> fragments = new ArrayList<>();
    int maxLength = index.maxSignatureLength();
    if (maxLength <= 1) {
      // Index only contains single-edge information; fall back to baseline grouping.
      for (AtomCQ atom : atoms) {
        fragments.add(PathFragment.singleton(atom));
      }
      return fragments;
    }

    int i = 0;
    while (i < atoms.size()) {
      AtomCQ start = atoms.get(i);
      List<AtomCQ> pathAtoms = new ArrayList<>();
      pathAtoms.add(start);
      List<String> labels = new ArrayList<>();
      labels.add(start.getLabel().getAlias());

      String currentTarget = varName(start.getTarget());
      List<String> bestSignature = null;
      int bestEndExclusive = i + 1;

      // Try to extend the path greedily up to maxLength.
      int j = i + 1;
      while (j < atoms.size() && pathAtoms.size() < maxLength) {
        AtomCQ next = atoms.get(j);
        if (!varName(next.getSource()).equals(currentTarget)) {
          break;
        }
        pathAtoms.add(next);
        labels.add(next.getLabel().getAlias());
        currentTarget = varName(next.getTarget());

        if (index.hasSignature(labels)) {
          bestSignature = List.copyOf(labels);
          bestEndExclusive = j + 1;
        }
        j++;
      }

      if (bestSignature != null && bestEndExclusive - i > 1) {
        List<AtomCQ> grouped = atoms.subList(i, bestEndExclusive);
        fragments.add(PathFragment.of(grouped, bestSignature));
        i = bestEndExclusive;
      } else {
        fragments.add(PathFragment.singleton(start));
        i++;
      }
    }

    return fragments;
  }

  private static String varName(VarCQ variable) {
    return "?" + variable.getName();
  }

  private static final class RelationBinding {
    private final RelationProjection projection;
    private final String sourceVar;
    private final String targetVar;
    private final String description;

    private RelationBinding(
        RelationProjection projection, String sourceVar, String targetVar, String description) {
      this.projection = projection;
      this.sourceVar = sourceVar;
      this.targetVar = targetVar;
      this.description = description;
    }

    static RelationBinding from(RelationProjection projection, AtomCQ atom) {
      String sourceVar = varName(atom.getSource());
      String targetVar = varName(atom.getTarget());
      String desc = atom.getLabel().getAlias();
      return new RelationBinding(projection, sourceVar, targetVar, desc);
    }

    static RelationBinding from(RelationProjection projection, PathFragment fragment) {
      String sourceVar = varName(fragment.atoms().get(0).getSource());
      String targetVar = varName(fragment.atoms().get(fragment.atoms().size() - 1).getTarget());
      String desc = fragment.signature().toString();
      return new RelationBinding(projection, sourceVar, targetVar, desc);
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
      throw new IllegalArgumentException(
          "Variable " + variable + " not part of relation " + description);
    }
  }

  private static final class PathFragment {
    private final List<AtomCQ> atoms;
    private final List<String> signature;

    private PathFragment(List<AtomCQ> atoms, List<String> signature) {
      this.atoms = List.copyOf(atoms);
      this.signature = List.copyOf(signature);
    }

    static PathFragment singleton(AtomCQ atom) {
      return new PathFragment(List.of(atom), List.of(atom.getLabel().getAlias()));
    }

    static PathFragment of(List<AtomCQ> atoms, List<String> signature) {
      return new PathFragment(atoms, signature);
    }

    List<AtomCQ> atoms() {
      return atoms;
    }

    List<String> signature() {
      return signature;
    }
  }
}
