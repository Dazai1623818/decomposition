package decomposition.eval;

import decomposition.examples.Example;
import dev.roanh.cpqindex.Index;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Executes decompositions over the native index by evaluating CPQ cores. */
public final class QueryEvaluationRunner {
  private static final int MAX_RESULTS_TO_PRINT = 99_999;
  private static final Path NAUTY_LIBRARY = Path.of("lib", "libnauty.so");
  private final IndexManager indexManager = new IndexManager();

  public void run(EvaluateOptions options) throws IOException {
    System.load(NAUTY_LIBRARY.toAbsolutePath().toString());

    Index index = indexManager.loadOrBuild(options.graphPath(), options.k());

    CpqIndexExecutor executor = new CpqIndexExecutor(index);

    ExampleQuery example = selectExample(options.exampleName());
    CQ cq = example.cq();
    Set<String> freeVariables = freeVariablesOf(cq);
    List<CpqIndexExecutor.Component> baselineComponents = componentsFromAtomsPublic(cq);
    CpqIndexExecutor.Component oversizedBaseline =
        CpqIndexExecutor.oversizedComponent(baselineComponents, options.k());
    if (oversizedBaseline != null) {
      System.out.println(
          "Cannot evaluate baseline: component '"
              + oversizedBaseline.description()
              + "' (diameter="
              + oversizedBaseline.cpq().getDiameter()
              + ") exceeds index k="
              + options.k());
      return;
    }
    List<Map<String, Integer>> results = executor.execute(baselineComponents);

    System.out.println(
        "Executing example '" + options.exampleName() + "' expressed in gMark notation.");
    System.out.println("gMark CQ: " + cq.toFormalSyntax());
    System.out.println("Result count: " + results.size());
    Set<Map<String, Integer>> projectedBaseline =
        DecompositionComparisonReporter.project(results, freeVariables);
    System.out.println(
        "Projected (free vars) result count: "
            + projectedBaseline.size()
            + " over "
            + freeVariables);
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

    for (DecompositionTask task : tasks) {
      Set<String> requiredVariables = new LinkedHashSet<>(freeVariables);
      requiredVariables.addAll(boundaryVariables(task.decomposition()));
      List<CpqIndexExecutor.Component> components =
          componentsFromDecomposition(task.decomposition(), requiredVariables);
      CpqIndexExecutor.Component oversized =
          CpqIndexExecutor.oversizedComponent(components, options.k());
      if (oversized != null) {
        System.out.println(
            "Skipping '"
                + task.label()
                + "' because component '"
                + oversized.description()
                + "' (diameter="
                + oversized.cpq().getDiameter()
                + ") exceeds index k="
                + options.k());
        continue;
      }
      List<Map<String, Integer>> joinedResults = executor.execute(components);
      System.out.println(
          "Decomposition '" + task.label() + "' result count: " + joinedResults.size());
      DecompositionComparisonReporter.report(task.label(), results, joinedResults, freeVariables);
    }
  }

  private ExampleQuery selectExample(String exampleName) {
    if ("example1".equalsIgnoreCase(exampleName)) {
      return ExampleQuery.of(Example.example1());
    }
    throw new IllegalArgumentException(
        "Unknown example '" + exampleName + "'. Available examples: example1");
  }

  /** Options forwarded from the CLI for evaluate command runs. */
  public record EvaluateOptions(
      String exampleName, Path graphPath, int k, List<Path> decompositionInputs) {
    public EvaluateOptions {
      Objects.requireNonNull(exampleName, "exampleName");
      Objects.requireNonNull(graphPath, "graphPath");
      if (k < 1) {
        throw new IllegalArgumentException("k must be at least 1");
      }
      decompositionInputs =
          decompositionInputs == null ? List.of() : List.copyOf(decompositionInputs);
    }
  }

  private record DecompositionTask(String label, QueryDecomposition decomposition) {}

  public List<CpqIndexExecutor.Component> componentsFromAtomsPublic(CQ cq) {
    return componentsFromAtoms(atomsOf(cq));
  }

  private static List<AtomCQ> atomsOf(CQ cq) {
    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
    List<AtomCQ> atoms = new ArrayList<>(graph.getEdgeCount());
    for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
      atoms.add(edge.getData());
    }
    return atoms;
  }

  private static List<CpqIndexExecutor.Component> componentsFromAtoms(List<AtomCQ> atoms) {
    List<CpqIndexExecutor.Component> components = new ArrayList<>(atoms.size());
    for (AtomCQ atom : atoms) {
      components.add(componentFromAtom(atom));
    }
    return components;
  }

  private static CpqIndexExecutor.Component componentFromAtom(AtomCQ atom) {
    CPQ cpq = CPQ.label(atom.getLabel());
    String source = normalize(atom.getSource().getName());
    String target = normalize(atom.getTarget().getName());
    String description = atom.getLabel().getAlias() + " (" + source + "→" + target + ")";
    return new CpqIndexExecutor.Component(source, target, cpq, description);
  }

  private static List<CpqIndexExecutor.Component> componentsFromDecomposition(
      QueryDecomposition decomposition, Set<String> requiredVariables) {
    List<CpqIndexExecutor.Component> components = new ArrayList<>();
    Deque<QueryDecomposition.Bag> stack = new ArrayDeque<>();
    stack.push(decomposition.root());
    while (!stack.isEmpty()) {
      QueryDecomposition.Bag bag = stack.pop();
      components.addAll(componentsFromBag(bag, requiredVariables));
      for (QueryDecomposition.Bag child : bag.children()) {
        stack.push(child);
      }
    }
    return components;
  }

  private static List<CpqIndexExecutor.Component> componentsFromBag(
      QueryDecomposition.Bag bag, Set<String> requiredVariables) {
    List<AtomCQ> atoms = linearize(bag.atoms());
    List<CpqIndexExecutor.Component> bagComponents = new ArrayList<>();
    List<CPQ> segment = new ArrayList<>();
    String currentSource = normalize(atoms.get(0).getSource().getName());
    for (int i = 0; i < atoms.size(); i++) {
      AtomCQ atom = atoms.get(i);
      segment.add(CPQ.label(atom.getLabel()));
      String targetVar = normalize(atom.getTarget().getName());
      boolean isLast = i == atoms.size() - 1;
      if (requiredVariables.contains(targetVar) || isLast) {
        CPQ cpq = segment.size() == 1 ? segment.get(0) : CPQ.concat(segment);
        bagComponents.add(
            new CpqIndexExecutor.Component(currentSource, targetVar, cpq, cpq.toString()));
        segment.clear();
        currentSource = targetVar;
      }
    }
    return bagComponents;
  }

  private static Set<String> boundaryVariables(QueryDecomposition decomposition) {
    Map<String, Integer> counts = new HashMap<>();
    Deque<QueryDecomposition.Bag> stack = new ArrayDeque<>();
    stack.push(decomposition.root());
    while (!stack.isEmpty()) {
      QueryDecomposition.Bag bag = stack.pop();
      Set<String> bagVars = new HashSet<>();
      for (AtomCQ atom : bag.atoms()) {
        String source = normalize(atom.getSource().getName());
        String target = normalize(atom.getTarget().getName());
        if (source != null && !source.isBlank()) {
          bagVars.add(source);
        }
        if (target != null && !target.isBlank()) {
          bagVars.add(target);
        }
      }
      for (String variable : bagVars) {
        counts.merge(variable, 1, Integer::sum);
      }
      for (QueryDecomposition.Bag child : bag.children()) {
        stack.push(child);
      }
    }
    Set<String> boundary = new LinkedHashSet<>();
    for (Map.Entry<String, Integer> entry : counts.entrySet()) {
      if (entry.getValue() > 1) {
        boundary.add(entry.getKey());
      }
    }
    return boundary;
  }

  private static List<AtomCQ> linearize(List<AtomCQ> atoms) {
    if (atoms.isEmpty()) {
      throw new IllegalArgumentException("Decomposition bag must contain at least one atom.");
    }
    if (atoms.size() == 1) {
      return List.of(atoms.get(0));
    }
    Map<String, AtomCQ> bySource = new HashMap<>();
    Map<String, Integer> indegree = new HashMap<>();
    for (AtomCQ atom : atoms) {
      String src = atom.getSource().getName();
      if (bySource.put(src, atom) != null) {
        throw new IllegalArgumentException("Component contains branching at " + src);
      }
      indegree.merge(atom.getTarget().getName(), 1, Integer::sum);
    }
    String start = null;
    for (AtomCQ atom : atoms) {
      String candidate = atom.getSource().getName();
      if (!indegree.containsKey(candidate)) {
        start = candidate;
        break;
      }
    }
    if (start == null) {
      start = atoms.get(0).getSource().getName();
    }
    List<AtomCQ> ordered = new ArrayList<>(atoms.size());
    Set<AtomCQ> seen = new HashSet<>();
    while (ordered.size() < atoms.size()) {
      AtomCQ next = bySource.remove(start);
      if (next == null || seen.contains(next)) {
        throw new IllegalArgumentException("Component edges do not form a simple path.");
      }
      ordered.add(next);
      seen.add(next);
      start = next.getTarget().getName();
    }
    if (!bySource.isEmpty()) {
      throw new IllegalArgumentException("Component edges do not form a simple path.");
    }
    return ordered;
  }

  private static Set<String> freeVariablesOf(CQ cq) {
    Set<String> variables = new LinkedHashSet<>();
    for (VarCQ variable : cq.getFreeVariables()) {
      String normalized = normalize(variable.getName());
      if (normalized != null && !normalized.isBlank()) {
        variables.add(normalized);
      }
    }
    return variables;
  }

  private static String normalize(String raw) {
    if (raw == null || raw.isBlank()) {
      return raw;
    }
    return raw.startsWith("?") ? raw : ("?" + raw);
  }
}
