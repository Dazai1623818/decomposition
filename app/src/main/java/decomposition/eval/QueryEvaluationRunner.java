package decomposition.eval;

import decomposition.nativeindex.CpqNativeIndex;
import decomposition.nativeindex.ProgressListener;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Executes decompositions over the native index by evaluating CPQ cores. */
public final class QueryEvaluationRunner {
  private static final int MAX_RESULTS_TO_PRINT = 99_999;

  public void run(EvaluateOptions options) throws IOException {
    System.load(options.nativeLibrary().toAbsolutePath().toString());

    CpqNativeIndex index;
    try {
      UniqueGraph<Integer, Predicate> graph = GraphLoader.load(options.graphPath());
      index =
          new CpqNativeIndex(
              graph,
              options.k(),
              true,
              true,
              Math.max(1, Runtime.getRuntime().availableProcessors() - 1),
              -1,
              ProgressListener.none());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IOException("Index construction interrupted", ex);
    }
    index.sort();

    CpqIndexExecutor executor = new CpqIndexExecutor(index);

    ExampleQuery example = selectExample(options.exampleName());
    CQ cq = example.cq();
    List<CpqIndexExecutor.Component> baselineComponents = componentsFromAtoms(atomsOf(cq));
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
      List<CpqIndexExecutor.Component> components =
          componentsFromDecomposition(task.decomposition());
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
      DecompositionComparisonReporter.report(task.label(), results, joinedResults);
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
      String exampleName,
      Path graphPath,
      Path nativeLibrary,
      int k,
      List<Path> decompositionInputs) {
    public EvaluateOptions {
      Objects.requireNonNull(exampleName, "exampleName");
      Objects.requireNonNull(graphPath, "graphPath");
      Objects.requireNonNull(nativeLibrary, "nativeLibrary");
      if (k < 1) {
        throw new IllegalArgumentException("k must be at least 1");
      }
      decompositionInputs =
          decompositionInputs == null ? List.of() : List.copyOf(decompositionInputs);
    }
  }

  private record DecompositionTask(String label, QueryDecomposition decomposition) {}

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
      QueryDecomposition decomposition) {
    List<CpqIndexExecutor.Component> components = new ArrayList<>();
    Deque<QueryDecomposition.Bag> stack = new ArrayDeque<>();
    stack.push(decomposition.root());
    while (!stack.isEmpty()) {
      QueryDecomposition.Bag bag = stack.pop();
      components.add(componentFromBag(bag));
      for (QueryDecomposition.Bag child : bag.children()) {
        stack.push(child);
      }
    }
    return components;
  }

  private static CpqIndexExecutor.Component componentFromBag(QueryDecomposition.Bag bag) {
    List<AtomCQ> atoms = linearize(bag.atoms());
    List<CPQ> segments = new ArrayList<>(atoms.size());
    for (AtomCQ atom : atoms) {
      segments.add(CPQ.label(atom.getLabel()));
    }
    CPQ cpq = segments.size() == 1 ? segments.get(0) : CPQ.concat(segments);
    String source = normalize(atoms.get(0).getSource().getName());
    String target = normalize(atoms.get(atoms.size() - 1).getTarget().getName());
    return new CpqIndexExecutor.Component(source, target, cpq, cpq.toString());
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

  private static String normalize(String raw) {
    if (raw == null || raw.isBlank()) {
      return raw;
    }
    return raw.startsWith("?") ? raw : ("?" + raw);
  }
}
