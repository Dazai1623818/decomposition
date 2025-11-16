package decomposition.eval;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Loads decompositions that were exported by the {@code decomposition} project. Each JSON file
 * contains a flat list of edges annotated with a component index.
 */
final class PartitionDecompositionLoader {
  private final CQ cq;
  private final Map<String, List<AtomCQ>> atomsBySignature;

  private PartitionDecompositionLoader(CQ cq) {
    this.cq = Objects.requireNonNull(cq, "cq");
    this.atomsBySignature = indexAtoms(cq);
  }

  static PartitionDecompositionLoader forQuery(CQ cq) {
    return new PartitionDecompositionLoader(cq);
  }

  List<NamedDecomposition> load(Path path) throws IOException {
    Objects.requireNonNull(path, "path");
    if (Files.isDirectory(path)) {
      try (Stream<Path> stream = Files.list(path)) {
        return stream
            .filter(Files::isRegularFile)
            .filter(p -> p.getFileName().toString().endsWith(".json"))
            .sorted(Comparator.comparing(p -> p.getFileName().toString()))
            .map(p -> loadSingle(p, relativeName(path, p)))
            .toList();
      }
    }
    if (Files.isRegularFile(path)) {
      return List.of(loadSingle(path, path.getFileName().toString()));
    }
    throw new IllegalArgumentException("Path does not exist: " + path);
  }

  private NamedDecomposition loadSingle(Path file, String name) {
    try {
      JsonElement parsed = JsonParser.parseString(Files.readString(file));
      if (!parsed.isJsonArray()) {
        throw new IllegalArgumentException("Expected JSON array in " + file);
      }
      JsonArray array = parsed.getAsJsonArray();
      Map<Integer, List<AtomCQ>> atomsPerComponent = new LinkedHashMap<>();
      Map<String, Deque<AtomCQ>> lookup = freshLookup();
      for (JsonElement element : array) {
        if (!element.isJsonObject()) {
          throw new IllegalArgumentException("Invalid entry in " + file + ": " + element);
        }
        JsonObject obj = element.getAsJsonObject();
        int component = readComponent(obj);
        String source = readRequiredString(obj, "source", file);
        String target = readRequiredString(obj, "target", file);
        String label = readRequiredString(obj, "label", file);
        AtomCQ atom = resolveAtom(source, label, target, lookup, file);
        atomsPerComponent.computeIfAbsent(component, ignored -> new ArrayList<>()).add(atom);
      }
      if (atomsPerComponent.isEmpty()) {
        throw new IllegalArgumentException("No edges found in " + file);
      }
      QueryDecomposition decomposition = buildDecomposition(atomsPerComponent);
      return new NamedDecomposition(name, decomposition);
    } catch (IOException ex) {
      throw new IllegalArgumentException("Failed to read decomposition " + file, ex);
    }
  }

  private Map<String, Deque<AtomCQ>> freshLookup() {
    Map<String, Deque<AtomCQ>> lookup = new HashMap<>();
    for (Map.Entry<String, List<AtomCQ>> entry : atomsBySignature.entrySet()) {
      lookup.put(entry.getKey(), new ArrayDeque<>(entry.getValue()));
    }
    return lookup;
  }

  private AtomCQ resolveAtom(
      String source, String label, String target, Map<String, Deque<AtomCQ>> lookup, Path file) {
    String signature = signature(source, label, target);
    Deque<AtomCQ> candidates = lookup.get(signature);
    if (candidates == null || candidates.isEmpty()) {
      throw new IllegalArgumentException(
          "Edge " + signature + " not present in CQ for file " + file);
    }
    return candidates.removeFirst();
  }

  private QueryDecomposition buildDecomposition(Map<Integer, List<AtomCQ>> atomsPerComponent) {
    List<Map.Entry<Integer, List<AtomCQ>>> ordered =
        atomsPerComponent.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
    Map.Entry<Integer, List<AtomCQ>> rootEntry = ordered.get(0);
    List<QueryDecomposition.Bag> children = new ArrayList<>();
    for (int i = 1; i < ordered.size(); i++) {
      children.add(QueryDecomposition.bag(ordered.get(i).getValue()));
    }
    QueryDecomposition.Bag root = QueryDecomposition.bag(rootEntry.getValue(), children);
    return QueryDecomposition.of(root);
  }

  private static Map<String, List<AtomCQ>> indexAtoms(CQ cq) {
    UniqueGraph<VarCQ, AtomCQ> graph = cq.toQueryGraph().toUniqueGraph();
    Map<String, List<AtomCQ>> index = new HashMap<>();
    for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
      AtomCQ atom = edge.getData();
      index.computeIfAbsent(signature(atom), ignored -> new ArrayList<>()).add(atom);
    }
    return index;
  }

  private static String readRequiredString(JsonObject obj, String field, Path file) {
    if (!obj.has(field) || obj.get(field).isJsonNull()) {
      throw new IllegalArgumentException("Missing '" + field + "' in " + file);
    }
    return obj.get(field).getAsString();
  }

  private static int readComponent(JsonObject obj) {
    if (!obj.has("component") || obj.get("component").isJsonNull()) {
      return 0;
    }
    return obj.get("component").getAsInt();
  }

  private static String signature(AtomCQ atom) {
    return signature(
        atom.getSource().getName(), atom.getLabel().getAlias(), atom.getTarget().getName());
  }

  private static String signature(String source, String label, String target) {
    return source + "|" + label + "|" + target;
  }

  private static String relativeName(Path baseDir, Path file) {
    Path relative = baseDir.relativize(file);
    return relative.toString().isEmpty() ? file.getFileName().toString() : relative.toString();
  }

  record NamedDecomposition(String name, QueryDecomposition decomposition) {
    NamedDecomposition {
      Objects.requireNonNull(name, "name");
      Objects.requireNonNull(decomposition, "decomposition");
    }
  }
}
