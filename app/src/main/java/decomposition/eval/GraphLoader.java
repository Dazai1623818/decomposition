package decomposition.eval;

import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Lightweight replacement for the legacy {@code dev.roanh.cpqindex.IndexUtil} loader. */
final class GraphLoader {
  private GraphLoader() {}

  static UniqueGraph<Integer, Predicate> load(Path path) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      String header = reader.readLine();
      if (header == null) {
        throw new IOException("Empty graph file: " + path);
      }
      String[] parts = header.trim().split("\\s+");
      if (parts.length < 3) {
        throw new IOException("Invalid graph header: " + header);
      }
      int nodeCount = Integer.parseInt(parts[0]);
      int labelCount = Integer.parseInt(parts[2]);
      UniqueGraph<Integer, Predicate> graph = new UniqueGraph<>();
      for (int i = 0; i < nodeCount; i++) {
        graph.addUniqueNode(i);
      }
      List<Predicate> labels = generateLabels(labelCount);

      UniqueGraph.GraphNode<Integer, Predicate> current = null;
      String line;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        String[] tokens = trimmed.split("\\s+");
        if (tokens.length < 3) {
          continue;
        }
        int source = Integer.parseInt(tokens[0]);
        int target = Integer.parseInt(tokens[1]);
        int labelIndex = Integer.parseInt(tokens[2]);
        if (current == null || current.getID() != source) {
          current = graph.getNode(source);
          if (current == null) {
            throw new IOException("Unknown node " + source + " in " + path);
          }
        }
        current.addUniqueEdgeTo(target, labels.get(labelIndex));
      }
      return graph;
    }
  }

  private static List<Predicate> generateLabels(int count) {
    List<Predicate> labels = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      labels.add(new Predicate(i, String.valueOf(i)));
    }
    return labels;
  }
}
