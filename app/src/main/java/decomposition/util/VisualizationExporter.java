package decomposition.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import decomposition.model.Edge;
import decomposition.model.Partition;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Writes partition data in the legacy format consumed by the visualization notebook. */
public final class VisualizationExporter {
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private VisualizationExporter() {}

  public static void export(
      Path baseDir,
      List<Edge> edges,
      Set<String> freeVariables,
      List<Partition> allPartitions,
      List<Partition> filteredPartitions,
      List<Partition> cpqPartitions)
      throws IOException {
    if (baseDir == null) {
      return;
    }
    cleanDirectory(baseDir);
    writeFreeVariables(baseDir, freeVariables);
    writePartitionSet(baseDir.resolve("unfiltered"), allPartitions, edges);
    writePartitionSet(baseDir.resolve("filtered"), filteredPartitions, edges);
    if (cpqPartitions != null && !cpqPartitions.isEmpty()) {
      writePartitionSet(baseDir.resolve("cpq"), cpqPartitions, edges);
    }
  }

  private static void writeFreeVariables(Path baseDir, Set<String> freeVariables)
      throws IOException {
    Files.createDirectories(baseDir);
    Path freeVarsPath = baseDir.resolve("free_vars.json");
    Files.writeString(freeVarsPath, GSON.toJson(freeVariables));
  }

  private static void writePartitionSet(Path dir, List<Partition> partitions, List<Edge> edges)
      throws IOException {
    if (partitions == null || partitions.isEmpty()) {
      return;
    }
    Files.createDirectories(dir);
    for (int i = 0; i < partitions.size(); i++) {
      Partition partition = partitions.get(i);
      List<Map<String, Object>> payload = new ArrayList<>();
      int componentIndex = 0;
      for (var component : partition.components()) {
        List<Integer> edgeIndices = BitsetUtils.toIndexList(component.edgeBits());
        for (Integer edgeIndex : edgeIndices) {
          Edge edge = edges.get(edgeIndex);
          Map<String, Object> entry = new HashMap<>();
          entry.put("source", edge.source());
          entry.put("target", edge.target());
          entry.put("label", edge.label());
          entry.put("component", componentIndex);
          payload.add(entry);
        }
        componentIndex++;
      }
      Path file = dir.resolve("partition_" + (i + 1) + ".json");
      Files.writeString(file, GSON.toJson(payload));
    }
  }

  private static void cleanDirectory(Path baseDir) throws IOException {
    if (!Files.exists(baseDir)) {
      return;
    }
    try (var walk = Files.walk(baseDir)) {
      Iterator<Path> iterator = walk.sorted(Comparator.reverseOrder()).iterator();
      while (iterator.hasNext()) {
        Files.deleteIfExists(iterator.next());
      }
    }
  }
}
