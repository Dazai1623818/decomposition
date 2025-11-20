package decomposition.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import decomposition.core.model.Edge;
import decomposition.core.model.Partition;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
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
    Path targetDir = baseDir.toAbsolutePath().normalize();
    Path parent = targetDir.getParent();
    if (parent == null) {
      parent = targetDir;
    }
    Files.createDirectories(parent);
    String prefix =
        targetDir.getFileName() != null ? targetDir.getFileName().toString() + "-" : "viz-";
    Path tempDir = Files.createTempDirectory(parent, prefix);
    boolean success = false;
    try {
      writeFreeVariables(tempDir, freeVariables);
      writePartitionSet(tempDir.resolve("unfiltered"), allPartitions, edges);
      writePartitionSet(tempDir.resolve("filtered"), filteredPartitions, edges);
      if (cpqPartitions != null && !cpqPartitions.isEmpty()) {
        writePartitionSet(tempDir.resolve("cpq"), cpqPartitions, edges);
      }
      moveIntoPlace(tempDir, targetDir);
      success = true;
    } finally {
      if (!success) {
        deleteRecursively(tempDir);
      }
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

  private static void moveIntoPlace(Path source, Path target) throws IOException {
    deleteRecursively(target);
    try {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException ex) {
      Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void deleteRecursively(Path path) throws IOException {
    if (path == null || !Files.exists(path)) {
      return;
    }
    Files.walkFileTree(
        path,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }
}
