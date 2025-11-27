package decomposition.nativeindex;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * Lightweight wrapper around the CPQ-native index fork that hides the original {@code dev.roanh}
 * packages from the rest of the codebase.
 */
public final class CpqNativeIndex {
  private final Index delegate;

  public CpqNativeIndex(UniqueGraph<Integer, Predicate> graph, int k, int threads)
      throws IllegalArgumentException, InterruptedException {
    this.delegate = new Index(graph, k, true, true, threads, 2, ProgressListener.LOG);
  }

  public void sort() {
    delegate.sort();
  }

  public void print() {
    try {
      delegate.print();
    } catch (NullPointerException ex) {
      // Some persisted indexes were saved without label metadata; skip printing in that case.
      System.out.println("Index loaded without labels; skipping printable summary.");
    }
  }

  public List<Pair> query(CPQ cpq) {
    Objects.requireNonNull(cpq, "cpq");
    List<dev.roanh.cpqindex.Pair> results = delegate.query(cpq);
    if (results == null || results.isEmpty()) {
      return List.of();
    }
    List<Pair> wrapped = new ArrayList<>(results.size());
    for (dev.roanh.cpqindex.Pair pair : results) {
      wrapped.add(new Pair(pair));
    }
    return Collections.unmodifiableList(wrapped);
  }

  public void save(Path path) throws IOException {
    Objects.requireNonNull(path, "path");
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    try (OutputStream out = Files.newOutputStream(path)) {
      delegate.write(out, false);
    } catch (RuntimeException ex) {
      // Some graphs may carry missing predicate aliases; avoid failing the main flow if persistence
      // is not possible.
      throw new IOException("Unable to persist native index: " + ex.getMessage(), ex);
    }
  }

  public static CpqNativeIndex load(Path path) throws IOException {
    Objects.requireNonNull(path, "path");
    try (InputStream in = Files.newInputStream(path)) {
      return new CpqNativeIndex(new Index(in));
    }
  }

  private CpqNativeIndex(Index delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  public static UniqueGraph<Integer, Predicate> readGraph(Path path) throws IOException {
    Objects.requireNonNull(path, "path");
    List<String> lines = Files.readAllLines(path);
    if (lines.isEmpty()) {
      throw new IOException("Empty graph file: " + path);
    }
    UniqueGraph<Integer, Predicate> graph = new UniqueGraph<>();
    HashMap<Integer, Predicate> predicates = new HashMap<>();
    boolean headerSkipped = false;
    for (String line : lines) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      String[] parts = trimmed.split("\\s+");
      if (parts.length < 3) {
        continue; // header or malformed line
      }
      if (!headerSkipped) {
        try {
          int labelCount = Integer.parseInt(parts[2]);
          for (int id = 0; id < labelCount; id++) {
            predicates.putIfAbsent(id, new Predicate(id, String.valueOf(id)));
          }
        } catch (NumberFormatException ignored) {
          // ignore header parsing issues
        }
        headerSkipped = true; // first line is counts header
        continue;
      }
      try {
        int src = Integer.parseInt(parts[0]);
        int tgt = Integer.parseInt(parts[1]);
        int labelId = Integer.parseInt(parts[2]);
        Predicate predicate =
            predicates.computeIfAbsent(labelId, id -> new Predicate(id, String.valueOf(id)));
        graph.addUniqueNode(src);
        graph.addUniqueNode(tgt);
        graph.addUniqueEdge(src, tgt, predicate);
      } catch (NumberFormatException ignored) {
        // Skip malformed lines; IndexUtil would throw similarly
      }
    }
    return graph;
  }

  public static final class Pair {
    private final dev.roanh.cpqindex.Pair delegate;

    private Pair(dev.roanh.cpqindex.Pair delegate) {
      this.delegate = delegate;
    }

    public int getSource() {
      return delegate.getSource();
    }

    public int getTarget() {
      return delegate.getTarget();
    }
  }
}
