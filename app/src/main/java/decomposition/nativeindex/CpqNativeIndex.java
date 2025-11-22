package decomposition.nativeindex;

import dev.roanh.cpqindex.Index;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
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
    this(new Index(graph, k, threads));
  }

  public CpqNativeIndex(
      UniqueGraph<Integer, Predicate> graph, int k, int threads, int maxIntersections)
      throws IllegalArgumentException, InterruptedException {
    this(new Index(graph, k, threads, maxIntersections));
  }

  public CpqNativeIndex(
      UniqueGraph<Integer, Predicate> graph,
      int k,
      boolean computeCores,
      boolean computeLabels,
      int threads)
      throws IllegalArgumentException, InterruptedException {
    this(new Index(graph, k, computeCores, computeLabels, threads));
  }

  public CpqNativeIndex(
      UniqueGraph<Integer, Predicate> graph,
      int k,
      boolean computeCores,
      boolean computeLabels,
      int threads,
      int maxIntersections,
      ProgressListener listener)
      throws IllegalArgumentException, InterruptedException {
    this(
        new Index(
            graph, k, computeCores, computeLabels, threads, maxIntersections, unwrap(listener)));
  }

  public CpqNativeIndex(InputStream source) throws IOException {
    this(new Index(source));
  }

  private CpqNativeIndex(Index delegate) {
    this.delegate = delegate;
  }

  public static CpqNativeIndex wrap(Index delegate) {
    return new CpqNativeIndex(delegate);
  }

  public Index delegate() {
    return delegate;
  }

  public void sort() {
    delegate.sort();
  }

  public List<Block> getBlocks() {
    List<Index.Block> blocks = delegate.getBlocks();
    if (blocks == null || blocks.isEmpty()) {
      return List.of();
    }
    List<Block> wrapped = new ArrayList<>(blocks.size());
    for (Index.Block block : blocks) {
      wrapped.add(new Block(block));
    }
    return Collections.unmodifiableList(wrapped);
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

  private static dev.roanh.cpqindex.ProgressListener unwrap(ProgressListener listener) {
    return listener == null ? ProgressListener.none().delegate() : listener.delegate();
  }

  public static final class Block {
    private final Index.Block delegate;

    private Block(Index.Block delegate) {
      this.delegate = delegate;
    }

    public List<LabelSequence> getLabels() {
      List<dev.roanh.cpqindex.LabelSequence> labels = delegate.getLabels();
      if (labels == null || labels.isEmpty()) {
        return List.of();
      }
      List<LabelSequence> wrapped = new ArrayList<>(labels.size());
      for (dev.roanh.cpqindex.LabelSequence label : labels) {
        wrapped.add(new LabelSequence(label));
      }
      return Collections.unmodifiableList(wrapped);
    }

    public List<Pair> getPaths() {
      List<dev.roanh.cpqindex.Pair> paths = delegate.getPaths();
      if (paths == null || paths.isEmpty()) {
        return List.of();
      }
      List<Pair> wrapped = new ArrayList<>(paths.size());
      for (dev.roanh.cpqindex.Pair path : paths) {
        wrapped.add(new Pair(path));
      }
      return Collections.unmodifiableList(wrapped);
    }
  }

  public static final class LabelSequence {
    private final dev.roanh.cpqindex.LabelSequence delegate;

    private LabelSequence(dev.roanh.cpqindex.LabelSequence delegate) {
      this.delegate = delegate;
    }

    public Predicate[] getLabels() {
      return delegate.getLabels();
    }
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
