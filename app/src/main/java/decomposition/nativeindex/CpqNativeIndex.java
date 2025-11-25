package decomposition.nativeindex;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.ProgressListener;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Lightweight wrapper around the CPQ-native index fork that hides the original
 * {@code dev.roanh}
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
    delegate.print();
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
