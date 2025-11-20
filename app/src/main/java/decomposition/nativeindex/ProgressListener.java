package decomposition.nativeindex;

import java.util.Objects;

/**
 * Wrapper for the forked CPQ-native index {@link dev.roanh.cpqindex.ProgressListener} that keeps
 * {@code dev.roanh} packages out of the rest of the codebase.
 */
public final class ProgressListener {
  private static final ProgressListener NONE =
      new ProgressListener(dev.roanh.cpqindex.ProgressListener.NONE);
  private static final ProgressListener LOG =
      new ProgressListener(dev.roanh.cpqindex.ProgressListener.LOG);

  private final dev.roanh.cpqindex.ProgressListener delegate;

  private ProgressListener(dev.roanh.cpqindex.ProgressListener delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  public static ProgressListener none() {
    return NONE;
  }

  public static ProgressListener log() {
    return LOG;
  }

  public static ProgressListener wrap(dev.roanh.cpqindex.ProgressListener delegate) {
    return new ProgressListener(delegate);
  }

  dev.roanh.cpqindex.ProgressListener delegate() {
    return delegate;
  }
}
