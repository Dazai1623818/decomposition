package decomposition.util;

/** Lightweight timer for tracking operation durations. */
public final class Timing {
  private final long startedAt;

  private Timing(long startedAt) {
    this.startedAt = startedAt;
  }

  public static Timing start() {
    return new Timing(System.nanoTime());
  }

  public long elapsedMillis() {
    return (System.nanoTime() - startedAt) / 1_000_000L;
  }

  public long elapsedNanos() {
    return System.nanoTime() - startedAt;
  }
}
