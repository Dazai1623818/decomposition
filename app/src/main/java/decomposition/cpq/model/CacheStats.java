package decomposition.cpq.model;

public final class CacheStats {
  private long hits;
  private long misses;

  public CacheStats() {}

  private CacheStats(long hits, long misses) {
    this.hits = hits;
    this.misses = misses;
  }

  public static CacheStats of(long hits, long misses) {
    return new CacheStats(hits, misses);
  }

  public void recordHit() {
    hits++;
  }

  public void recordMiss() {
    misses++;
  }

  public long hits() {
    return hits;
  }

  public long misses() {
    return misses;
  }

  public long lookups() {
    return hits + misses;
  }

  public double hitRate() {
    long total = lookups();
    return total == 0 ? 0.0 : hits / (double) total;
  }

  public CacheStats snapshot() {
    return CacheStats.of(hits, misses);
  }
}
