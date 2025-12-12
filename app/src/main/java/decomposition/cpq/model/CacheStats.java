package decomposition.cpq.model;

import java.util.BitSet;
import java.util.Objects;
import java.util.Set;

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

  public static record ComponentKey(BitSet edgeBits, String source, String target) {
    public ComponentKey {
      edgeBits = (BitSet) edgeBits.clone();
      Objects.requireNonNull(source, "source");
      Objects.requireNonNull(target, "target");
    }

    public BitSet bits() {
      return (BitSet) edgeBits.clone();
    }
  }

  public static record RuleCacheKey(
      String signature,
      Set<String> joinNodes,
      int edgeCount,
      int diameterCap,
      boolean firstHit,
      boolean enforceEndpointRoles) {
    public RuleCacheKey {
      joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
    }
  }
}
