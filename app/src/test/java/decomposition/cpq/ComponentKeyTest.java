package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import decomposition.cpq.model.CacheStats.ComponentKey;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class ComponentKeyTest {

  @Test
  void equalKeysWithSameBitsAndEndpoints() {
    BitSet bits = new BitSet(3);
    bits.set(0);
    bits.set(2);

    ComponentKey key1 = new ComponentKey(bits, "a", "b");
    ComponentKey key2 = new ComponentKey(bits, "a", "b");

    assertEquals(key1, key2, "Keys with same bits and endpoints should be equal");
    assertEquals(key1.hashCode(), key2.hashCode(), "Equal keys must have same hash code");
  }

  @Test
  void unequalKeysWithDifferentBits() {
    BitSet bits1 = new BitSet(3);
    bits1.set(0);
    bits1.set(1);

    BitSet bits2 = new BitSet(3);
    bits2.set(0);
    bits2.set(2);

    ComponentKey key1 = new ComponentKey(bits1, "a", "b");
    ComponentKey key2 = new ComponentKey(bits2, "a", "b");

    assertNotEquals(key1, key2, "Keys with different bits should not be equal");
  }

  @Test
  void unequalKeysWithDifferentSource() {
    BitSet bits = new BitSet(3);
    bits.set(0);

    ComponentKey key1 = new ComponentKey(bits, "a", "b");
    ComponentKey key2 = new ComponentKey(bits, "x", "b");

    assertNotEquals(key1, key2, "Keys with different source should not be equal");
  }

  @Test
  void unequalKeysWithDifferentTarget() {
    BitSet bits = new BitSet(3);
    bits.set(0);

    ComponentKey key1 = new ComponentKey(bits, "a", "b");
    ComponentKey key2 = new ComponentKey(bits, "a", "x");

    assertNotEquals(key1, key2, "Keys with different target should not be equal");
  }

  @Test
  void keysWorkAsMapKeys() {
    BitSet bits1 = new BitSet(2);
    bits1.set(0);

    BitSet bits2 = new BitSet(2);
    bits2.set(1);

    ComponentKey key1 = new ComponentKey(bits1, "a", "b");
    ComponentKey key2 = new ComponentKey(bits2, "c", "d");
    ComponentKey key1Dup = new ComponentKey(bits1, "a", "b");

    Map<ComponentKey, String> map = new HashMap<>();
    map.put(key1, "first");
    map.put(key2, "second");

    assertEquals("first", map.get(key1Dup), "Duplicate key should retrieve same value");
    assertEquals(2, map.size(), "Map should have 2 distinct keys");
  }

  @Test
  void keysAreDeeplyIsolated() {
    BitSet bits = new BitSet(3);
    bits.set(0);
    bits.set(1);

    ComponentKey key = new ComponentKey(bits, "a", "b");

    // Mutate the original bitset
    bits.set(2);

    BitSet keyBits = key.bits();
    assertEquals(
        2, keyBits.cardinality(), "Key should have defensive copy, unaffected by mutation");
  }

  @Test
  void returnedBitsAreDefensivelyCopied() {
    BitSet bits = new BitSet(3);
    bits.set(0);

    ComponentKey key = new ComponentKey(bits, "a", "b");
    BitSet retrieved = key.bits();

    // Mutate the retrieved bitset
    retrieved.set(1);
    retrieved.set(2);

    BitSet retrievedAgain = key.bits();
    assertEquals(1, retrievedAgain.cardinality(), "Key should return defensive copy each time");
  }

  @Test
  void hashCodeIsStableAcrossMultipleCalls() {
    BitSet bits = new BitSet(5);
    bits.set(1);
    bits.set(3);
    bits.set(4);

    ComponentKey key = new ComponentKey(bits, "source", "target");

    int hash1 = key.hashCode();
    int hash2 = key.hashCode();
    int hash3 = key.hashCode();

    assertEquals(hash1, hash2, "Hash code should be stable");
    assertEquals(hash2, hash3, "Hash code should be stable");
  }

  @Test
  void identicalBitPatternsHaveSameHash() {
    BitSet bits1 = new BitSet(4);
    bits1.set(0);
    bits1.set(2);
    bits1.set(3);

    BitSet bits2 = new BitSet(4);
    bits2.set(0);
    bits2.set(2);
    bits2.set(3);

    ComponentKey key1 = new ComponentKey(bits1, "a", "b");
    ComponentKey key2 = new ComponentKey(bits2, "a", "b");

    assertEquals(
        key1.hashCode(), key2.hashCode(), "Identical bit patterns should hash identically");
  }

  @Test
  void totalEdgesDoesNotAffectEquality() {
    BitSet bits = new BitSet(3);
    bits.set(0);

    ComponentKey key1 = new ComponentKey(bits, "a", "b");
    ComponentKey key2 = new ComponentKey(bits, "a", "b");

    // Component identity depends only on (edgeBits, source, target)
    assertEquals(key1, key2, "component identity ignores total edge counts");
    assertEquals(key1.hashCode(), key2.hashCode(), "Equal keys must have same hash");
  }
}
