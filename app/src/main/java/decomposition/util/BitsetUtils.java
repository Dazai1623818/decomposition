package decomposition.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

/** Utility helpers for working with {@link BitSet} based edge masks. */
public final class BitsetUtils {
  private BitsetUtils() {}

  public static BitSet fromMask(long mask, int size) {
    BitSet bitSet = new BitSet(size);
    for (int i = 0; i < size; i++) {
      if (((mask >>> i) & 1L) != 0) {
        bitSet.set(i);
      }
    }
    return bitSet;
  }

  public static BitSet fromIndices(Iterable<Integer> indices) {
    BitSet bitSet = new BitSet();
    for (Integer index : indices) {
      if (index != null && index >= 0) {
        bitSet.set(index);
      }
    }
    return bitSet;
  }

  public static List<Integer> toIndexList(BitSet bitSet) {
    List<Integer> indices = new ArrayList<>();
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      indices.add(i);
    }
    return indices;
  }

  public static boolean isSubset(BitSet subset, BitSet superset) {
    BitSet clone = (BitSet) subset.clone();
    clone.andNot(superset);
    return clone.isEmpty();
  }

  public static BitSet copy(BitSet bitSet) {
    return bitSet == null ? new BitSet() : (BitSet) bitSet.clone();
  }

  public static BitSet copyAndSet(BitSet bitSet, int index) {
    BitSet clone = copy(bitSet);
    clone.set(index);
    return clone;
  }

  public static BitSet copyAndClear(BitSet bitSet, int index) {
    BitSet clone = copy(bitSet);
    clone.clear(index);
    return clone;
  }

  public static BitSet complement(BitSet bitSet, int size) {
    BitSet clone = copy(bitSet);
    clone.flip(0, size);
    return clone;
  }

  public static BitSet allOnes(int size) {
    BitSet bitSet = new BitSet(size);
    bitSet.set(0, size);
    return bitSet;
  }

  public static String signature(BitSet bitSet, int sizeHint) {
    StringBuilder builder = new StringBuilder(sizeHint);
    builder.append('[');
    boolean first = true;
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      if (!first) {
        builder.append(',');
      }
      builder.append(i);
      first = false;
    }
    return builder.append(']').toString();
  }

  public static IntStream stream(BitSet bitSet) {
    return IntStream.iterate(bitSet.nextSetBit(0), i -> i >= 0, i -> bitSet.nextSetBit(i + 1));
  }
}
