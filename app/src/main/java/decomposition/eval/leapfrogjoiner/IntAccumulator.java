package decomposition.eval.leapfrogjoiner;

import java.util.Arrays;

/** Helper for building sorted integer arrays. */
public final class IntAccumulator {
  private static final int[] EMPTY_INT_ARRAY = new int[0];
  private int[] buffer = new int[16];
  private int size = 0;

  public void add(int value) {
    if (size == buffer.length) {
      int[] expanded = new int[buffer.length * 2];
      System.arraycopy(buffer, 0, expanded, 0, buffer.length);
      buffer = expanded;
    }
    buffer[size++] = value;
  }

  public int[] toSortedDistinctArray() {
    if (size == 0) {
      return EMPTY_INT_ARRAY;
    }
    int[] result = new int[size];
    System.arraycopy(buffer, 0, result, 0, size);
    Arrays.sort(result);
    int unique = 1;
    for (int i = 1; i < result.length; i++) {
      if (result[i] != result[unique - 1]) {
        result[unique++] = result[i];
      }
    }
    if (unique == result.length) {
      return result;
    }
    int[] trimmed = new int[unique];
    System.arraycopy(result, 0, trimmed, 0, unique);
    return trimmed;
  }
}
