package decomposition.eval.leapfrogjoiner;

/** A lightweight cursor over a sorted integer array for Leapfrog Trie Join. */
public final class IntCursor {
  private final int[] values;
  private int position;

  public IntCursor(int[] values) {
    this.values = values;
    this.position = 0;
  }

  public void seekToStart() {
    position = 0;
  }

  public boolean atEnd() {
    return position >= values.length;
  }

  public int key() {
    return values[position];
  }

  public void next() {
    position++;
  }

  public void seek(int target) {
    int low = position;
    int high = values.length - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (values[mid] < target) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    position = low;
  }
}
