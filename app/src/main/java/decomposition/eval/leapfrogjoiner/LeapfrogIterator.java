package decomposition.eval.leapfrogjoiner;

import java.util.List;

/** Intersects multiple {@link LeapfrogIterator.IntCursor} streams using the Leapfrog strategy. */
final class LeapfrogIterator {
  private final List<IntCursor> cursors;
  private int p;

  LeapfrogIterator(List<IntCursor> cursors) {
    this.cursors = cursors;
    this.p = 0;
  }

  void init() {
    if (cursors.isEmpty()) {
      return;
    }
    for (IntCursor cursor : cursors) {
      cursor.seekToStart();
      if (cursor.atEnd()) {
        return;
      }
    }
    p = 0;
    leapfrogSearch();
  }

  boolean atEnd() {
    return cursors.isEmpty() || cursors.get(p).atEnd();
  }

  int key() {
    return cursors.get(p).key();
  }

  void next() {
    if (atEnd()) {
      return;
    }
    cursors.get(p).next();
    leapfrogSearch();
  }

  private void leapfrogSearch() {
    if (cursors.isEmpty()) {
      return;
    }
    int k = cursors.size();
    while (true) {
      if (cursors.get(p).atEnd()) {
        return;
      }
      int maxKey = cursors.get(p).key();
      int matches = 0;

      for (int i = 1; i < k; i++) {
        int idx = (p + i) % k;
        IntCursor cursor = cursors.get(idx);
        cursor.seek(maxKey);
        if (cursor.atEnd()) {
          p = idx; // Mark as atEnd
          return;
        }
        int key = cursor.key();
        if (key == maxKey) {
          matches++;
        } else {
          // key > maxKey
          p = idx;
          matches = -1;
          break;
        }
      }

      if (matches == k - 1) {
        return;
      }
    }
  }

  /** A lightweight cursor over a sorted integer array for Leapfrog Trie Join. */
  static final class IntCursor {
    private final int[] values;
    private int position;

    IntCursor(int[] values) {
      this.values = values;
      this.position = 0;
    }

    void seekToStart() {
      position = 0;
    }

    boolean atEnd() {
      return position >= values.length;
    }

    int key() {
      return values[position];
    }

    void next() {
      position++;
    }

    void seek(int target) {
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
}
