package decomposition.eval.engine;

import java.util.List;

/** Intersects multiple {@link IntCursor} streams using the Leapfrog strategy. */
public final class LeapfrogIterator {
  private final List<IntCursor> cursors;
  private int p;

  public LeapfrogIterator(List<IntCursor> cursors) {
    this.cursors = cursors;
    this.p = 0;
  }

  public void init() {
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

  public boolean atEnd() {
    return cursors.isEmpty() || cursors.get(p).atEnd();
  }

  public int key() {
    return cursors.get(p).key();
  }

  public void next() {
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
}
