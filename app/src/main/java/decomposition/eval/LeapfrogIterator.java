package decomposition.eval;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Implements the value-skipping intersection used by Leapfrog Triejoin. Each cursor enumerates
 * sorted integers and supports {@code seek}.
 */
final class LeapfrogIterator {
  private final List<IntCursor> cursors;
  private int focus;
  private int currentKey;
  private boolean atEnd;

  LeapfrogIterator(List<IntCursor> cursors) {
    this.cursors = new ArrayList<>(cursors);
    this.focus = 0;
    this.currentKey = -1;
    this.atEnd = cursors.isEmpty();
  }

  void init() {
    if (atEnd) {
      return;
    }
    for (IntCursor cursor : cursors) {
      cursor.toFirst();
      if (cursor.atEnd()) {
        atEnd = true;
        return;
      }
    }
    cursors.sort(Comparator.comparingInt(IntCursor::key));
    focus = cursors.size() - 1;
    leapfrogSearch();
  }

  boolean atEnd() {
    return atEnd;
  }

  int key() {
    return currentKey;
  }

  void next() {
    if (atEnd) {
      return;
    }
    IntCursor cursor = cursors.get(focus);
    cursor.next();
    if (cursor.atEnd()) {
      atEnd = true;
      return;
    }
    focus = (focus + 1) % cursors.size();
    leapfrogSearch();
  }

  private void leapfrogSearch() {
    int count = 0;
    while (count < cursors.size()) {
      IntCursor current = cursors.get(focus);
      IntCursor previous = cursors.get((focus + cursors.size() - 1) % cursors.size());
      int currentValue = current.key();
      int previousValue = previous.key();

      if (currentValue == previousValue) {
        count++;
        focus = (focus + 1) % cursors.size();
      } else {
        current.seek(previousValue);
        if (current.atEnd()) {
          atEnd = true;
          return;
        }
        count = 1;
        focus = (focus + 1) % cursors.size();
      }
    }
    currentKey = cursors.get(0).key();
  }

  static final class IntCursor {
    private final int[] data;
    private int index;

    IntCursor(int[] data) {
      this.data = data;
      this.index = 0;
    }

    void toFirst() {
      index = 0;
    }

    boolean atEnd() {
      return data.length == 0 || index >= data.length;
    }

    int key() {
      return data[index];
    }

    void next() {
      index++;
    }

    void seek(int target) {
      if (atEnd()) {
        return;
      }
      if (data[index] >= target) {
        return;
      }
      int lo = index;
      int hi = data.length - 1;
      int pos = data.length;
      while (lo <= hi) {
        int mid = (lo + hi) >>> 1;
        if (data[mid] < target) {
          lo = mid + 1;
        } else {
          pos = mid;
          hi = mid - 1;
        }
      }
      index = pos;
    }
  }
}
