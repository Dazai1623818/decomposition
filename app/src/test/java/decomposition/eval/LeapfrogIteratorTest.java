package decomposition.eval;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class LeapfrogIteratorTest {

  @Test
  void testIntersectionOfThree() {
    // A: 10, 20, 30
    // B: 10, 25, 30
    // C: 20, 25, 30
    // Intersection: 30
    // Buggy implementation might return 20 or 25 or 10 depending on order.

    // Case 1: A=10, B=20, C=20. Sorted: A, B, C.
    // If we have:
    // A: 10, 30
    // B: 20, 30
    // C: 20, 30

    int[] a = {10, 30};
    int[] b = {20, 30};
    int[] c = {20, 30};

    LeapfrogIterator iter =
        new LeapfrogIterator(
            List.of(
                new LeapfrogIterator.IntCursor(a),
                new LeapfrogIterator.IntCursor(b),
                new LeapfrogIterator.IntCursor(c)));

    iter.init();

    // Should return 30.
    // If bug exists:
    // Sorted: A(10), B(20), C(20). Focus C.
    // C(20) == B(20). Returns 20.

    assertFalse(iter.atEnd(), "Should not be at end");
    assertEquals(30, iter.key(), "Should find 30");

    iter.next();
    assertTrue(iter.atEnd(), "Should be at end after 30");
  }

  @Test
  void testFalsePositive() {
    // A: 10
    // B: 20
    // C: 20
    // Intersection: None.

    int[] a = {10};
    int[] b = {20};
    int[] c = {20};

    LeapfrogIterator iter =
        new LeapfrogIterator(
            List.of(
                new LeapfrogIterator.IntCursor(a),
                new LeapfrogIterator.IntCursor(b),
                new LeapfrogIterator.IntCursor(c)));

    iter.init();

    assertTrue(iter.atEnd(), "Should be at end (no intersection)");
  }
}
