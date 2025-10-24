package decomposition.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.model.Edge;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

final class GraphUtilsTest {

  @Test
  void detectsConnectivity() {
    List<Edge> edges =
        List.of(
            new Edge("a", "b", new Predicate(1, "r1"), 0),
            new Edge("b", "c", new Predicate(2, "r2"), 1),
            new Edge("d", "e", new Predicate(3, "r3"), 2));

    BitSet connectedBits = new BitSet(3);
    connectedBits.set(0);
    connectedBits.set(1);

    BitSet disconnectedBits = new BitSet(3);
    disconnectedBits.set(0);
    disconnectedBits.set(2);

    assertTrue(
        GraphUtils.isConnected(connectedBits, edges), "First two edges form a connected path");
    assertFalse(GraphUtils.isConnected(disconnectedBits, edges), "Edges 0 and 2 are disjoint");
  }
}
