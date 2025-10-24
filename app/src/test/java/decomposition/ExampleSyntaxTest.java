package decomposition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.roanh.gmark.lang.cq.CQ;
import org.junit.jupiter.api.Test;

final class ExampleSyntaxTest {

  @Test
  void example1FormalSyntaxRoundTrips() {
    CQ original = Example.example1();
    String formal = original.toFormalSyntax();
    System.out.println("Example1 formal syntax: " + formal);
    CQ reparsed = CQ.parse(formal);
    assertEquals(original.toFormalSyntax(), reparsed.toFormalSyntax());
  }
}
