package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

final class CPQStringBuilderTest {

    @Test
    void concatenationIsParenthesized() {
        assertEquals("(a ◦ b)", CPQStringBuilder.concat("a", "b"));
    }

    @Test
    void conjunctionIsParenthesized() {
        assertEquals("(a ∩ b)", CPQStringBuilder.conjunction("a", "b"));
    }

    @Test
    void withIdAnchorsExpression() {
        assertEquals("((a ◦ b) ∩ id)", CPQStringBuilder.withId(CPQStringBuilder.concat("a", "b")));
    }
}
