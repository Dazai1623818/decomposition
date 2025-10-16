package decomposition;
import dev.roanh.gmark.util.graph.GraphPanel;
import dev.roanh.gmark.lang.cpq.CPQ;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class AppTest {

    @Test
    void processExampleQueryWithoutException() {
        // GraphPanel.show(CPQ.parse("2◦((1∩2)∩2⁻∩id)◦2⁻∩id"));
        // GraphPanel.show(CPQ.parse("(1 ◦ ((1 ◦ 1⁻) ∩ id) ◦ 1⁻) ∩ id"));
        // GraphPanel.show(CPQ.parse("(1 ◦((1 ◦ ((1 ◦ 1⁻) ∩ id) ◦ 1⁻) ∩ id)◦ 1⁻)∩ id"));
        // GraphPanel.show(CPQ.parse("1∩(1◦1⁻)∩id"));
        // assertDoesNotThrow(() -> App.processQuery(Example.example1()));
    }
}
