package decomposition;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.util.graph.GraphPanel;

/**
 * Small helper entry point that opens a Swing window showing the structure of a CPQ.
 *
 * Usage examples:
 * <pre>
 *   ./gradlew runPlot --args='r1'
 *   ./gradlew runPlot --args='(r1 ◦ (r2 ∩ r3⁻))'
 * </pre>
 */
public final class PlotCPQ {

    private PlotCPQ() {
        // utility
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Provide a CPQ expression to plot. Example: ./gradlew runPlot --args='r1 ◦ r2'");
            System.exit(1);
        }

        String cpqExpression = String.join(" ", args).replace(" ", "");
        try {
            CPQ cpq = CPQ.parse(cpqExpression);
            System.out.println("Parsed CPQ: " + cpq);
            System.out.println("Diameter: " + cpq.getDiameter());
            GraphPanel.show(cpq);
        } catch (IllegalArgumentException ex) {
            System.err.println("Unable to parse CPQ: " + ex.getMessage());
            System.exit(2);
        }
    }
}
