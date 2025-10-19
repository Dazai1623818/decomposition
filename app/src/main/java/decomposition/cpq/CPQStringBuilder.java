package decomposition.cpq;

/**
 * Helper for composing CPQ expressions as strings with consistent parenthesization.
 */
public final class CPQStringBuilder {
    private CPQStringBuilder() {
    }

    public static String atom(String label) {
        return label;
    }

    public static String inverse(String expr) {
        return "(" + expr + ")⁻";
    }

    public static String concat(String left, String right) {
        return "(" + left + " ◦ " + right + ")";
    }

    public static String conjunction(String left, String right) {
        return "(" + left + " ∩ " + right + ")";
    }

    public static String withId(String expr) {
        return "(" + expr + " ∩ id)";
    }
}
