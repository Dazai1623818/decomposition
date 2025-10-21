package decomposition.cpq;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility that canonicalizes CPQ string representations so logically equivalent
 * expressions share a stable textual form. This allows deduplication logic to
 * treat (a ∩ b) and (b ∩ a) as identical while preserving composition order.
 */
final class CPQCanonicalizer {
    private CPQCanonicalizer() {
        // Utility class
    }

    static String canonicalize(String cpqStr) {
        return normalizeExpression(cpqStr.replaceAll("\\s+", ""));
    }

    private static String normalizeExpression(String expr) {
        if (!expr.contains("∩") && !expr.contains("◦")) {
            return expr;
        }

        if (expr.startsWith("(") && expr.endsWith(")")) {
            int depth = 0;
            int matchingClose = -1;
            for (int i = 0; i < expr.length(); i++) {
                char ch = expr.charAt(i);
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                    if (depth == 0) {
                        matchingClose = i;
                        break;
                    }
                }
            }
            if (matchingClose == expr.length() - 1) {
                return "(" + normalizeExpression(expr.substring(1, expr.length() - 1)) + ")";
            }
        }

        int topLevelOp = findTopLevelOperator(expr);
        if (topLevelOp == -1) {
            return expr;
        }

        char op = expr.charAt(topLevelOp);
        if (op == '∩') {
            return normalizeConjunction(expr);
        }
        if (op == '◦') {
            return normalizeComposition(expr, topLevelOp);
        }
        return expr;
    }

    private static int findTopLevelOperator(String expr) {
        int depth = 0;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && c == '∩') {
                return i;
            }
        }

        depth = 0;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && c == '◦') {
                return i;
            }
        }

        return -1;
    }

    private static String normalizeConjunction(String expr) {
        List<String> operands = splitByTopLevelOperator(expr, '∩');
        List<String> normalizedOperands = new ArrayList<>(operands.size());
        for (String operand : operands) {
            normalizedOperands.add(normalizeExpression(operand.trim()));
        }
        normalizedOperands.sort(String::compareTo);

        if (normalizedOperands.size() == 1) {
            return normalizedOperands.get(0);
        }

        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < normalizedOperands.size(); i++) {
            if (i > 0) {
                sb.append('∩');
            }
            sb.append(normalizedOperands.get(i));
        }
        return sb.append(')').toString();
    }

    private static String normalizeComposition(String expr, int opPos) {
        String left = expr.substring(0, opPos);
        String right = expr.substring(opPos + 1);
        String normalizedLeft = normalizeExpression(left.trim());
        String normalizedRight = normalizeExpression(right.trim());
        return "(" + normalizedLeft + "◦" + normalizedRight + ")";
    }

    private static List<String> splitByTopLevelOperator(String expr, char operator) {
        List<String> operands = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && c == operator) {
                operands.add(expr.substring(start, i));
                start = i + 1;
            }
        }
        if (start < expr.length()) {
            operands.add(expr.substring(start));
        }
        return operands;
    }
}
