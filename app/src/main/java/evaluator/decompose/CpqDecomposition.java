package evaluator.decompose;

import evaluator.cq.ConjunctiveQuery;
import dev.roanh.gmark.lang.cq.VarCQ;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.BitSet;
import dev.roanh.gmark.lang.cpq.CPQ;

public final class CpqDecomposition {
    public static record Component(
            VarCQ s,
            VarCQ t,
            int diameter,
            BitSet mask,
            boolean coverable,
            int[] atomCounts,
            CPQ cpq,
            String canonical) {
        public Component {
            Objects.requireNonNull(s, "s");
            Objects.requireNonNull(t, "t");
            Objects.requireNonNull(mask, "mask");
            Objects.requireNonNull(cpq, "cpq");
            Objects.requireNonNull(canonical, "canonical");
            if (diameter < 0) {
                throw new IllegalArgumentException("diameter must be >= 0");
            }
            mask = (BitSet) mask.clone();
            if (atomCounts != null) {
                atomCounts = atomCounts.clone();
                int max = 0;
                for (int c : atomCounts) {
                    if (c < 0) {
                        throw new IllegalArgumentException("atomCounts must be >= 0");
                    }
                    max = Math.max(max, c);
                }
                boolean computedCoverable = max <= 1;
                if (computedCoverable != coverable) {
                    throw new IllegalArgumentException("coverable does not match atomCounts");
                }
                for (int e = mask.nextSetBit(0); e >= 0; e = mask.nextSetBit(e + 1)) {
                    if (e >= atomCounts.length || atomCounts[e] == 0) {
                        throw new IllegalArgumentException("mask contains atom " + e + " not present in atomCounts");
                    }
                }
            }
        }

        public Component(VarCQ s, VarCQ t, int diameter, BitSet mask, CPQ cpq, String canonical) {
            this(s, t, diameter, mask, true, null, cpq, canonical);
        }

        public boolean isUnary() {
            return s.equals(t);
        }
    }

    private final ConjunctiveQuery cq;
    private final List<Component> components;
    private final List<String> variableOrder;

    public CpqDecomposition(ConjunctiveQuery cq, List<Component> components) {
        this.cq = Objects.requireNonNull(cq, "cq");
        this.components = List.copyOf(Objects.requireNonNull(components, "components"));
        this.variableOrder = computeVariableOrder(components);
    }

    public ConjunctiveQuery cq() {
        return cq;
    }

    public List<Component> components() {
        return components;
    }

    public Set<VarCQ> freeVars() {
        return cq.freeVariables();
    }

    public List<String> variableOrder() {
        return variableOrder;
    }

    private static List<String> computeVariableOrder(List<Component> components) {
        Map<String, Integer> counts = new HashMap<>();
        for (Component part : components) {
            counts.merge(varName(part.s()), 1, Integer::sum);
            counts.merge(varName(part.t()), 1, Integer::sum);
        }
        return counts.keySet().stream()
                .sorted(Comparator
                        .comparingInt((String v) -> counts.getOrDefault(v, 0))
                        .reversed()
                        .thenComparing(Comparator.naturalOrder()))
                .toList();
    }

    public static String varName(VarCQ v) {
        return "?" + v.getName();
    }
}
