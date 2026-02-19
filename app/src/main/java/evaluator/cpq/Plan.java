package evaluator.cpq;

import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class Plan {
    private final ConjunctiveQuery cq;
    private final List<Component> components;
    private final List<String> variableOrder;

    public Plan(ConjunctiveQuery cq, List<Component> components) {
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

    public int size() {
        return components.size();
    }

    public int maxDiameter() {
        int max = 0;
        for (Component component : components) {
            max = Math.max(max, component.diameter());
        }
        return max;
    }

    public Set<VarCQ> freeVars() {
        return cq.freeVariables();
    }

    public List<String> variableOrder() {
        return variableOrder;
    }

    /**
     * Returns free variables using evaluator naming format, sorted for stable
     * projection output order.
     */
    public List<String> projectedVariableNames() {
        return freeVars().stream()
                .map(Plan::varName)
                .sorted()
                .toList();
    }

    private static List<String> computeVariableOrder(List<Component> components) {
        Map<String, Integer> counts = new HashMap<>();
        for (Component part : components) {
            counts.merge(part.sourceVarName(), 1, Integer::sum);
            counts.merge(part.targetVarName(), 1, Integer::sum);
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

    public static final class Component {
        private final VarCQ s;
        private final VarCQ t;
        private final int diameter;
        private final BitSet mask;
        private final BitSet inverseAtoms;
        private final CPQ cpq;
        private final String normalized;
        private final int id;
        private final int size;

        public Component(VarCQ s, VarCQ t, int diameter, BitSet mask, CPQ cpq, String normalized) {
            this(s, t, diameter, mask, new BitSet(), cpq, normalized, -1, 0);
        }

        /**
         * Internal constructor used by decomposition algorithms that already own
         * atom masks and metadata.
         */
        public Component(
                VarCQ s,
                VarCQ t,
                int diameter,
                BitSet mask,
                BitSet inverseAtoms,
                CPQ cpq,
                String normalized,
                int id,
                int size) {
            Objects.requireNonNull(s, "s");
            Objects.requireNonNull(t, "t");
            Objects.requireNonNull(mask, "mask");
            Objects.requireNonNull(inverseAtoms, "inverseAtoms");
            Objects.requireNonNull(cpq, "cpq");
            Objects.requireNonNull(normalized, "normalized");
            if (diameter < 0) {
                throw new IllegalArgumentException("diameter must be >= 0");
            }
            this.s = s;
            this.t = t;
            this.diameter = diameter;
            this.mask = (BitSet) mask.clone();
            this.inverseAtoms = (BitSet) inverseAtoms.clone();
            this.cpq = cpq;
            this.normalized = normalized;
            this.id = id;
            this.size = size;
        }

        public VarCQ s() {
            return s;
        }

        public VarCQ t() {
            return t;
        }

        public int diameter() {
            return diameter;
        }

        public BitSet mask() {
            return (BitSet) mask.clone();
        }

        /**
         * Internal fast-path accessor for decomposition algorithms.
         * Callers must not mutate the returned bitset.
         */
        public BitSet maskUnsafe() {
            return mask;
        }

        /**
         * Internal fast-path accessor for inverse atoms.
         * Callers must not mutate the returned bitset.
         */
        public BitSet inverseAtoms() {
            return inverseAtoms;
        }

        public CPQ cpq() {
            return cpq;
        }

        public String sourceVarName() {
            return "?" + s.getName();
        }

        public String targetVarName() {
            return "?" + t.getName();
        }

        public String normalized() {
            return normalized;
        }

        /**
         * Stable identity string used for deduplication in cover selection.
         */
        public String signature() {
            return s.getName()
                    + "->" + t.getName()
                    + "#d=" + diameter
                    + "#m=" + mask
                    + "#n=" + normalized;
        }

        public int id() {
            return id;
        }

        public int size() {
            return size;
        }

        public boolean isUnary() {
            return s.equals(t);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Component component)) {
                return false;
            }
            return diameter == component.diameter
                    && s.equals(component.s)
                    && t.equals(component.t)
                    && mask.equals(component.mask)
                    && cpq.equals(component.cpq)
                    && normalized.equals(component.normalized);
        }

        @Override
        public int hashCode() {
            return Objects.hash(s, t, diameter, mask, cpq, normalized);
        }

        @Override
        public String toString() {
            return "Component[s=" + s + ", t=" + t + ", diameter=" + diameter
                    + ", mask=" + mask + ", cpq=" + cpq + ", normalized=" + normalized + "]";
        }
    }
}
