package evaluator.evaluation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Relation {
    private final String sourceVar;
    private final String targetVar;
    private final String description;
    private final RelationProjection projection;
    private final int[] unaryDomain;

    private Relation(String sourceVar, String targetVar, String description, RelationProjection projection) {
        this.sourceVar = Objects.requireNonNull(sourceVar, "sourceVar");
        this.targetVar = Objects.requireNonNull(targetVar, "targetVar");
        this.description = Objects.requireNonNull(description, "description");
        this.projection = Objects.requireNonNull(projection, "projection");
        this.unaryDomain = null;
    }

    private Relation(String variable, String description, int[] unaryDomain) {
        this.sourceVar = Objects.requireNonNull(variable, "variable");
        this.targetVar = null;
        this.description = Objects.requireNonNull(description, "description");
        this.projection = null;
        this.unaryDomain = Objects.requireNonNull(unaryDomain, "unaryDomain");
    }

    public static Relation binary(String sourceVar, String targetVar, String description, RelationProjection projection) {
        return new Relation(sourceVar, targetVar, description, projection);
    }

    public static Relation unary(String variable, String description, int[] domain) {
        return new Relation(variable, description, domain);
    }

    /**
     * Compiles a variable-specific domain accessor for hot-loop join execution.
     * The returned accessor captures any required variable index once and avoids
     * repeated map/string branching while evaluating domains.
     */
    public DomainAccessor compileForVariable(String variable, Map<String, Integer> indexByVar) {
        Objects.requireNonNull(variable, "variable");
        Objects.requireNonNull(indexByVar, "indexByVar");
        if (isUnary()) {
            return new UnaryDomainAccessor(unaryDomain);
        }
        if (variable.equals(sourceVar)) {
            int targetIndex = indexByVar.getOrDefault(targetVar, -1);
            return new SourceDomainAccessor(projection, targetIndex);
        }
        if (variable.equals(targetVar)) {
            int sourceIndex = indexByVar.getOrDefault(sourceVar, -1);
            return new TargetDomainAccessor(projection, sourceIndex);
        }
        throw new IllegalArgumentException("Variable " + variable + " not part of relation " + description);
    }

    public void register(Map<String, List<Relation>> relationsPerVariable) {
        Objects.requireNonNull(relationsPerVariable, "relationsPerVariable");
        for (String variable : variables()) {
            registerVariable(relationsPerVariable, variable);
        }
    }

    public int[] domainFor(String variable, int[] assignment, boolean[] bound, Map<String, Integer> indexByVar) {
        Objects.requireNonNull(variable, "variable");
        if (isUnary()) {
            return unaryDomain;
        }
        if (variable.equals(sourceVar)) {
            return sourcesForTarget(assignment, bound, indexByVar);
        }
        if (variable.equals(targetVar)) {
            return targetsForSource(assignment, bound, indexByVar);
        }
        throw new IllegalArgumentException("Variable " + variable + " not part of relation " + description);
    }

    public String sourceVar() {
        return sourceVar;
    }

    public String targetVar() {
        return targetVar;
    }

    public String description() {
        return description;
    }

    public boolean isUnary() {
        return unaryDomain != null;
    }

    /**
     * Returns relation variable names in stable order.
     */
    public List<String> variables() {
        return targetVar == null ? List.of(sourceVar) : List.of(sourceVar, targetVar);
    }

    private void registerVariable(Map<String, List<Relation>> relationsPerVariable, String variable) {
        relationsPerVariable.computeIfAbsent(variable, ignored -> new ArrayList<>()).add(this);
    }

    private int[] sourcesForTarget(int[] assignment, boolean[] bound, Map<String, Integer> indexByVar) {
        Integer targetIndex = indexByVar.get(targetVar);
        if (targetIndex != null && bound[targetIndex]) {
            int target = assignment[targetIndex];
            return projection.sourcesForTarget(target);
        }
        return projection.allSources();
    }

    private int[] targetsForSource(int[] assignment, boolean[] bound, Map<String, Integer> indexByVar) {
        Integer sourceIndex = indexByVar.get(sourceVar);
        if (sourceIndex != null && bound[sourceIndex]) {
            int source = assignment[sourceIndex];
            return projection.targetsForSource(source);
        }
        return projection.allTargets();
    }

    /**
     * Variable-specific domain lookup used by compiled join constraints.
     */
    public interface DomainAccessor {
        int[] domain(int[] assignment, boolean[] bound);
    }

    private static final class UnaryDomainAccessor implements DomainAccessor {
        private final int[] domain;

        UnaryDomainAccessor(int[] domain) {
            this.domain = domain;
        }

        @Override
        public int[] domain(int[] assignment, boolean[] bound) {
            return domain;
        }
    }

    private static final class SourceDomainAccessor implements DomainAccessor {
        private final RelationProjection projection;
        private final int targetIndex;

        SourceDomainAccessor(RelationProjection projection, int targetIndex) {
            this.projection = projection;
            this.targetIndex = targetIndex;
        }

        @Override
        public int[] domain(int[] assignment, boolean[] bound) {
            if (targetIndex >= 0 && bound[targetIndex]) {
                return projection.sourcesForTarget(assignment[targetIndex]);
            }
            return projection.allSources();
        }
    }

    private static final class TargetDomainAccessor implements DomainAccessor {
        private final RelationProjection projection;
        private final int sourceIndex;

        TargetDomainAccessor(RelationProjection projection, int sourceIndex) {
            this.projection = projection;
            this.sourceIndex = sourceIndex;
        }

        @Override
        public int[] domain(int[] assignment, boolean[] bound) {
            if (sourceIndex >= 0 && bound[sourceIndex]) {
                return projection.targetsForSource(assignment[sourceIndex]);
            }
            return projection.allTargets();
        }
    }

    public static final class RelationProjection {
        private static final int[] EMPTY_INT_ARRAY = new int[0];
        private static final int[][] EMPTY_INT_MATRIX = new int[0][];

        private final int[] allSources;
        private final int[] allTargets;
        private final int[] forwardKeys;
        private final int[][] forwardValues;
        private final int[] reverseKeys;
        private final int[][] reverseValues;

        /**
         * Builds a relation projection from boxed maps.
         * Maps are densified once so hot-path lookups avoid boxing.
         */
        public RelationProjection(int[] allSources, int[] allTargets, Map<Integer, int[]> forward, Map<Integer, int[]> reverse) {
            this(allSources, allTargets, denseLookup(forward, "forward"), denseLookup(reverse, "reverse"));
        }

        private RelationProjection(int[] allSources, int[] allTargets, DenseLookup forward, DenseLookup reverse) {
            this(allSources, allTargets, forward.keys(), forward.values(), reverse.keys(), reverse.values());
        }

        /**
         * Builds a projection from sorted key/domain arrays.
         * Keys must be strictly increasing; each key index maps to the domain at the same position.
         */
        public RelationProjection(
                int[] allSources,
                int[] allTargets,
                int[] forwardKeys,
                int[][] forwardValues,
                int[] reverseKeys,
                int[][] reverseValues) {
            this.allSources = Objects.requireNonNull(allSources, "allSources");
            this.allTargets = Objects.requireNonNull(allTargets, "allTargets");
            this.forwardKeys = Objects.requireNonNull(forwardKeys, "forwardKeys");
            this.forwardValues = Objects.requireNonNull(forwardValues, "forwardValues");
            this.reverseKeys = Objects.requireNonNull(reverseKeys, "reverseKeys");
            this.reverseValues = Objects.requireNonNull(reverseValues, "reverseValues");
            if (forwardKeys.length != forwardValues.length) {
                throw new IllegalArgumentException("forwardKeys/forwardValues length mismatch");
            }
            if (reverseKeys.length != reverseValues.length) {
                throw new IllegalArgumentException("reverseKeys/reverseValues length mismatch");
            }
            requireStrictlyIncreasing(forwardKeys, "forwardKeys");
            requireStrictlyIncreasing(reverseKeys, "reverseKeys");
            requireNonNullDomains(forwardValues, "forwardValues");
            requireNonNullDomains(reverseValues, "reverseValues");
        }

        public boolean isEmpty() {
            return allSources.length == 0 || allTargets.length == 0;
        }

        public int[] allSources() {
            return allSources;
        }

        public int[] allTargets() {
            return allTargets;
        }

        public int[] targetsForSource(int source) {
            return lookup(source, forwardKeys, forwardValues);
        }

        public int[] sourcesForTarget(int target) {
            return lookup(target, reverseKeys, reverseValues);
        }

        private static int[] lookup(int key, int[] keys, int[][] values) {
            int index = Arrays.binarySearch(keys, key);
            return index >= 0 ? values[index] : EMPTY_INT_ARRAY;
        }

        private static DenseLookup denseLookup(Map<Integer, int[]> input, String name) {
            Objects.requireNonNull(input, name);
            if (input.isEmpty()) {
                return new DenseLookup(EMPTY_INT_ARRAY, EMPTY_INT_MATRIX);
            }

            int[] keys = new int[input.size()];
            int position = 0;
            for (Integer key : input.keySet()) {
                if (key == null) {
                    throw new IllegalArgumentException(name + " must not contain null keys");
                }
                keys[position++] = key;
            }
            Arrays.sort(keys);

            int[][] values = new int[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                int[] domain = input.get(keys[i]);
                values[i] = Objects.requireNonNull(domain, name + " must not contain null domains");
            }
            return new DenseLookup(keys, values);
        }

        private static void requireStrictlyIncreasing(int[] keys, String name) {
            for (int i = 1; i < keys.length; i++) {
                if (keys[i] <= keys[i - 1]) {
                    throw new IllegalArgumentException(name + " must be strictly increasing");
                }
            }
        }

        private static void requireNonNullDomains(int[][] domains, String name) {
            for (int i = 0; i < domains.length; i++) {
                if (domains[i] == null) {
                    throw new IllegalArgumentException(name + " must not contain null at index " + i);
                }
            }
        }

        private record DenseLookup(int[] keys, int[][] values) {
        }
    }
}
