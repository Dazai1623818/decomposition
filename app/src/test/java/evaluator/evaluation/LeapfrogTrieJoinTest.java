package evaluator.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import evaluator.evaluation.LeapfrogJoin.JoinMode;
import evaluator.evaluation.LeapfrogJoin.JoinResult;
import evaluator.evaluation.Relation;
import evaluator.evaluation.Relation.RelationProjection;
import evaluator.util.Deadline;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LeapfrogTrieJoinTest {
    @Test
    void joinsRowsAndCountsAcrossRelations() {
        Relation r1 = relation("?x", "?y", new int[][] { { 1, 2 }, { 1, 3 }, { 2, 3 } });
        Relation r2 = relation("?y", "?z", new int[][] { { 2, 5 }, { 3, 6 } });

        List<Relation> relations = List.of(r1, r2);
        List<String> order = List.of("?x", "?y", "?z");

        JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                relations,
                order,
                order,
                JoinMode.PROJECTED_ROWS);
        JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                relations,
                order,
                order,
                JoinMode.PROJECTED_COUNT);

        Set<List<Integer>> expected = Set.of(
                List.of(1, 2, 5),
                List.of(1, 3, 6),
                List.of(2, 3, 6));
        assertEquals(expected, toTupleSet(rowsResult.rows(), order));
        assertEquals(expected.size(), countResult.count());
    }

    @Test
    void joinsProjectedRowsAndCounts() {
        Relation r1 = relation("?x", "?y", new int[][] { { 1, 2 }, { 1, 3 }, { 2, 3 } });
        Relation r2 = relation("?y", "?z", new int[][] { { 2, 5 }, { 3, 6 } });

        List<Relation> relations = List.of(r1, r2);
        List<String> order = List.of("?x", "?y", "?z");
        List<String> projected = List.of("?x", "?z");

        JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                relations,
                order,
                projected,
                JoinMode.PROJECTED_ROWS);
        JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                relations,
                order,
                projected,
                JoinMode.PROJECTED_COUNT);

        Set<List<Integer>> expected = Set.of(
                List.of(1, 5),
                List.of(1, 6),
                List.of(2, 6));
        assertEquals(expected, toTupleSet(rowsResult.rows(), projected));
        assertEquals(expected.size(), countResult.count());
    }

    @Test
    void projectedExistsStopsAtFirstDistinctProjectedAnswer() {
        Relation r1 = relation("?x", "?y", new int[][] { { 1, 2 }, { 1, 3 }, { 2, 3 } });
        Relation r2 = relation("?y", "?z", new int[][] { { 2, 5 }, { 3, 6 } });

        List<Relation> relations = List.of(r1, r2);
        List<String> order = List.of("?x", "?y", "?z");
        List<String> projected = List.of("?x", "?z");

        JoinResult.Count existsResult = (JoinResult.Count) LeapfrogJoin.join(
                relations,
                order,
                projected,
                JoinMode.PROJECTED_EXISTS);
        JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                relations,
                order,
                projected,
                JoinMode.PROJECTED_COUNT);

        assertEquals(1L, existsResult.count());
        assertTrue(countResult.count() > existsResult.count());
    }

    @Test
    void projectedEmptyVarsReturnsEmptyEvenWhenExtensionsExist() {
        Relation relation = Relation.unary("?x", "u", new int[] { 1 });

        JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                List.of(relation),
                List.of("?x"),
                List.of(),
                JoinMode.PROJECTED_ROWS);
        JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                List.of(relation),
                List.of("?x"),
                List.of(),
                JoinMode.PROJECTED_COUNT);

        assertTrue(rowsResult.rows().isEmpty());
        assertEquals(0L, countResult.count());
    }

    @Test
    void projectedEmptyVarsWithNoMatchesReturnsEmpty() {
        Relation relation = Relation.unary("?x", "u", new int[0]);

        JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                List.of(relation),
                List.of("?x"),
                List.of(),
                JoinMode.PROJECTED_ROWS);
        JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                List.of(relation),
                List.of("?x"),
                List.of(),
                JoinMode.PROJECTED_COUNT);

        assertTrue(rowsResult.rows().isEmpty());
        assertEquals(0L, countResult.count());
    }

    @Test
    void unprojectedVariablesStillConstrainProjectedResults() {
        Relation r1 = relation("?x", "?y", new int[][] { { 1, 2 }, { 1, 3 } });
        Relation r2 = relation("?y", "?z", new int[][] { { 2, 9 } });

        JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                List.of(r1, r2),
                List.of("?x", "?y", "?z"),
                List.of("?x"),
                JoinMode.PROJECTED_ROWS);
        JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                List.of(r1, r2),
                List.of("?x", "?y", "?z"),
                List.of("?x"),
                JoinMode.PROJECTED_COUNT);

        assertEquals(Set.of(List.of(1)), toTupleSet(rowsResult.rows(), List.of("?x")));
        assertEquals(1L, countResult.count());
    }

    @Test
    void rejectsDuplicateOrUnknownProjectedVariables() {
        Relation relation = Relation.unary("?x", "u", new int[] { 1 });

        assertThrows(IllegalArgumentException.class, () -> LeapfrogJoin.join(
                List.of(relation),
                List.of("?x"),
                List.of("?x", "?x"),
                JoinMode.PROJECTED_COUNT));

        assertThrows(IllegalArgumentException.class, () -> LeapfrogJoin.join(
                List.of(relation),
                List.of("?x"),
                List.of("?x", "?missing"),
                JoinMode.PROJECTED_COUNT));
    }

    @Test
    void joinRejectsExpiredDeadline() {
        Relation relation = relation("?x", "?y", new int[][] { { 1, 2 }, { 2, 3 } });

        assertThrows(Deadline.Exceeded.class, () -> LeapfrogJoin.join(
                List.of(relation),
                List.of("?x", "?y"),
                List.of("?x"),
                JoinMode.PROJECTED_COUNT,
                true,
                System.nanoTime() - 1));
    }

    @Test
    void intersectsMultipleUnaryConstraintsLikeNaiveIntersection() {
        Random random = new Random(0x5eed);
        for (int iteration = 0; iteration < 500; iteration++) {
            int constraintCount = 2 + random.nextInt(4); // 2..5 constraints
            List<Relation> relations = new ArrayList<>(constraintCount);
            List<int[]> domains = new ArrayList<>(constraintCount);
            for (int c = 0; c < constraintCount; c++) {
                int[] domain = randomSortedDistinctDomain(random);
                domains.add(domain);
                relations.add(Relation.unary("?x", "u" + c, domain));
            }

            Set<Integer> expected = intersect(domains);

            JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                    relations,
                    List.of("?x"),
                    List.of("?x"),
                    JoinMode.PROJECTED_ROWS);
            JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                    relations,
                    List.of("?x"),
                    List.of("?x"),
                    JoinMode.PROJECTED_COUNT);

            Set<Integer> actual = new HashSet<>();
            for (Map<String, Integer> row : rowsResult.rows()) {
                actual.add(row.get("?x"));
            }

            assertEquals(expected, actual);
            assertEquals(expected.size(), rowsResult.rows().size(), "expected no duplicate rows");
            assertEquals(expected.size(), countResult.count());
        }
    }

    @Test
    void exhaustiveThreeWayUnaryIntersectionMatchesNaive() {
        // Exhaustively validate 3-way intersections over a small universe to catch
        // subtle iterator bugs.
        int universeSize = 5; // values: 0..4
        int subsetCount = 1 << universeSize;
        for (int aMask = 0; aMask < subsetCount; aMask++) {
            int[] aDomain = domainFromMask(aMask, universeSize);
            Relation a = Relation.unary("?x", "a", aDomain);
            for (int bMask = 0; bMask < subsetCount; bMask++) {
                int[] bDomain = domainFromMask(bMask, universeSize);
                Relation b = Relation.unary("?x", "b", bDomain);
                for (int cMask = 0; cMask < subsetCount; cMask++) {
                    int[] cDomain = domainFromMask(cMask, universeSize);
                    Relation c = Relation.unary("?x", "c", cDomain);

                    Set<Integer> expected = intersect(List.of(aDomain, bDomain, cDomain));

                    JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                            List.of(a, b, c),
                            List.of("?x"),
                            List.of("?x"),
                            JoinMode.PROJECTED_ROWS);
                    JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                            List.of(a, b, c),
                            List.of("?x"),
                            List.of("?x"),
                            JoinMode.PROJECTED_COUNT);

                    Set<Integer> actual = new HashSet<>();
                    for (Map<String, Integer> row : rowsResult.rows()) {
                        actual.add(row.get("?x"));
                    }

                    assertEquals(expected, actual);
                    assertEquals(expected.size(), rowsResult.rows().size(), "expected no duplicate rows");
                    assertEquals(expected.size(), countResult.count());
                }
            }
        }
    }

    @Test
    void maxValueInDomainShouldNotMatchCursorAtEnd() {
        // If Integer.MAX_VALUE is a valid value, an ended cursor must not be treated as
        // producing it.
        // The correct intersection of these constraints is exactly {Integer.MAX_VALUE}.
        Relation a = Relation.unary("?x", "a", new int[] { 1, Integer.MAX_VALUE });
        Relation b = Relation.unary("?x", "b", new int[] { Integer.MAX_VALUE });

        JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                List.of(a, b),
                List.of("?x"),
                List.of("?x"),
                JoinMode.PROJECTED_ROWS);
        JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                List.of(a, b),
                List.of("?x"),
                List.of("?x"),
                JoinMode.PROJECTED_COUNT);

        assertEquals(1, rowsResult.rows().size(), "intersection should contain a single match");
        assertEquals(Integer.MAX_VALUE, rowsResult.rows().get(0).get("?x"));
        assertEquals(1L, countResult.count());
    }

    @Test
    void seekOnEmptyRangeDoesNotThrow() {
        // Regression test for claim that IntCursor.seek might throw when pos ==
        // data.length.
        // This exercises the internal seek() with an empty binarySearch range in the
        // current JDK.
        Relation a = Relation.unary("?x", "a", new int[] { 1, Integer.MAX_VALUE });
        Relation b = Relation.unary("?x", "b", new int[] { Integer.MAX_VALUE });

        assertDoesNotThrow(() -> LeapfrogJoin.join(
                List.of(a, b),
                List.of("?x"),
                List.of("?x"),
                JoinMode.PROJECTED_COUNT));
    }

    @Test
    void oracleRandomBinaryChainJoinMatchesNaiveAcrossOrdersAndProjections() {
        // Oracle test: naive full join enumeration + project(distinct) must match
        // LeapfrogTrieJoin PROJECTED_*.
        Random random = new Random(0xC0FFEE);
        int universeSize = 5;

        List<String> vars = List.of("?x", "?y", "?z");
        List<List<String>> orders = permutations(vars);
        List<List<String>> projections = List.of(
                List.of("?x", "?z"),
                List.of("?x"),
                vars);

        for (int iteration = 0; iteration < 60; iteration++) {
            boolean[][] r = randomBinaryRelation(random, universeSize, 0.35);
            boolean[][] s = randomBinaryRelation(random, universeSize, 0.35);
            List<BinaryConstraint> binaryConstraints = List.of(
                    new BinaryConstraint("?x", "?y", r),
                    new BinaryConstraint("?y", "?z", s));

            boolean addUnary = random.nextBoolean();
            boolean[] yDomain = addUnary ? randomUnaryDomain(random, universeSize, 0.5) : null;
            List<UnaryConstraint> unaryConstraints = addUnary
                    ? List.of(new UnaryConstraint("?y", yDomain))
                    : List.of();

            List<Relation> relations = new ArrayList<>(addUnary ? 3 : 2);
            relations.add(binaryBinding("?x", "?y", r));
            relations.add(binaryBinding("?y", "?z", s));
            if (addUnary) {
                relations.add(unaryBinding("?y", yDomain));
            }

            Map<List<String>, Set<List<Integer>>> expectedByProjection = new HashMap<>();
            for (List<String> projection : projections) {
                expectedByProjection.put(
                        projection,
                        naiveProjectedTuples(vars, unaryConstraints, binaryConstraints, projection, universeSize));
            }

            for (List<String> order : orders) {
                for (List<String> projection : projections) {
                    Set<List<Integer>> expected = expectedByProjection.get(projection);

                    JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                            relations,
                            order,
                            projection,
                            JoinMode.PROJECTED_ROWS);
                    JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                            relations,
                            order,
                            projection,
                            JoinMode.PROJECTED_COUNT);

                    Set<List<Integer>> actual = toTupleSet(rowsResult.rows(), projection);
                    assertEquals(expected, actual);
                    assertEquals(expected.size(), rowsResult.rows().size(), "expected no duplicate rows");
                    assertEquals(expected.size(), countResult.count());
                }
            }
        }
    }

    @Test
    void oracleRandomBinaryTriangleJoinMatchesNaiveAcrossOrdersAndProjections() {
        // Oracle test covering cyclic joins (triangles).
        Random random = new Random(0xBADC0DE);
        int universeSize = 5;

        List<String> vars = List.of("?x", "?y", "?z");
        List<List<String>> orders = permutations(vars);
        List<List<String>> projections = List.of(
                List.of("?x", "?z"),
                List.of("?y"),
                vars);

        for (int iteration = 0; iteration < 60; iteration++) {
            boolean[][] r = randomBinaryRelation(random, universeSize, 0.3);
            boolean[][] s = randomBinaryRelation(random, universeSize, 0.3);
            boolean[][] t = randomBinaryRelation(random, universeSize, 0.3);
            List<BinaryConstraint> binaryConstraints = List.of(
                    new BinaryConstraint("?x", "?y", r),
                    new BinaryConstraint("?y", "?z", s),
                    new BinaryConstraint("?z", "?x", t));

            List<UnaryConstraint> unaryConstraints = List.of();
            List<Relation> relations = List.of(
                    binaryBinding("?x", "?y", r),
                    binaryBinding("?y", "?z", s),
                    binaryBinding("?z", "?x", t));

            Map<List<String>, Set<List<Integer>>> expectedByProjection = new HashMap<>();
            for (List<String> projection : projections) {
                expectedByProjection.put(
                        projection,
                        naiveProjectedTuples(vars, unaryConstraints, binaryConstraints, projection, universeSize));
            }

            for (List<String> order : orders) {
                for (List<String> projection : projections) {
                    Set<List<Integer>> expected = expectedByProjection.get(projection);

                    JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                            relations,
                            order,
                            projection,
                            JoinMode.PROJECTED_ROWS);
                    JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                            relations,
                            order,
                            projection,
                            JoinMode.PROJECTED_COUNT);

                    Set<List<Integer>> actual = toTupleSet(rowsResult.rows(), projection);
                    assertEquals(expected, actual);
                    assertEquals(expected.size(), rowsResult.rows().size(), "expected no duplicate rows");
                    assertEquals(expected.size(), countResult.count());
                }
            }
        }
    }

    @Test
    void disconnectedComponentsProduceCartesianProductAcrossOrders() {
        // Regression test for disconnected components: results should be a cartesian
        // product.
        int universeSize = 3;

        boolean[][] r = new boolean[universeSize][universeSize];
        r[0][1] = true;
        r[1][1] = true;

        boolean[][] s = new boolean[universeSize][universeSize];
        s[0][0] = true;
        s[2][1] = true;

        List<String> vars = List.of("?x", "?y", "?z", "?w");
        List<List<String>> orders = List.of(
                vars,
                List.of("?z", "?w", "?x", "?y"),
                List.of("?y", "?x", "?w", "?z"));
        List<List<String>> projections = List.of(
                vars,
                List.of("?x", "?y"),
                List.of("?z", "?w"));

        List<BinaryConstraint> binaryConstraints = List.of(
                new BinaryConstraint("?x", "?y", r),
                new BinaryConstraint("?z", "?w", s));
        List<UnaryConstraint> unaryConstraints = List.of();
        List<Relation> relations = List.of(
                binaryBinding("?x", "?y", r),
                binaryBinding("?z", "?w", s));

        Map<List<String>, Set<List<Integer>>> expectedByProjection = new HashMap<>();
        for (List<String> projection : projections) {
            expectedByProjection.put(
                    projection,
                    naiveProjectedTuples(vars, unaryConstraints, binaryConstraints, projection, universeSize));
        }

        for (List<String> order : orders) {
            for (List<String> projection : projections) {
                Set<List<Integer>> expected = expectedByProjection.get(projection);

                JoinResult.Rows rowsResult = (JoinResult.Rows) LeapfrogJoin.join(
                        relations,
                        order,
                        projection,
                        JoinMode.PROJECTED_ROWS);
                JoinResult.Count countResult = (JoinResult.Count) LeapfrogJoin.join(
                        relations,
                        order,
                        projection,
                        JoinMode.PROJECTED_COUNT);

                Set<List<Integer>> actual = toTupleSet(rowsResult.rows(), projection);
                assertEquals(expected, actual);
                assertEquals(expected.size(), rowsResult.rows().size(), "expected no duplicate rows");
                assertEquals(expected.size(), countResult.count());
            }
        }
    }

    private static Relation relation(String sourceVar, String targetVar, int[][] pairs) {
        Map<Integer, List<Integer>> forwardList = new HashMap<>();
        Map<Integer, List<Integer>> reverseList = new HashMap<>();
        for (int[] pair : pairs) {
            forwardList.computeIfAbsent(pair[0], ignored -> new ArrayList<>()).add(pair[1]);
            reverseList.computeIfAbsent(pair[1], ignored -> new ArrayList<>()).add(pair[0]);
        }

        Map<Integer, int[]> forward = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : forwardList.entrySet()) {
            int[] values = entry.getValue().stream().mapToInt(Integer::intValue).toArray();
            Arrays.sort(values);
            forward.put(entry.getKey(), values);
        }

        Map<Integer, int[]> reverse = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : reverseList.entrySet()) {
            int[] values = entry.getValue().stream().mapToInt(Integer::intValue).toArray();
            Arrays.sort(values);
            reverse.put(entry.getKey(), values);
        }

        int[] allSources = sortedKeys(forward.keySet());
        int[] allTargets = sortedKeys(reverse.keySet());
        RelationProjection projection = new RelationProjection(allSources, allTargets, forward, reverse);
        return Relation.binary(sourceVar, targetVar, sourceVar + "->" + targetVar, projection);
    }

    private static int[] sortedKeys(Set<Integer> keys) {
        int[] out = keys.stream().mapToInt(Integer::intValue).toArray();
        Arrays.sort(out);
        return out;
    }

    private static Set<List<Integer>> toTupleSet(List<Map<String, Integer>> rows, List<String> order) {
        Set<List<Integer>> out = new HashSet<>();
        for (Map<String, Integer> row : rows) {
            List<Integer> tuple = new ArrayList<>(order.size());
            for (String key : order) {
                tuple.add(row.get(key));
            }
            out.add(tuple);
        }
        return out;
    }

    private static int[] randomSortedDistinctDomain(Random random) {
        int len = random.nextInt(12); // 0..11
        Set<Integer> values = new HashSet<>();
        while (values.size() < len) {
            // Keep values comfortably away from Integer.MAX_VALUE to avoid sentinel
            // collisions.
            values.add(random.nextInt(50));
        }
        int[] domain = values.stream().mapToInt(Integer::intValue).toArray();
        Arrays.sort(domain);
        return domain;
    }

    private static Set<Integer> intersect(List<int[]> domains) {
        Set<Integer> current = null;
        for (int[] domain : domains) {
            Set<Integer> asSet = new HashSet<>();
            for (int value : domain) {
                asSet.add(value);
            }
            if (current == null) {
                current = asSet;
            } else {
                current.retainAll(asSet);
            }
        }
        return current == null ? Set.of() : current;
    }

    private static int[] domainFromMask(int mask, int universeSize) {
        int[] tmp = new int[Integer.bitCount(mask)];
        int outIndex = 0;
        for (int value = 0; value < universeSize; value++) {
            if ((mask & (1 << value)) != 0) {
                tmp[outIndex++] = value;
            }
        }
        return tmp;
    }

    private record UnaryConstraint(String var, boolean[] allowed) {
    }

    private record BinaryConstraint(String left, String right, boolean[][] allowed) {
    }

    private static Set<List<Integer>> naiveProjectedTuples(
            List<String> variables,
            List<UnaryConstraint> unaryConstraints,
            List<BinaryConstraint> binaryConstraints,
            List<String> projectedVars,
            int universeSize) {
        Map<String, Integer> indexByVar = new HashMap<>(variables.size());
        for (int i = 0; i < variables.size(); i++) {
            indexByVar.put(variables.get(i), i);
        }
        int[] assignment = new int[variables.size()];
        Set<List<Integer>> out = new HashSet<>();
        enumerateAssignments(0, universeSize, assignment, indexByVar, unaryConstraints, binaryConstraints,
                projectedVars, out);
        return out;
    }

    private static void enumerateAssignments(
            int depth,
            int universeSize,
            int[] assignment,
            Map<String, Integer> indexByVar,
            List<UnaryConstraint> unaryConstraints,
            List<BinaryConstraint> binaryConstraints,
            List<String> projectedVars,
            Set<List<Integer>> out) {
        if (depth == assignment.length) {
            if (satisfiesConstraints(assignment, indexByVar, unaryConstraints, binaryConstraints)) {
                out.add(projectTuple(assignment, indexByVar, projectedVars));
            }
            return;
        }
        for (int value = 0; value < universeSize; value++) {
            assignment[depth] = value;
            enumerateAssignments(
                    depth + 1,
                    universeSize,
                    assignment,
                    indexByVar,
                    unaryConstraints,
                    binaryConstraints,
                    projectedVars,
                    out);
        }
    }

    private static boolean satisfiesConstraints(
            int[] assignment,
            Map<String, Integer> indexByVar,
            List<UnaryConstraint> unaryConstraints,
            List<BinaryConstraint> binaryConstraints) {
        for (UnaryConstraint constraint : unaryConstraints) {
            int idx = Objects.requireNonNull(indexByVar.get(constraint.var()), "unknown var " + constraint.var());
            if (!constraint.allowed()[assignment[idx]]) {
                return false;
            }
        }
        for (BinaryConstraint constraint : binaryConstraints) {
            int left = Objects.requireNonNull(indexByVar.get(constraint.left()), "unknown var " + constraint.left());
            int right = Objects.requireNonNull(indexByVar.get(constraint.right()), "unknown var " + constraint.right());
            if (!constraint.allowed()[assignment[left]][assignment[right]]) {
                return false;
            }
        }
        return true;
    }

    private static List<Integer> projectTuple(int[] assignment, Map<String, Integer> indexByVar,
            List<String> projectedVars) {
        List<Integer> tuple = new ArrayList<>(projectedVars.size());
        for (String var : projectedVars) {
            int idx = Objects.requireNonNull(indexByVar.get(var), "unknown var " + var);
            tuple.add(assignment[idx]);
        }
        return tuple;
    }

    private static Relation binaryBinding(String left, String right, boolean[][] allowed) {
        List<int[]> pairs = new ArrayList<>();
        for (int src = 0; src < allowed.length; src++) {
            for (int trg = 0; trg < allowed[src].length; trg++) {
                if (allowed[src][trg]) {
                    pairs.add(new int[] { src, trg });
                }
            }
        }
        return relation(left, right, pairs.toArray(new int[0][]));
    }

    private static Relation unaryBinding(String var, boolean[] allowed) {
        int count = 0;
        for (boolean value : allowed) {
            if (value) {
                count++;
            }
        }
        int[] domain = new int[count];
        int outIndex = 0;
        for (int value = 0; value < allowed.length; value++) {
            if (allowed[value]) {
                domain[outIndex++] = value;
            }
        }
        return Relation.unary(var, "unary(" + var + ")", domain);
    }

    private static boolean[][] randomBinaryRelation(Random random, int universeSize, double density) {
        boolean[][] allowed = new boolean[universeSize][universeSize];
        for (int src = 0; src < universeSize; src++) {
            for (int trg = 0; trg < universeSize; trg++) {
                allowed[src][trg] = random.nextDouble() < density;
            }
        }
        return allowed;
    }

    private static boolean[] randomUnaryDomain(Random random, int universeSize, double density) {
        boolean[] allowed = new boolean[universeSize];
        for (int value = 0; value < universeSize; value++) {
            allowed[value] = random.nextDouble() < density;
        }
        return allowed;
    }

    private static List<List<String>> permutations(List<String> input) {
        List<List<String>> out = new ArrayList<>();
        permute(new ArrayList<>(input), 0, out);
        return out;
    }

    private static void permute(List<String> values, int start, List<List<String>> out) {
        if (start == values.size()) {
            out.add(List.copyOf(values));
            return;
        }
        for (int i = start; i < values.size(); i++) {
            swap(values, start, i);
            permute(values, start + 1, out);
            swap(values, start, i);
        }
    }

    private static void swap(List<String> values, int i, int j) {
        if (i == j) {
            return;
        }
        String tmp = values.get(i);
        values.set(i, values.get(j));
        values.set(j, tmp);
    }
}
