package evaluator.joiner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import dev.roanh.gmark.data.SourceTargetPair;
import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.eval.DatabaseGraph;
import dev.roanh.gmark.eval.PathQuery;
import dev.roanh.gmark.eval.ReachabilityQueryEvaluator;
import dev.roanh.gmark.eval.ResultGraph;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.EdgeCPQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.IntGraph;
import evaluator.cpq.ConjunctiveQuery;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.Plan;
import evaluator.evaluation.LeapfrogJoin;
import evaluator.evaluation.Relation;
import evaluator.evaluation.Relation.RelationProjection;
import java.io.BufferedReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.junit.jupiter.api.Test;

class JoinerTest {
    @Test
    void joinerMatchesExpectedGmarkCases() throws Exception {
        Path baseDir = Path.of("src/test/java/evaluator/joiner/gmark-testcases");
        ExpectedRoot expected = readExpected(baseDir.resolve("expected/cpq_expected.json"));

        assertNotNull(expected, "missing expected root");
        assertNotNull(expected.datasets, "missing datasets");
        for (ExpectedDataset dataset : expected.datasets) {
            Path graphFile = resolveGraph(baseDir, dataset.graph);
            GraphIndex index = GraphIndex.load(graphFile);
            List<QueryLine> queryLines = readQueryLines(baseDir.resolve(dataset.queryFile));

            for (ExpectedCase testCase : dataset.cases) {
                QuerySpec spec = buildQuerySpec(testCase, queryLines);
                QueryResult result = evaluateQuery(index, spec);
                assertMatchesExpected(dataset.name, testCase, spec, result);
            }
        }
    }

    private static void assertMatchesExpected(
            String datasetName,
            ExpectedCase testCase,
            QuerySpec spec,
            QueryResult result) {
        String label = datasetName + ":" + testCase.name + " (" + spec.description + ")";

        if (testCase.expectedPairs != null) {
            Set<IntPair> expectedPairs = toPairSet(testCase.expectedPairs);
            assertEquals(expectedPairs, result.pairs, label + " pairs");
        }
        if (testCase.expectedCardStat != null) {
            int[] expected = testCase.expectedCardStat;
            assertEquals(expected[0], result.sourceCount, label + " sourceCount");
            assertEquals(expected[1], result.pairCount, label + " pairCount");
            assertEquals(expected[2], result.targetCount, label + " targetCount");
        }
    }

    private static QueryResult evaluateQuery(GraphIndex index, QuerySpec spec) {
        if (spec.cpq.getDiameter() == 0) {
            return evaluateIdentity(index, spec);
        }
        ConjunctiveQuery cq = ConjunctiveQuery.from(spec.cpq.toCQ());
        Plan decomposition = cq.decomposeSingleEdge();
        List<Relation> relations = new ArrayList<>();

        for (Component component : decomposition.components()) {
            Relation binding = evaluateComponent(index, component);
            if (binding == null) {
                return QueryResult.empty();
            }
            relations.add(binding);
        }

        VarPair vars = resolveVars(cq.freeVariables());
        String srcVar = vars.source;
        String trgVar = vars.target;
        if (spec.source != null) {
            relations.add(Relation.unary(srcVar, "source", new int[] { spec.source }));
        }
        if (spec.target != null) {
            relations.add(Relation.unary(trgVar, "target", new int[] { spec.target }));
        }

        if (relations.isEmpty()) {
            relations.add(identityRelation(srcVar, trgVar, index.nodeCount));
        }

        List<String> order = decomposition.variableOrder();
        if (order.isEmpty()) {
            order = srcVar.equals(trgVar) ? List.of(srcVar) : List.of(srcVar, trgVar);
        }

        List<String> projected = srcVar.equals(trgVar) ? List.of(srcVar) : List.of(srcVar, trgVar);
        LeapfrogJoin.JoinResult.Rows rowsResult = (LeapfrogJoin.JoinResult.Rows) LeapfrogJoin.join(
                relations,
                order,
                projected,
                LeapfrogJoin.JoinMode.PROJECTED_ROWS);
        LeapfrogJoin.JoinResult.Count countResult = (LeapfrogJoin.JoinResult.Count) LeapfrogJoin.join(
                relations,
                order,
                projected,
                LeapfrogJoin.JoinMode.PROJECTED_COUNT);
        List<Map<String, Integer>> rows = rowsResult.rows();
        long count = countResult.count();

        assertEquals((long) rows.size(), count, "joinCount mismatch");
        return QueryResult.fromRows(rows, srcVar, trgVar);
    }

    private static QueryResult evaluateIdentity(GraphIndex index, QuerySpec spec) {
        ReachabilityQueryEvaluator evaluator = new ReachabilityQueryEvaluator(index.identityGraph);
        PathQuery query = buildPathQuery(spec);
        ResultGraph result = evaluator.evaluate(query);
        return QueryResult.fromResultGraph(result);
    }

    private static PathQuery buildPathQuery(QuerySpec spec) {
        Integer source = spec.source;
        Integer target = spec.target;
        if (source != null && target != null) {
            return PathQuery.of(source, spec.cpq, target);
        }
        if (source != null) {
            return PathQuery.of(source, spec.cpq);
        }
        if (target != null) {
            return PathQuery.of(spec.cpq, target);
        }
        return PathQuery.of(spec.cpq);
    }

    private static Relation evaluateComponent(GraphIndex index, Component component) {
        Predicate label = extractEdgeLabel(component);
        int labelId = parseLabelId(label);
        List<Edge> edges = index.edgesByLabel.getOrDefault(labelId, List.of());
        if (edges.isEmpty()) {
            return null;
        }

        String left = Plan.varName(component.s());
        String right = Plan.varName(component.t());
        boolean inverse = label.isInverse();
        String description = label.getAlias();

        if (left.equals(right)) {
            Set<Integer> values = new HashSet<>();
            for (Edge e : edges) {
                int src = inverse ? e.target : e.source;
                int trg = inverse ? e.source : e.target;
                if (src == trg) {
                    values.add(src);
                }
            }
            if (values.isEmpty()) {
                return null;
            }
            int[] domain = values.stream().mapToInt(Integer::intValue).sorted().toArray();
            return Relation.unary(left, description, domain);
        }

        Map<Integer, IntAccumulator> forward = new HashMap<>();
        Map<Integer, IntAccumulator> reverse = new HashMap<>();
        for (Edge e : edges) {
            int src = inverse ? e.target : e.source;
            int trg = inverse ? e.source : e.target;
            forward.computeIfAbsent(src, ignored -> new IntAccumulator()).add(trg);
            reverse.computeIfAbsent(trg, ignored -> new IntAccumulator()).add(src);
        }

        RelationProjection projection = new RelationProjection(
                sortedKeys(forward.keySet()),
                sortedKeys(reverse.keySet()),
                toIntArrayMap(forward),
                toIntArrayMap(reverse));

        if (projection.isEmpty()) {
            return null;
        }
        return Relation.binary(left, right, description, projection);
    }

    private static Predicate extractEdgeLabel(Component component) {
        CPQ cpq = component.cpq();
        if (cpq instanceof EdgeCPQ edge) {
            return edge.getLabel();
        }

        QueryTree tree = cpq.toAbstractSyntaxTree();
        List<QueryTree> parts = new ArrayList<>();
        collectIntersectionParts(tree, parts);
        Predicate label = null;
        for (QueryTree part : parts) {
            if (part.getOperation() == OperationType.EDGE) {
                if (label != null) {
                    throw new IllegalStateException("Unsupported CPQ component " + component.normalized());
                }
                label = part.getEdgeAtom().getLabel();
                continue;
            }
            if (part.getOperation() == OperationType.IDENTITY) {
                continue;
            }
            throw new IllegalStateException("Unsupported CPQ component " + component.normalized());
        }

        if (label == null) {
            throw new IllegalStateException("Unsupported CPQ component " + component.normalized());
        }
        return label;
    }

    private static void collectIntersectionParts(QueryTree node, List<QueryTree> out) {
        if (node.getOperation() == OperationType.INTERSECTION) {
            collectIntersectionParts(node.getOperand(0), out);
            collectIntersectionParts(node.getOperand(1), out);
            return;
        }
        out.add(node);
    }

    private static Relation identityRelation(String srcVar, String trgVar, int nodeCount) {
        if (srcVar.equals(trgVar)) {
            int[] nodes = new int[nodeCount];
            for (int i = 0; i < nodeCount; i++) {
                nodes[i] = i;
            }
            return Relation.unary(srcVar, "id", nodes);
        }
        int[] nodes = new int[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            nodes[i] = i;
        }
        Map<Integer, int[]> forward = new HashMap<>(nodeCount);
        Map<Integer, int[]> reverse = new HashMap<>(nodeCount);
        for (int node : nodes) {
            forward.put(node, new int[] { node });
            reverse.put(node, new int[] { node });
        }
        RelationProjection projection = new RelationProjection(nodes, nodes, forward, reverse);
        return Relation.binary(srcVar, trgVar, "id", projection);
    }

    private static int[] sortedKeys(Set<Integer> keys) {
        if (keys.isEmpty()) {
            return new int[0];
        }
        int[] sorted = keys.stream().mapToInt(Integer::intValue).toArray();
        Arrays.sort(sorted);
        return sorted;
    }

    private static Map<Integer, int[]> toIntArrayMap(Map<Integer, IntAccumulator> input) {
        Map<Integer, int[]> result = new HashMap<>(input.size());
        for (Map.Entry<Integer, IntAccumulator> entry : input.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toSortedDistinctArray());
        }
        return result;
    }

    private static VarPair resolveVars(Set<VarCQ> vars) {
        VarCQ src = null;
        VarCQ trg = null;
        VarCQ only = null;
        for (VarCQ v : vars) {
            String name = v.getName();
            if ("src".equals(name)) {
                src = v;
            } else if ("trg".equals(name)) {
                trg = v;
            }
            only = v;
        }
        if (src != null && trg != null) {
            return new VarPair(Plan.varName(src), Plan.varName(trg));
        }
        if (vars.size() == 1 && only != null) {
            String var = Plan.varName(only);
            return new VarPair(var, var);
        }
        List<String> names = vars.stream().map(VarCQ::getName).sorted().toList();
        if (names.size() >= 2) {
            return new VarPair("?" + names.get(0), "?" + names.get(1));
        }
        throw new IllegalStateException("Missing free variables");
    }

    private static QuerySpec buildQuerySpec(ExpectedCase testCase, List<QueryLine> queryLines) {
        Integer source = testCase.source;
        Integer target = testCase.target;
        String description = testCase.name;

        if (testCase.queryLine != null) {
            QueryLine line = queryLines.get(testCase.queryLine - 1);
            source = source == null ? line.source : source;
            target = target == null ? line.target : target;
            return new QuerySpec(CPQ.parse(line.query), source, target, line.query);
        }

        QueryExpr expr = Objects.requireNonNull(testCase.queryExpr, "query_expr");
        return new QuerySpec(parseQueryExpr(expr), source, target, expr.type);
    }

    private static CPQ parseQueryExpr(QueryExpr expr) {
        return switch (expr.type) {
            case "id" -> CPQ.id();
            case "label" -> {
                Predicate predicate = new Predicate(expr.id, String.valueOf(expr.id));
                if (Boolean.TRUE.equals(expr.inverse)) {
                    predicate = predicate.getInverse();
                }
                yield CPQ.label(predicate);
            }
            default -> throw new IllegalArgumentException("Unsupported query_expr type " + expr.type);
        };
    }

    private static Set<IntPair> toPairSet(List<List<Integer>> pairs) {
        Set<IntPair> out = new HashSet<>();
        for (List<Integer> pair : pairs) {
            if (pair.size() != 2) {
                throw new IllegalArgumentException("Expected pair of size 2");
            }
            out.add(new IntPair(pair.get(0), pair.get(1)));
        }
        return out;
    }

    private static ExpectedRoot readExpected(Path expectedFile) throws Exception {
        Gson gson = new Gson();
        try (Reader reader = Files.newBufferedReader(expectedFile)) {
            return gson.fromJson(reader, ExpectedRoot.class);
        }
    }

    private static Path resolveGraph(Path baseDir, String graphPath) {
        Path path = baseDir.resolve(graphPath);
        if (Files.exists(path)) {
            return path;
        }
        if (graphPath.startsWith("graphs/")) {
            Path fallback = baseDir.resolve(graphPath.replaceFirst("graphs/", "examplegraphs/"));
            if (Files.exists(fallback)) {
                return fallback;
            }
        }
        throw new IllegalArgumentException("Missing graph file " + graphPath);
    }

    private static List<QueryLine> readQueryLines(Path queryFile) throws Exception {
        List<QueryLine> out = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(queryFile)) {
            for (String line; (line = reader.readLine()) != null;) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                String[] parts = trimmed.split("\\s*,\\s*", 3);
                if (parts.length != 3) {
                    throw new IllegalArgumentException("Invalid query line: " + line);
                }
                Integer source = parseOptionalInt(parts[0]);
                String query = parts[1].trim();
                Integer target = parseOptionalInt(parts[2]);
                out.add(new QueryLine(query, source, target));
            }
        }
        return out;
    }

    private static Integer parseOptionalInt(String text) {
        String trimmed = text.trim();
        if (trimmed.equals("*")) {
            return null;
        }
        return Integer.parseInt(trimmed);
    }

    private record QuerySpec(CPQ cpq, Integer source, Integer target, String description) {
    }

    private record VarPair(String source, String target) {
    }

    private static int parseLabelId(Predicate label) {
        String alias = label.getAlias();
        int value = 0;
        boolean found = false;
        for (int i = 0; i < alias.length(); i++) {
            char c = alias.charAt(i);
            if (c < '0' || c > '9') {
                break;
            }
            found = true;
            value = value * 10 + (c - '0');
        }
        return found ? value : label.getID();
    }

    private record QueryLine(String query, Integer source, Integer target) {
    }

    private record IntPair(int source, int target) {
    }

    private record QueryResult(Set<IntPair> pairs, int sourceCount, int pairCount, int targetCount) {
        static QueryResult empty() {
            return new QueryResult(Set.of(), 0, 0, 0);
        }

        static QueryResult fromResultGraph(ResultGraph result) {
            Set<IntPair> pairs = new HashSet<>();
            for (SourceTargetPair pair : result.getSourceTargetPairs()) {
                pairs.add(new IntPair(pair.source(), pair.target()));
            }
            Set<Integer> sources = new HashSet<>();
            Set<Integer> targets = new HashSet<>();
            for (IntPair pair : pairs) {
                sources.add(pair.source);
                targets.add(pair.target);
            }
            return new QueryResult(pairs, sources.size(), pairs.size(), targets.size());
        }

        static QueryResult fromRows(List<Map<String, Integer>> rows, String srcVar, String trgVar) {
            Set<IntPair> pairs = new HashSet<>();
            for (Map<String, Integer> row : rows) {
                Integer src = row.get(srcVar);
                Integer trg = row.get(trgVar);
                if (src != null && trg != null) {
                    pairs.add(new IntPair(src, trg));
                }
            }
            Set<Integer> sources = new HashSet<>();
            Set<Integer> targets = new HashSet<>();
            for (IntPair pair : pairs) {
                sources.add(pair.source);
                targets.add(pair.target);
            }
            return new QueryResult(pairs, sources.size(), pairs.size(), targets.size());
        }
    }

    private record GraphIndex(int nodeCount, Map<Integer, List<Edge>> edgesByLabel, DatabaseGraph identityGraph) {
        static GraphIndex load(Path file) throws Exception {
            try (BufferedReader reader = Files.newBufferedReader(file)) {
                String header = reader.readLine();
                if (header == null) {
                    throw new IllegalArgumentException("Empty graph file " + file);
                }
                String[] headerParts = header.trim().split("\\s+");
                if (headerParts.length < 3) {
                    throw new IllegalArgumentException("Invalid graph header " + header);
                }
                int nodeCount = Integer.parseInt(headerParts[0]);
                int labelCount = Integer.parseInt(headerParts[2]);
                Map<Integer, List<Edge>> edgesByLabel = new HashMap<>();
                for (String line; (line = reader.readLine()) != null;) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                        continue;
                    }
                    String[] parts = trimmed.split("\\s+");
                    if (parts.length < 3) {
                        continue;
                    }
                    int source = Integer.parseInt(parts[0]);
                    int target = Integer.parseInt(parts[1]);
                    int label = Integer.parseInt(parts[2]);
                    edgesByLabel.computeIfAbsent(label, ignored -> new ArrayList<>())
                            .add(new Edge(source, target));
                }
                DatabaseGraph identityGraph = new DatabaseGraph(new IntGraph(nodeCount, labelCount));
                return new GraphIndex(nodeCount, edgesByLabel, identityGraph);
            }
        }
    }

    private record Edge(int source, int target) {
    }

    private static final class IntAccumulator {
        private int[] data = new int[8];
        private int size = 0;

        void add(int value) {
            if (size == data.length) {
                data = Arrays.copyOf(data, Math.max(8, data.length * 2));
            }
            data[size++] = value;
        }

        int[] toSortedDistinctArray() {
            if (size == 0) {
                return new int[0];
            }
            int[] out = Arrays.copyOf(data, size);
            Arrays.sort(out);
            int unique = 1;
            for (int i = 1; i < out.length; i++) {
                if (out[i] != out[unique - 1]) {
                    out[unique++] = out[i];
                }
            }
            return unique == out.length ? out : Arrays.copyOf(out, unique);
        }
    }

    private static final class ExpectedRoot {
        List<ExpectedDataset> datasets;
    }

    private static final class ExpectedDataset {
        String name;
        String graph;
        @SerializedName("query_file")
        String queryFile;
        List<ExpectedCase> cases;
    }

    private static final class ExpectedCase {
        String name;
        @SerializedName("query_line")
        Integer queryLine;
        @SerializedName("query_expr")
        QueryExpr queryExpr;
        Integer source;
        Integer target;
        @SerializedName("expected_card_stat")
        int[] expectedCardStat;
        @SerializedName("expected_pairs")
        List<List<Integer>> expectedPairs;
    }

    private static final class QueryExpr {
        String type;
        Integer id;
        Boolean inverse;
    }
}
