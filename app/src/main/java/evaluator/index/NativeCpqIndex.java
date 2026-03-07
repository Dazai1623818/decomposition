package evaluator.index;

import dev.roanh.cpqindex.CanonForm;
import dev.roanh.cpqindex.CanonForm.CoreHash;
import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.Pair;
import dev.roanh.gmark.ast.OperationType;
import dev.roanh.gmark.ast.QueryTree;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.type.schema.Predicate;
import evaluator.cpq.Plan;
import evaluator.cpq.Plan.Component;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public final class NativeCpqIndex implements CpqIndex {
    static {
        try {
            Main.loadNatives();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final String LABELS_TOKEN = "labels=";
    private static final LabelCoverage ALL_LABELS = new AllLabelCoverage();
    private static final double OVERLAP_WEIGHT = 4.0D;
    private static final double FANOUT_CAP = 1.0E6D;
    private static final double STRUCTURAL_INTERSECTION_WEIGHT = 0.20D;
    private static final double STRUCTURAL_CONCAT_WEIGHT = 0.10D;
    private static final double LABEL_REPEAT_WEIGHT = 0.20D;
    private final List<Entry> entries;
    private final int k;
    private final int maxIntersections;
    private final Map<CPQ, EndpointStats> endpointStatsCache = new ConcurrentHashMap<>();
    private final Map<CPQ, StructuralStats> structuralStatsCache = new ConcurrentHashMap<>();
    private final Map<Path, OverlapSynopsis> overlapSynopsisCache = new ConcurrentHashMap<>();

    private NativeCpqIndex(List<Entry> entries, int k, int maxIntersections) {
        this.entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        if (this.entries.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty");
        }
        this.k = k;
        this.maxIntersections = maxIntersections;
    }

    /**
     * Loads an index from a single file or all index files within a directory.
     * Index label subsets are inferred from the filename convention (labels=...).
     * @param savedIndexPath File or directory containing index files.
     * @return Loaded CPQ-native index wrapper.
     * @throws Exception When loading fails.
     */
    public static NativeCpqIndex load(Path savedIndexFile) throws Exception {
        Objects.requireNonNull(savedIndexFile, "savedIndexFile");
        List<Path> indexFiles = listIndexFiles(savedIndexFile);
        List<Entry> entries = new ArrayList<>(indexFiles.size());
        Integer k = null;
        Integer maxIntersections = null;
        for (Path file : indexFiles) {
            try (InputStream in = Files.newInputStream(file)) {
                Index index = new Index(in);
                int nextK = index.getK();
                int nextIntersections = index.getIntersections();
                if (k == null) {
                    k = nextK;
                    maxIntersections = nextIntersections;
                } else if (k != nextK || maxIntersections != nextIntersections) {
                    throw new IllegalArgumentException("Mismatched k or intersections in " + file);
                }
                LabelCoverage coverage = parseLabelCoverage(file.getFileName().toString());
                entries.add(new Entry(index, coverage, file));
            }
        }
        if (k == null || maxIntersections == null) {
            throw new IllegalArgumentException("No index files found in " + savedIndexFile);
        }
        return new NativeCpqIndex(entries, k, maxIntersections);
    }

    @Override
    public int k() {
        return k;
    }

    @Override
    public int intersections() {
        return maxIntersections;
    }

    @Override
    public List<Edge> query(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        Entry entry = selectEntry(cpq);
        List<Pair> pairs = entry.index().query(cpq);
        List<Edge> edges = new ArrayList<>(pairs.size());
        for (Pair pair : pairs) {
            edges.add(new Edge(pair.getSource(), pair.getTarget()));
        }
        return edges;
    }

    @Override
    public long cost(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        BitSet needed = labelsFor(cpq);
        long best = Long.MAX_VALUE;
        for (Entry entry : entries) {
            if (!entry.coverage().covers(needed)) {
                continue;
            }
            long cost = entry.index().cost(cpq);
            if (cost < best) {
                best = cost;
            }
        }
        return best;
    }

    @Override
    public double overlapScore(Plan plan) {
        Objects.requireNonNull(plan, "plan");
        if (plan.components().isEmpty()) {
            return 0.0D;
        }

        double score = 0.0D;
        Map<String, List<EndpointView>> byVariable = new HashMap<>();
        for (Component component : plan.components()) {
            EndpointStats endpointStats = endpointStats(component.cpq());
            double cardinality = Math.max(1.0D, endpointStats.cardinality());
            double sourceNdv = clampNdv(endpointStats.sourceNdv(), cardinality);
            double targetNdv = clampNdv(endpointStats.targetNdv(), cardinality);
            score += Math.log1p(cardinality);

            addEndpoint(byVariable, component.sourceVarName(), sourceNdv, cardinality);
            if (!component.sourceVarName().equals(component.targetVarName())) {
                addEndpoint(byVariable, component.targetVarName(), targetNdv, cardinality);
            }

            StructuralStats structuralStats = structuralStats(component.cpq());
            score += STRUCTURAL_INTERSECTION_WEIGHT * structuralStats.intersections();
            score += STRUCTURAL_CONCAT_WEIGHT * structuralStats.concatenations();
            if (structuralStats.edgeCount() > 0) {
                double repeatRatio = (double) structuralStats.edgeCount() / Math.max(1.0D, structuralStats.distinctLabels());
                score += LABEL_REPEAT_WEIGHT * Math.max(0.0D, repeatRatio - 1.0D);
            }
        }

        for (List<EndpointView> endpoints : byVariable.values()) {
            if (endpoints.size() < 2) {
                continue;
            }
            double pairRisk = 0.0D;
            for (int i = 0; i < endpoints.size(); i++) {
                EndpointView left = endpoints.get(i);
                for (int j = i + 1; j < endpoints.size(); j++) {
                    EndpointView right = endpoints.get(j);
                    pairRisk += 1.0D / Math.max(left.ndv(), right.ndv());
                }
            }

            double fanout = 1.0D;
            for (EndpointView endpoint : endpoints) {
                fanout *= Math.min(FANOUT_CAP, Math.max(1.0D, endpoint.fanout()));
                if (fanout >= FANOUT_CAP) {
                    fanout = FANOUT_CAP;
                    break;
                }
            }
            score += OVERLAP_WEIGHT * Math.log1p(pairRisk * fanout);
        }

        return score;
    }

    @Override
    public double overlapJoinScore(Plan plan) {
        Objects.requireNonNull(plan, "plan");
        if (plan.components().isEmpty()) {
            return 0.0D;
        }

        double score = 0.0D;
        Map<String, List<EndpointView>> byVariable = new HashMap<>();
        for (Component component : plan.components()) {
            EndpointStats endpointStats = endpointStats(component.cpq());
            double cardinality = Math.max(1.0D, endpointStats.cardinality());
            double sourceNdv = clampNdv(endpointStats.sourceNdv(), cardinality);
            double targetNdv = clampNdv(endpointStats.targetNdv(), cardinality);

            addEndpoint(byVariable, component.sourceVarName(), sourceNdv, cardinality);
            if (!component.sourceVarName().equals(component.targetVarName())) {
                addEndpoint(byVariable, component.targetVarName(), targetNdv, cardinality);
            }
        }

        for (List<EndpointView> endpoints : byVariable.values()) {
            if (endpoints.size() < 2) {
                continue;
            }
            double pairRisk = 0.0D;
            for (int i = 0; i < endpoints.size(); i++) {
                EndpointView left = endpoints.get(i);
                for (int j = i + 1; j < endpoints.size(); j++) {
                    EndpointView right = endpoints.get(j);
                    pairRisk += 1.0D / Math.max(left.ndv(), right.ndv());
                }
            }

            double fanout = 1.0D;
            for (EndpointView endpoint : endpoints) {
                fanout *= Math.min(FANOUT_CAP, Math.max(1.0D, endpoint.fanout()));
                if (fanout >= FANOUT_CAP) {
                    fanout = FANOUT_CAP;
                    break;
                }
            }
            score += Math.log1p(pairRisk * fanout);
        }
        return score;
    }

    /**
     * Checks whether the given label set is covered by at least one loaded index.
     * @param labels Bitset of base label IDs used by a component.
     * @return True if any index label subset covers all labels.
     */
    public boolean coversLabels(BitSet labels) {
        Objects.requireNonNull(labels, "labels");
        for (Entry entry : entries) {
            if (entry.coverage().covers(labels)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks whether any loaded index can answer the given CPQ based on its labels and diameter.
     * @param cpq The CPQ to check.
     * @return True if at least one index label subset covers the CPQ labels.
     */
    @Override
    public boolean isIndexable(CPQ cpq) {
        Objects.requireNonNull(cpq, "cpq");
        if (cpq.getDiameter() > k) {
            return false;
        }
        BitSet needed = labelsFor(cpq);
        for (Entry entry : entries) {
            if (entry.coverage().covers(needed)) {
                return true;
            }
        }
        return false;
    }

    private Entry selectEntry(CPQ cpq) {
        BitSet needed = labelsFor(cpq);
        Entry bestEntry = null;
        long bestCost = Long.MAX_VALUE;
        for (Entry entry : entries) {
            if (!entry.coverage().covers(needed)) {
                continue;
            }
            long cost = entry.index().cost(cpq);
            if (cost < bestCost) {
                bestCost = cost;
                bestEntry = entry;
            }
        }
        if (bestEntry == null) {
            throw new IllegalArgumentException("No index covers labels for CPQ");
        }
        return bestEntry;
    }

    private EndpointStats endpointStats(CPQ cpq) {
        EndpointStats cached = endpointStatsCache.get(cpq);
        if (cached != null) {
            return cached;
        }
        Entry entry = selectEntry(cpq);
        EndpointStats computed = endpointStats(entry, cpq);
        EndpointStats previous = endpointStatsCache.putIfAbsent(cpq, computed);
        return previous == null ? computed : previous;
    }

    private EndpointStats endpointStats(Entry entry, CPQ cpq) {
        OverlapSynopsis synopsis = overlapSynopsis(entry);
        if (!synopsis.statsByCore().isEmpty()) {
            CoreHash hash = CanonForm.computeCanon(cpq, false).toHashCanon();
            EndpointStats stats = synopsis.statsByCore().get(hash);
            if (stats != null) {
                return stats;
            }
        }
        return endpointStatsFromPairs(entry.index().query(cpq));
    }

    private OverlapSynopsis overlapSynopsis(Entry entry) {
        OverlapSynopsis cached = overlapSynopsisCache.get(entry.source());
        if (cached != null) {
            return cached;
        }
        OverlapSynopsis built = buildOverlapSynopsis(entry);
        OverlapSynopsis previous = overlapSynopsisCache.putIfAbsent(entry.source(), built);
        return previous == null ? built : previous;
    }

    private static OverlapSynopsis buildOverlapSynopsis(Entry entry) {
        Map<CoreHash, MutableEndpointStats> mutable = new HashMap<>();
        for (Index.Block block : entry.index().getBlocks()) {
            Set<CoreHash> cores = block.getCanonCores();
            if (cores == null || cores.isEmpty()) {
                continue;
            }
            EndpointStats blockStats = endpointStatsFromPairs(block.getPaths());
            for (CoreHash core : cores) {
                MutableEndpointStats stats = mutable.computeIfAbsent(core, ignored -> new MutableEndpointStats());
                stats.cardinality += blockStats.cardinality();
                stats.sourceNdv += blockStats.sourceNdv();
                stats.targetNdv += blockStats.targetNdv();
            }
        }

        if (mutable.isEmpty()) {
            return new OverlapSynopsis(Map.of());
        }

        Map<CoreHash, EndpointStats> statsByCore = new HashMap<>(mutable.size());
        for (Map.Entry<CoreHash, MutableEndpointStats> entryStats : mutable.entrySet()) {
            MutableEndpointStats stats = entryStats.getValue();
            double cardinality = Math.max(1.0D, stats.cardinality);
            statsByCore.put(entryStats.getKey(), new EndpointStats(
                    Math.max(1L, stats.cardinality),
                    clampNdv(stats.sourceNdv, cardinality),
                    clampNdv(stats.targetNdv, cardinality)));
        }
        return new OverlapSynopsis(Map.copyOf(statsByCore));
    }

    private static EndpointStats endpointStatsFromPairs(List<Pair> pairs) {
        long cardinality = pairs.size();
        if (pairs.isEmpty()) {
            return new EndpointStats(0L, 1.0D, 1.0D);
        }
        Set<Integer> sources = new HashSet<>();
        Set<Integer> targets = new HashSet<>();
        for (Pair pair : pairs) {
            sources.add(pair.getSource());
            targets.add(pair.getTarget());
        }
        return new EndpointStats(
                cardinality,
                Math.max(1.0D, sources.size()),
                Math.max(1.0D, targets.size()));
    }

    private StructuralStats structuralStats(CPQ cpq) {
        StructuralStats cached = structuralStatsCache.get(cpq);
        if (cached != null) {
            return cached;
        }
        StructuralStats computed = computeStructuralStats(cpq);
        StructuralStats previous = structuralStatsCache.putIfAbsent(cpq, computed);
        return previous == null ? computed : previous;
    }

    private static StructuralStats computeStructuralStats(CPQ cpq) {
        QueryTree tree = cpq.toAbstractSyntaxTree();
        ArrayDeque<QueryTree> stack = new ArrayDeque<>();
        stack.push(tree);
        int intersections = 0;
        int concatenations = 0;
        while (!stack.isEmpty()) {
            QueryTree node = stack.pop();
            OperationType op = node.getOperation();
            if (op == OperationType.INTERSECTION) {
                intersections++;
            } else if (op == OperationType.CONCATENATION) {
                concatenations++;
            }
            for (int i = 0; i < node.getArity(); i++) {
                stack.push(node.getOperand(i));
            }
        }

        QueryGraphCPQ graph = cpq.toQueryGraph();
        Set<Predicate> labels = new HashSet<>();
        for (QueryGraphCPQ.Edge edge : graph.getEdges()) {
            labels.add(edge.getLabel());
        }
        return new StructuralStats(intersections, concatenations, graph.getEdges().size(), labels.size());
    }

    private static void addEndpoint(
            Map<String, List<EndpointView>> byVariable,
            String variable,
            double ndv,
            double cardinality) {
        byVariable.computeIfAbsent(variable, ignored -> new ArrayList<>())
                .add(new EndpointView(
                        Math.max(1.0D, ndv),
                        cardinality / Math.max(1.0D, ndv)));
    }

    private static double clampNdv(double ndv, double cardinality) {
        if (!(ndv > 0.0D)) {
            return 1.0D;
        }
        return Math.max(1.0D, Math.min(ndv, cardinality));
    }

    private static BitSet labelsFor(CPQ cpq) {
        QueryGraphCPQ graph = cpq.toQueryGraph();
        BitSet labels = new BitSet();
        for (QueryGraphCPQ.Edge edge : graph.getEdges()) {
            Predicate label = edge.getLabel();
            labels.set(label.getID());
        }
        return labels;
    }

    private static List<Path> listIndexFiles(Path path) throws Exception {
        if (Files.isDirectory(path)) {
            List<Path> files = new ArrayList<>();
            try (Stream<Path> stream = Files.list(path)) {
                stream.filter(Files::isRegularFile)
                        .filter(NativeCpqIndex::isIndexFile)
                        .sorted()
                        .forEach(files::add);
            }
            return files;
        }
        if (!Files.exists(path) || Files.isDirectory(path)) {
            throw new IllegalArgumentException("Index file not found: " + path);
        }
        return List.of(path);
    }

    private static boolean isIndexFile(Path path) {
        String name = path.getFileName().toString();
        return name.endsWith(".idx") || name.endsWith(".bin");
    }

    private static LabelCoverage parseLabelCoverage(String fileName) {
        int idx = fileName.indexOf(LABELS_TOKEN);
        if (idx < 0) {
            return ALL_LABELS;
        }
        int start = idx + LABELS_TOKEN.length();
        int end = fileName.indexOf('.', start);
        String raw = end >= 0 ? fileName.substring(start, end) : fileName.substring(start);
        if ("all".equals(raw)) {
            return ALL_LABELS;
        }
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("labels= is empty in " + fileName);
        }
        BitSet labels = new BitSet();
        int pos = 0;
        while (pos < raw.length()) {
            int next = raw.indexOf(',', pos);
            String token = next >= 0 ? raw.substring(pos, next) : raw.substring(pos);
            if (token.isEmpty()) {
                throw new IllegalArgumentException("Invalid labels list in " + fileName);
            }
            labels.set(Integer.parseInt(token));
            if (next < 0) {
                break;
            }
            pos = next + 1;
        }
        return new SubsetLabelCoverage(labels);
    }

    private record EndpointStats(long cardinality, double sourceNdv, double targetNdv) {
    }

    private record StructuralStats(int intersections, int concatenations, int edgeCount, int distinctLabels) {
    }

    private record EndpointView(double ndv, double fanout) {
    }

    private record OverlapSynopsis(Map<CoreHash, EndpointStats> statsByCore) {
    }

    private static final class MutableEndpointStats {
        private long cardinality;
        private double sourceNdv;
        private double targetNdv;
    }


    private sealed interface LabelCoverage permits AllLabelCoverage, SubsetLabelCoverage {
        boolean covers(BitSet needed);
    }

    private static final class AllLabelCoverage implements LabelCoverage {
        @Override
        public boolean covers(BitSet needed) {
            return true;
        }
    }

    private static final class SubsetLabelCoverage implements LabelCoverage {
        private final BitSet labels;

        private SubsetLabelCoverage(BitSet labels) {
            this.labels = (BitSet) Objects.requireNonNull(labels, "labels").clone();
        }

        @Override
        public boolean covers(BitSet needed) {
            for (int bit = needed.nextSetBit(0); bit >= 0; bit = needed.nextSetBit(bit + 1)) {
                if (!labels.get(bit)) {
                    return false;
                }
            }
            return true;
        }
    }

    private record Entry(Index index, LabelCoverage coverage, Path source) {
    }
}
