package evaluator.index;

import dev.roanh.cpqindex.Index;
import dev.roanh.cpqindex.Main;
import dev.roanh.cpqindex.Pair;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
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
    private final List<Entry> entries;
    private final int k;
    private final int maxIntersections;

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
