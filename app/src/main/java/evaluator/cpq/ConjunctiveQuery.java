package evaluator.cpq;

import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.ParserCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class ConjunctiveQuery {
    private final dev.roanh.gmark.lang.cq.CQ syntax;
    private final List<VarCQ> vertices;
    private final List<AtomCQ> edges;
    private final Set<VarCQ> freeVars;

    private ConjunctiveQuery(dev.roanh.gmark.lang.cq.CQ syntax) {
        this.syntax = Objects.requireNonNull(syntax, "syntax");
        UniqueGraph<VarCQ, AtomCQ> graph = syntax.toQueryGraph().toUniqueGraph();
        ensureNoIsolatedFreeVariables(this.syntax, graph);

        this.vertices = graph.getNodes().stream()
                .map(node -> node.getData())
                .sorted(Comparator.comparing(VarCQ::getName))
                .toList();

        List<AtomCQ> atoms = new ArrayList<>(graph.getEdgeCount());
        for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
            atoms.add(edge.getData());
        }
        this.edges = List.copyOf(atoms);
        this.freeVars = Set.copyOf(syntax.getFreeVariables());
    }

    public static ConjunctiveQuery parse(String text) {
        Objects.requireNonNull(text, "text");
        return new ConjunctiveQuery(ParserCQ.parse(text));
    }

    public static ConjunctiveQuery parse(String text, List<Predicate> labelAlphabet) {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(labelAlphabet, "labelAlphabet");
        return new ConjunctiveQuery(ParserCQ.parse(text, labelAlphabet));
    }

    public static ConjunctiveQuery from(dev.roanh.gmark.lang.cq.CQ syntax) {
        Objects.requireNonNull(syntax, "syntax");
        return new ConjunctiveQuery(syntax);
    }

    public dev.roanh.gmark.lang.cq.CQ syntax() {
        return syntax;
    }

    public Set<VarCQ> freeVariables() {
        return freeVars;
    }

    public List<VarCQ> vertices() {
        return vertices;
    }

    public List<AtomCQ> edges() {
        return edges;
    }

    public List<AtomCQ> atoms() {
        return edges;
    }

    public Plan decomposeSingleEdge() {
        List<Plan.Component> components = new ArrayList<>(edges.size());
        for (int i = 0; i < edges.size(); i++) {
            AtomCQ atom = edges.get(i);
            BitSet mask = new BitSet();
            mask.set(i);

            VarCQ u = atom.getSource();
            VarCQ v = atom.getTarget();
            Predicate label = atom.getLabel();

            boolean keepDirection = u.getName().compareTo(v.getName()) <= 0;
            VarCQ s = keepDirection ? u : v;
            VarCQ t = keepDirection ? v : u;
            Predicate useLabel = keepDirection ? label : label.getInverse();
            CPQ cpq = CPQ.label(useLabel);
            String description = useLabel.getAlias();
            if (s.equals(t)) {
                cpq = CPQ.intersect(cpq, CPQ.id());
                description = "(" + description + "&id)";
            }
            components.add(new Plan.Component(s, t, 1, mask, cpq, description));
        }
        return new Plan(this, components);
    }

    /**
     * Ensures all free variables appear in the query graph.
     * Decomposition assumes every free variable is connected to at least one atom.
     */
    private static void ensureNoIsolatedFreeVariables(
            dev.roanh.gmark.lang.cq.CQ syntax,
            UniqueGraph<VarCQ, AtomCQ> graph) {
        Set<VarCQ> freeVars = new HashSet<>(syntax.getFreeVariables());
        if (freeVars.isEmpty()) {
            return;
        }
        for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
            AtomCQ atom = edge.getData();
            freeVars.remove(atom.getSource());
            freeVars.remove(atom.getTarget());
            if (freeVars.isEmpty()) {
                return;
            }
        }

        List<String> names = new ArrayList<>(freeVars.size());
        for (VarCQ var : freeVars) {
            names.add("?" + var.getName());
        }
        names.sort(String::compareTo);
        throw new IllegalArgumentException("CQ has isolated free variables: " + String.join(", ", names));
    }
}
