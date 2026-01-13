package evaluator.cq;

import evaluator.decompose.CpqDecomposition;
import evaluator.decompose.CpqDecomposition.Component;
import evaluator.decompose.CpqEnumeration;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.ParserCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.Util;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class ConjunctiveQuery {
    private final dev.roanh.gmark.lang.cq.CQ syntax;

    public enum Strategy {
        FIRST,
        RANDOM,
        SINGLE_EDGE
    }

    private ConjunctiveQuery(dev.roanh.gmark.lang.cq.CQ syntax) {
        this.syntax = Objects.requireNonNull(syntax, "syntax");
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
        return syntax.getFreeVariables();
    }

    public List<AtomCQ> atoms() {
        UniqueGraph<VarCQ, AtomCQ> graph = syntax.toQueryGraph().toUniqueGraph();
        List<AtomCQ> atoms = new ArrayList<>(graph.getEdgeCount());
        for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : graph.getEdges()) {
            atoms.add(edge.getData());
        }
        return atoms;
    }

    public CpqDecomposition decompose(int k) {
        return decompose(k, Strategy.FIRST);
    }

    public CpqDecomposition decompose(int k, Strategy strategy) {
        Objects.checkIndex(Math.max(k, 1) - 1, Integer.MAX_VALUE);
        Objects.requireNonNull(strategy, "strategy");

        List<List<Component>> exact = CpqEnumeration.enumerateExactDecompositions(syntax, k, 0);
        if (strategy == Strategy.SINGLE_EDGE || exact.isEmpty()) {
            return new CpqDecomposition(this, SingleEdgeDecomposition());
        }

        int selected = switch (strategy) {
            case RANDOM -> Util.getRandom().nextInt(exact.size());
            case FIRST -> 0;
            case SINGLE_EDGE -> 0;
        };
        return new CpqDecomposition(this, exact.get(selected));
    }

    private List<Component> SingleEdgeDecomposition() {
        List<AtomCQ> atoms = atoms();
        int edgeCount = atoms.size();
        List<Component> out = new ArrayList<>(edgeCount);
        for (int i = 0; i < edgeCount; i++) {
            AtomCQ atom = atoms.get(i);
            BitSet mask = new BitSet(edgeCount);
            mask.set(i);
            VarCQ u = atom.getSource();
            VarCQ v = atom.getTarget();
            Predicate label = atom.getLabel();

            boolean keepDirection = u.getName().compareTo(v.getName()) <= 0;
            VarCQ s = keepDirection ? u : v;
            VarCQ t = keepDirection ? v : u;
            Predicate useLabel = keepDirection ? label : label.getInverse();

            CPQ cpq = CPQ.label(useLabel);
            out.add(new Component(s, t, 1, mask, cpq, useLabel.getAlias()));
        }
        return out;
    }
}
