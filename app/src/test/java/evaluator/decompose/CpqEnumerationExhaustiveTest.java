package evaluator.decompose;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.util.Util;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import evaluator.cq.ConjunctiveQuery;
import evaluator.decompose.CpqDecomposition.Component;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class CpqEnumerationExhaustiveTest {
    @Test
    @Timeout(30)
    void generatedCpqsHaveValidExactCoverDecompositions() {
        int queryCount = Integer.getInteger("cpq.test.queryCount", 50);
        int depth = Integer.getInteger("cpq.test.depth", 10);
        int labelCount = Integer.getInteger("cpq.test.labelCount", 4);
        long baseSeed = 1;

        int collected = 0;
        int attemptLimit = Integer.getInteger("cpq.test.attemptLimit", 5_000);
        boolean dump = Boolean.getBoolean("cpq.test.dumpDecompositions");
        for (int attempt = 0; collected < queryCount && attempt < attemptLimit; attempt++) {
            long seed = baseSeed + attempt;
            Util.setRandomSeed(seed);

            CPQ cpq = CPQ.generateRandomCPQ(depth, labelCount);

            ConjunctiveQuery cq = ConjunctiveQuery.from(cpq.toCQ());
            int k = cpq.getDiameter();
            List<Component> decomposition = cq.decompose(k).components();

            if (dump) {
                int edgeCount = cq.syntax().toQueryGraph().toUniqueGraph().getEdgeCount();
                System.out.println("seed=" + seed + " diameter=" + k + " edges=" + edgeCount);
                System.out.println("decomp size=" + decomposition.size());
                for (Component component : decomposition) {
                    System.out.println("  " + formatComponent(component));
                }
            }

            assertTrue(
                    !decomposition.isEmpty(),
                    () -> "Missing exact-cover decomposition"
                            + " (seed=" + seed
                            + ", diameter=" + k
                            + ", cpq=" + cpq.toFormalSyntax() + ")");
            assertExactCover(cq, decomposition);
            assertEndpointExposure(cq, decomposition);

            collected++;
        }

        assertTrue(
                collected == queryCount,
                "Only generated " + collected + " CPQs within " + attemptLimit + " attempts.");
    }

    private static void assertExactCover(ConjunctiveQuery cq, List<Component> decomposition) {
        int edgeCount = cq.syntax().toQueryGraph().toUniqueGraph().getEdgeCount();
        BitSet covered = new BitSet(edgeCount);
        for (Component component : decomposition) {
            BitSet mask = component.mask();
            assertTrue(!mask.intersects(covered), "Components overlap on atoms");
            covered.or(mask);
        }
        assertTrue(covered.cardinality() == edgeCount, "Decomposition does not cover all atoms");
    }

    private static void assertEndpointExposure(ConjunctiveQuery cq, List<Component> decomposition) {
        UniqueGraph<VarCQ, AtomCQ> graph = cq.syntax().toQueryGraph().toUniqueGraph();
        List<UniqueGraph.GraphEdge<VarCQ, AtomCQ>> edges = graph.getEdges();
        List<VarCQ> vertices = graph.getNodes().stream()
                .map(node -> node.getData())
                .toList();

        Map<VarCQ, Integer> varIndex = new HashMap<>(vertices.size());
        for (int i = 0; i < vertices.size(); i++) {
            varIndex.put(vertices.get(i), i);
        }

        List<BitSet> componentVars = new ArrayList<>(decomposition.size());
        List<BitSet> componentEndpoints = new ArrayList<>(decomposition.size());
        for (Component component : decomposition) {
            BitSet vars = new BitSet(vertices.size());
            BitSet endpoints = new BitSet(vertices.size());
            Integer sIdx = varIndex.get(component.s());
            if (sIdx != null) {
                endpoints.set(sIdx);
            }
            Integer tIdx = varIndex.get(component.t());
            if (tIdx != null) {
                endpoints.set(tIdx);
            }

            BitSet mask = component.mask();
            for (int e = mask.nextSetBit(0); e >= 0; e = mask.nextSetBit(e + 1)) {
                AtomCQ atom = edges.get(e).getData();
                vars.set(varIndex.get(atom.getSource()));
                vars.set(varIndex.get(atom.getTarget()));
            }
            componentVars.add(vars);
            componentEndpoints.add(endpoints);
        }

        int[] counts = new int[vertices.size()];
        for (BitSet vars : componentVars) {
            for (int v = vars.nextSetBit(0); v >= 0; v = vars.nextSetBit(v + 1)) {
                counts[v]++;
            }
        }

        BitSet shared = new BitSet(vertices.size());
        for (int v = 0; v < vertices.size(); v++) {
            if (counts[v] >= 2) {
                shared.set(v);
            }
        }

        for (int i = 0; i < componentVars.size(); i++) {
            BitSet vars = componentVars.get(i);
            if (!vars.intersects(shared)) {
                continue;
            }
            BitSet requiredEndpoints = (BitSet) vars.clone();
            requiredEndpoints.and(shared);
            requiredEndpoints.andNot(componentEndpoints.get(i));
            assertTrue(requiredEndpoints.isEmpty(), "Shared variable not exposed as endpoint");
        }

        Set<VarCQ> endpointsCovered = new HashSet<>();
        for (Component component : decomposition) {
            endpointsCovered.add(component.s());
            endpointsCovered.add(component.t());
        }
        assertTrue(endpointsCovered.containsAll(cq.freeVariables()), "Free variable not exposed as endpoint");

        Set<VarCQ> incident = new HashSet<>();
        for (UniqueGraph.GraphEdge<VarCQ, AtomCQ> edge : edges) {
            AtomCQ atom = edge.getData();
            incident.add(atom.getSource());
            incident.add(atom.getTarget());
        }
        Set<VarCQ> isolated = new HashSet<>(cq.freeVariables());
        isolated.removeAll(incident);
        assertTrue(isolated.isEmpty(), "CQ must be connected (no isolated free variables)");
        for (Component component : decomposition) {
            assertTrue(!component.mask().isEmpty(), "Identity components are not allowed");
        }
    }

    private static String formatComponent(Component component) {
        return "s=" + component.s().getName()
                + " t=" + component.t().getName()
                + " diam=" + component.diameter()
                + " mask=" + component.mask()
                + " canonical=" + component.canonical();
    }

}
