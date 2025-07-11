package decomposition;

import dev.roanh.gmark.lang.cq.*;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.lang.cpq.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class App {

    public static class ComponentInfo {
        public final CQ cq;
        public final CPQ cpq;
        public final Set<String> variables;
        public final boolean isKnown;

        public ComponentInfo(CQ cq, CPQ cpq, Set<String> variables, boolean isKnown) {
            this.cq = cq;
            this.cpq = cpq;
            this.variables = variables;
            this.isKnown = isKnown;
        }

        public ComponentInfo markKnown() {
            return new ComponentInfo(cq, cpq, variables, true);
        }
    }

    public static CQ initializeExampleCQ() {
        CQ cq = CQ.empty();
        VarCQ a = cq.addFreeVariable("A");
        VarCQ b = cq.addBoundVariable("B");
        VarCQ c = cq.addBoundVariable("C");
        VarCQ d = cq.addBoundVariable("D");

        cq.addAtom(a, new Predicate(1, "r1"), b);
        cq.addAtom(b, new Predicate(2, "r2"), c);
        cq.addAtom(c, new Predicate(3, "r3"), d);
        cq.addAtom(d, new Predicate(4, "r4"), a);

        return cq;
    }

    public static void processPartitions(CQ cq) {
        Set<String> freeVariables = cq.getFreeVariables().stream().map(VarCQ::getName).collect(Collectors.toSet());
        List<Partitioning.Edge> inputEdges = extractEdgesFromCQ(cq);
        List<List<List<Partitioning.Edge>>> partitions = Partitioning.enumerateConnectedEdgePartitions(inputEdges);
        System.out.println("Total partitions: " + partitions.size());

        List<List<List<Partitioning.Edge>>> filteredPartitions = Partitioning.filterPartitionsByJoinConstraint(partitions, 2, freeVariables);
        System.out.println("Total filtered partitions: " + filteredPartitions.size());

        filteredPartitions.sort(Comparator.comparing(App::computeKey, Comparator.comparingInt((int[] k) -> k[0]).thenComparingInt(k -> k[1])));
        Map<List<Partitioning.Edge>, ComponentInfo> componentMap = new HashMap<>();

        for (List<Partitioning.Edge> component : partitions.get(0)) {
            CPQ cpq = new EdgeCPQ(component.get(0).getPredicate());
            CQ subCq = buildCQFromEdges(component);
            Set<String> vars = getVarsFromEdges(component);
            componentMap.put(component, new ComponentInfo(subCq, cpq, vars, true));
            System.out.println("Initial known CPQ: " + component.get(0).getPredicate().getAlias());
        }

        for (int i = 0; i < filteredPartitions.size(); i++) {
            List<List<Partitioning.Edge>> partition = filteredPartitions.get(i);
            System.out.println("\n========= Partition #" + (i + 1) + " =========");
            processPartition(partition, componentMap);
        }
    }

    public static void processPartition(List<List<Partitioning.Edge>> partition, Map<List<Partitioning.Edge>, ComponentInfo> componentMap) {
        for (List<Partitioning.Edge> component : partition) {
            ComponentInfo info = componentMap.get(component);
            if (info != null && info.isKnown) {
                System.out.println("Component already known:");
                component.forEach(e -> System.out.println("  " + e));
                continue;
            }

            CQ subCq = buildCQFromEdges(component);
            CPQ cpq = constructCPQFromEdges(component);
            Set<String> vars = getVarsFromEdges(component);
            componentMap.put(component, new ComponentInfo(subCq, cpq, vars, false));

            System.out.println("Processing Unknown Component:");
            component.forEach(e -> System.out.println("  " + e));
            printCQAtoms(subCq);

            tryMatchWithKnownComponents(component, componentMap);
        }
    }

    public static void tryMatchWithKnownComponents(List<Partitioning.Edge> component, Map<List<Partitioning.Edge>, ComponentInfo> componentMap) {
        Set<Partitioning.Edge> targetEdges = new HashSet<>(component);
        List<List<Partitioning.Edge>> known = componentMap.entrySet().stream()
                .filter(e -> e.getValue().isKnown)
                .map(Map.Entry::getKey)
                .toList();

        for (int i = 0; i < known.size(); i++) {
            for (int j = i + 1; j < known.size(); j++) {
                List<Partitioning.Edge> kc1 = known.get(i);
                List<Partitioning.Edge> kc2 = known.get(j);
                Set<Partitioning.Edge> union = new HashSet<>(kc1);
                union.addAll(kc2);
                if (!union.equals(targetEdges)) continue;

                Set<String> vars1 = componentMap.get(kc1).variables;
                Set<String> vars2 = componentMap.get(kc2).variables;
                Set<String> shared = new HashSet<>(vars1);
                shared.retainAll(vars2);

                CPQ cpq1 = componentMap.get(kc1).cpq;
                CPQ cpq2 = componentMap.get(kc2).cpq;
                CPQ candidate = (shared.size() == 1) ? new ConcatCPQ(List.of(cpq1, cpq2)) : (shared.size() == 2 ? new IntersectionCPQ(List.of(cpq1, cpq2)) : null);

                if (candidate != null && isEquivalent(candidate, componentMap.get(component).cpq)) {
                    System.out.println("  >> Match successful with " + (shared.size() == 1 ? "concatenation" : "conjunction"));
                    componentMap.put(component, componentMap.get(component).markKnown());
                    return;
                }
            }
        }
    }

    public static CQ buildCQFromEdges(List<Partitioning.Edge> edges) {
        CQ cq = CQ.empty();
        Map<String, VarCQ> varMap = new HashMap<>();
        int predicateId = 1;
        for (Partitioning.Edge edge : edges) {
            VarCQ u = varMap.computeIfAbsent(edge.source, cq::addBoundVariable);
            VarCQ v = varMap.computeIfAbsent(edge.target, cq::addBoundVariable);
            cq.addAtom(u, new Predicate(predicateId++, edge.label()), v);
        }
        return cq;
    }

    public static void printCQAtoms(CQ cq) {
        cq.toQueryGraph().toUniqueGraph().getEdges().stream()
                .map(e -> e.getData())
                .forEach(atom -> System.out.println(atom.getSource().getName() + " --" + atom.getLabel().getAlias() + "--> " + atom.getTarget().getName()));
    }

    public static int[] computeKey(List<List<Partitioning.Edge>> partition) {
        int maxSize = 0, maxCount = 0;
        for (List<Partitioning.Edge> comp : partition) {
            int size = comp.size();
            if (size > maxSize) {
                maxSize = size;
                maxCount = 1;
            } else if (size == maxSize) maxCount++;
        }
        return new int[]{maxSize, maxCount};
    }

    public static List<Partitioning.Edge> extractEdgesFromCQ(CQ cq) {
        return cq.toQueryGraph().toUniqueGraph().getEdges().stream()
                .map(e -> e.getData())
                .map(atom -> new Partitioning.Edge(atom.getSource().getName(), atom.getTarget().getName(), atom.getLabel()))
                .collect(Collectors.toList());
    }

    public static Set<String> getVarsFromEdges(List<Partitioning.Edge> edges) {
        return edges.stream().flatMap(e -> Stream.of(e.source, e.target)).collect(Collectors.toSet());
    }

    public static CPQ constructCPQFromEdges(List<Partitioning.Edge> edges) {
        if (edges.size() == 1) return new EdgeCPQ(edges.get(0).getPredicate());
        Map<String, Partitioning.Edge> outMap = new HashMap<>();
        Map<String, Partitioning.Edge> inMap = new HashMap<>();
        Set<String> nodes = new HashSet<>();

        for (Partitioning.Edge edge : edges) {
            outMap.put(edge.source, edge);
            inMap.put(edge.target, edge);
            nodes.add(edge.source);
            nodes.add(edge.target);
        }

        String start = nodes.stream().filter(n -> !inMap.containsKey(n)).findFirst().orElse(null);
        if (start != null) {
            List<CPQ> sequence = new ArrayList<>();
            String curr = start;
            while (outMap.containsKey(curr)) {
                Partitioning.Edge e = outMap.get(curr);
                sequence.add(new EdgeCPQ(e.getPredicate()));
                curr = e.target;
            }
            if (sequence.size() == edges.size()) return new ConcatCPQ(sequence);
        }

        return new IntersectionCPQ(edges.stream().map(e -> new EdgeCPQ(e.getPredicate())).collect(Collectors.toList()));
    }

    public static boolean isEquivalent(CPQ c1, CPQ c2) {
        return c1.toQueryGraph().toUniqueGraph().equals(c2.toQueryGraph().toUniqueGraph());
    }

    public static void main(String[] args) {
        CQ cq = initializeExampleCQ();
        processPartitions(cq);
    }
}
