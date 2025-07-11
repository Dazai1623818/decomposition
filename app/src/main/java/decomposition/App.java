package decomposition;

import dev.roanh.gmark.lang.cq.*;
import dev.roanh.gmark.lang.cpq.*;
import dev.roanh.gmark.type.schema.Predicate;

import java.util.*;
import java.util.stream.Collectors;

import static decomposition.QueryUtils.*;

public class App {

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

        List<List<List<Partitioning.Edge>>> filteredPartitions =
                Partitioning.filterPartitionsByJoinConstraint(partitions, 2, freeVariables);
        System.out.println("Total filtered partitions: " + filteredPartitions.size());

        filteredPartitions.sort(Comparator.comparing(App::computeKey,
                Comparator.comparingInt((int[] k) -> k[0]).thenComparingInt(k -> k[1])));

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

    public static void processPartition(List<List<Partitioning.Edge>> partition,
                                        Map<List<Partitioning.Edge>, ComponentInfo> componentMap) {
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

    public static void tryMatchWithKnownComponents(List<Partitioning.Edge> component,
                                                   Map<List<Partitioning.Edge>, ComponentInfo> componentMap) {
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

                CPQ candidate = null;
                if (shared.size() == 1) {
                    candidate = new ConcatCPQ(List.of(cpq1, cpq2));
                } else if (shared.size() == 2) {
                    candidate = new IntersectionCPQ(List.of(cpq1, cpq2));
                }

                if (candidate != null && isEquivalent(candidate, componentMap.get(component).cpq)) {
                    System.out.println("  >> Match successful with " + (shared.size() == 1 ? "concatenation" : "conjunction"));
                    componentMap.put(component, componentMap.get(component).markKnown());
                    return;
                }
            }
        }
    }

    public static int[] computeKey(List<List<Partitioning.Edge>> partition) {
        int maxSize = 0, maxCount = 0;
        for (List<Partitioning.Edge> comp : partition) {
            int size = comp.size();
            if (size > maxSize) {
                maxSize = size;
                maxCount = 1;
            } else if (size == maxSize) {
                maxCount++;
            }
        }
        return new int[]{maxSize, maxCount};
    }

    public static void main(String[] args) {
        CQ cq = initializeExampleCQ();
        processPartitions(cq);
    }
}
