package decomposition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import decomposition.Partitioning.Edge;
import decomposition.QueryUtils.ComponentInfo;
import static decomposition.QueryUtils.buildCQFromEdges;
import static decomposition.QueryUtils.constructCPQFromEdges;
import static decomposition.QueryUtils.getVarsFromEdges;
import static decomposition.QueryUtils.isEquivalent;
import static decomposition.QueryUtils.printCQAtoms;
import static decomposition.Util.exportAllPartitionsToJson;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.ConcatCPQ;
import dev.roanh.gmark.lang.cpq.EdgeCPQ;
import dev.roanh.gmark.lang.cpq.IntersectionCPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;

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
        // cq.addAtom(a, new Predicate(5, "r5"), d);
        return cq;
    }

    public static void processPartitions(CQ cq) {
        // Extract free variables from the query
        Set<String> freeVariables = cq.getFreeVariables().stream()
                .map(VarCQ::getName)
                .collect(Collectors.toSet());

        // Enumerate and filter connected edge partitions
        List<List<List<Edge>>> allPartitions = Partitioning.enumerateConnectedEdgePartitions(cq);
        System.out.println("Total partitions found: " + allPartitions.size());

        List<List<List<Edge>>> filteredPartitions = Partitioning.filterPartitionsByJoinConstraint(
                allPartitions, 2, freeVariables);
        System.out.println("Filtered partitions (≤2 join nodes/component): " + filteredPartitions.size());

        // Initialize known components map from the first partition
        Map<List<Edge>, ComponentInfo> knownComponents = new HashMap<>();
        List<List<Edge>> initialPartition = allPartitions.get(0);

        for (List<Edge> component : initialPartition) {
            CPQ cpq = new EdgeCPQ(component.get(0).getPredicate());
            CQ subCq = buildCQFromEdges(component);
            Set<String> vars = getVarsFromEdges(component);
            knownComponents.put(component, new ComponentInfo(subCq, cpq, vars, true));

            System.out.println("Initial known CPQ: " + component.get(0).getPredicate().getAlias());
        }

        // Export filtered partitions for visualization
        exportAllPartitionsToJson(filteredPartitions);

        // Process each filtered partition
        for (int partitionIndex = 0; partitionIndex < filteredPartitions.size(); partitionIndex++) {
            List<List<Edge>> partition = filteredPartitions.get(partitionIndex);
            System.out.println("\n========= Partition #" + (partitionIndex + 1) + " =========");
            processPartition(partition, knownComponents);
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

    // public static int[] computeKey(List<List<Partitioning.Edge>> partition) {
    //     int maxSize = 0, maxCount = 0;
    //     for (List<Partitioning.Edge> comp : partition) {
    //         int size = comp.size();
    //         if (size > maxSize) {
    //             maxSize = size;
    //             maxCount = 1;
    //         } else if (size == maxSize) {
    //             maxCount++;
    //         }
    //     }
    //     return new int[]{maxSize, maxCount};
    // }

    public static void main(String[] args) {
        CQ cq = initializeExampleCQ();
        processPartitions(cq);
    }
}
