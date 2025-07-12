package decomposition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import decomposition.Partitioning.Edge;
import decomposition.QueryUtils.ComponentInfo;
import static decomposition.QueryUtils.buildCQFromEdges;
import static decomposition.QueryUtils.constructCPQFromEdges;
import static decomposition.QueryUtils.extractFreeVariableNames;
import static decomposition.QueryUtils.getVarsFromEdges;
import static decomposition.QueryUtils.initializeKnownComponents;
import static decomposition.QueryUtils.isEquivalent;
import static decomposition.QueryUtils.printCQAtoms;
import static decomposition.Util.exportAllPartitionsToJson;
import static decomposition.Util.exportFreeVarsToJson;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.ConcatCPQ;
import dev.roanh.gmark.lang.cpq.IntersectionCPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;

public class App {

    public static void main(String[] args) {
        CQ cq = initializeExampleCQ();
        processQuery(cq);
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
        cq.addAtom(a, new Predicate(5, "r5"), c);

        return cq;
    }

    public static void processQuery(CQ cq) {
        Set<String> freeVars = extractFreeVariableNames(cq);

        // Get and export filtered partitions
        List<List<List<Edge>>> filteredPartitions = preparePartitions(cq, freeVars);

        // Initialize known components from first partition
        Map<List<Edge>, ComponentInfo> knownComponents = initializeKnownComponents(filteredPartitions);

        // Process each partition
        for (int i = 0; i < filteredPartitions.size(); i++) {
            List<List<Edge>> partition = filteredPartitions.get(i);
            System.out.println("\n========= Partition #" + (i + 1) + " =========");
            processPartition(partition, knownComponents);
        }
    }

    private static List<List<List<Edge>>> preparePartitions(CQ cq, Set<String> freeVars) {
        List<List<List<Edge>>> allPartitions = Partitioning.enumerateConnectedEdgePartitions(cq);
        System.out.println("Total partitions found: " + allPartitions.size());

        exportFreeVarsToJson(freeVars);

        List<List<List<Edge>>> filtered = Partitioning.filterPartitionsByJoinConstraint(allPartitions, 2, freeVars);
        System.out.println("Filtered partitions (≤2 join nodes/component): " + filtered.size());

        // Compute unfiltered by removing filtered from all
        List<List<List<Edge>>> unfiltered = new ArrayList<>(allPartitions);
        unfiltered.removeAll(filtered);

        // Step: Identify CPQ-valid partitions
        List<List<List<Edge>>> cpqValidPartitions = new ArrayList<>();

        for (List<List<Edge>> partition : filtered) {
            Map<List<Edge>, ComponentInfo> componentMap = new HashMap<>();

            // This processes each component and attempts to mark it as known
            processPartition(partition, componentMap);

            boolean allComponentsKnown = partition.stream()
                .allMatch(component -> {
                    ComponentInfo info = componentMap.get(component);
                    return info != null && info.isKnown;
                });

            if (allComponentsKnown) {
                cpqValidPartitions.add(partition);
            }
        }

        // Export all sets
        exportAllPartitionsToJson(filtered, "filtered");
        exportAllPartitionsToJson(unfiltered, "unfiltered");

        return filtered;
    }

    public static void processPartition(List<List<Edge>> partition, Map<List<Edge>, ComponentInfo> componentMap) {
        for (List<Edge> component : partition) {
            ComponentInfo info = componentMap.get(component);
            if (info != null && info.isKnown) {
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

    public static void tryMatchWithKnownComponents(List<Edge> component,
                                                Map<List<Edge>, ComponentInfo> componentMap) {
        Set<Edge> targetEdges = new HashSet<>(component);
        List<List<Edge>> knownComponents = componentMap.entrySet().stream()
                .filter(e -> e.getValue().isKnown)
                .map(Map.Entry::getKey)
                .toList();

        for (int i = 0; i < knownComponents.size(); i++) {
            for (int j = i + 1; j < knownComponents.size(); j++) {
                List<Edge> kc1 = knownComponents.get(i);
                List<Edge> kc2 = knownComponents.get(j);

                Set<Edge> combined = new HashSet<>(kc1);
                combined.addAll(kc2);

                if (!combined.equals(targetEdges)) continue;

                Set<String> sharedVars = new HashSet<>(componentMap.get(kc1).variables);
                sharedVars.retainAll(componentMap.get(kc2).variables);

                CPQ cpq1 = componentMap.get(kc1).cpq;
                CPQ cpq2 = componentMap.get(kc2).cpq;
                CPQ target = componentMap.get(component).cpq;

                if (sharedVars.size() == 1) {
                    CPQ concat1 = new ConcatCPQ(List.of(cpq1, cpq2));
                    CPQ concat2 = new ConcatCPQ(List.of(cpq2, cpq1));

                    if (isEquivalent(concat1, target)) {
                        System.out.println("Match found by concatenation (cpq1 followed by cpq2)");
                        componentMap.put(component, componentMap.get(component).markKnown());
                        return;
                    }
                    if (isEquivalent(concat2, target)) {
                        System.out.println("Match found by concatenation (cpq2 followed by cpq1)");
                        componentMap.put(component, componentMap.get(component).markKnown());
                        return;
                    }

                } else if (sharedVars.size() == 2) {
                    CPQ intersection = new IntersectionCPQ(List.of(cpq1, cpq2));

                    if (isEquivalent(intersection, target)) {
                        System.out.println("Match found by conjunction (intersection of cpq1 and cpq2)");
                        componentMap.put(component, componentMap.get(component).markKnown());
                        return;
                    }
                }
            }
        }
    }

}
