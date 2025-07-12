package decomposition;

import decomposition.Partitioning.Edge;
import decomposition.QueryUtils.ComponentInfo;
import dev.roanh.gmark.lang.cpq.*;
import dev.roanh.gmark.lang.cq.*;
import dev.roanh.gmark.type.schema.Predicate;

import java.util.*;
import java.util.stream.Collectors;

import static decomposition.QueryUtils.*;
import static decomposition.Util.exportAllPartitionsToJson;

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
        // cq.addAtom(a, new Predicate(5, "r5"), d);

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

        // Export separately
        exportAllPartitionsToJson(filtered, "filtered");
        exportAllPartitionsToJson(unfiltered, "unfiltered");

        return filtered;
    }

    private static Map<List<Edge>, ComponentInfo> initializeKnownComponents(List<List<List<Edge>>> filteredPartitions) {
        Map<List<Edge>, ComponentInfo> known = new HashMap<>();
        if (filteredPartitions.isEmpty()) return known;

        List<List<Edge>> firstPartition = filteredPartitions.get(0);
        for (List<Edge> component : firstPartition) {
            Predicate p = component.get(0).getPredicate();
            CPQ cpq = new EdgeCPQ(p);
            CQ subCq = buildCQFromEdges(component);
            Set<String> vars = getVarsFromEdges(component);
            known.put(component, new ComponentInfo(subCq, cpq, vars, true));

            System.out.println("Initial known CPQ: " + p.getAlias());
        }
        return known;
    }

    public static void processPartition(List<List<Edge>> partition, Map<List<Edge>, ComponentInfo> componentMap) {
        for (List<Edge> component : partition) {
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
