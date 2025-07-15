package decomposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import decomposition.Partitioning.Edge;
import decomposition.QueryUtils.ComponentInfo;
import static decomposition.QueryUtils.Matches;
import static decomposition.QueryUtils.buildCQFromEdges;
import static decomposition.QueryUtils.extractFreeVariableNames;
import static decomposition.QueryUtils.getVarsFromEdges;
import static decomposition.QueryUtils.initializeKnownComponents;
import static decomposition.QueryUtils.printEdgesFromCPQ;
import static decomposition.Util.clearTempFolder;
import static decomposition.Util.exportAllPartitionsToJson;
import static decomposition.Util.exportFreeVarsToJson;
import static decomposition.Util.printKnownComponents;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.ConcatCPQ;
import dev.roanh.gmark.lang.cpq.IntersectionCPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;

public class App {

    public static void main(String[] args) {
        clearTempFolder("temp");
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

        // enumerate and filter partitions
        List<List<List<Edge>>> allPartitions = Partitioning.enumerateConnectedEdgePartitions(cq);
        System.out.println("Total partitions found: " + allPartitions.size());
        exportFreeVarsToJson(freeVars);

        List<List<List<Edge>>> filteredPartitions = Partitioning.filterPartitionsByJoinConstraint(allPartitions, 2, freeVars);
        System.out.println("Filtered partitions (≤ 2 join nodes/component): " + filteredPartitions.size());

        List<List<List<Edge>>> unfiltered = new ArrayList<>(allPartitions);
        unfiltered.removeAll(filteredPartitions);
        exportAllPartitionsToJson(filteredPartitions, "filtered");
        exportAllPartitionsToJson(unfiltered, "unfiltered");

        //Initialize known components
        Map<Set<Edge>, ComponentInfo> knownComponents = initializeKnownComponents(filteredPartitions, freeVars);
        List<List<List<Edge>>> cpqValidPartitions = new ArrayList<>();

        for (int i = 0; i < filteredPartitions.size(); i++) {
            List<List<Edge>> partition = filteredPartitions.get(i);
            System.out.println("\n========= Partition #" + (i + 1) + " =========");
            // if (i +1  == 10){
                // printKnownComponents(knownComponents);
                
            // }
            List<Set<String>> joinNodesPerComponent = Partitioning.getJoinNodesPerComponent(partition, freeVars);

            for (int j = 0; j < partition.size(); j++) {
                List<Edge> component = partition.get(j);
                Set<Edge> componentKey = new HashSet<>(component);
                ComponentInfo info = knownComponents.get(componentKey);
                if (info != null && info.isKnown) continue;

                CQ subCq = buildCQFromEdges(component);
                Set<String> vars = getVarsFromEdges(component);
                Set<String> joinNodes = joinNodesPerComponent.get(j);

                knownComponents.put(componentKey, new ComponentInfo(subCq, new ArrayList<>(), vars, joinNodes, false));

                System.out.println("Processing Unknown Component:");
                component.forEach(e -> System.out.println("  " + e));

                tryMatchWithKnownComponents(component, knownComponents);
            }

            boolean allHaveCPQs = partition.stream()
                    .map(HashSet::new)
                    .allMatch(compSet -> {
                        ComponentInfo info = knownComponents.get(compSet);
                        return info != null && info.isKnown && !info.cpqs.isEmpty();
                    });

            if (allHaveCPQs) cpqValidPartitions.add(partition);
        }

        // printKnownComponents(knownComponents);
        exportAllPartitionsToJson(cpqValidPartitions, "cpq");
    }

    public static void tryMatchWithKnownComponents(List<Edge> component, Map<Set<Edge>, ComponentInfo> componentMap) {
        Set<Edge> componentKey = new HashSet<>(component);
        Set<Edge> targetEdges = componentKey;

        List<Set<Edge>> knownComponents = componentMap.entrySet().stream()
                .filter(e -> e.getValue().isKnown)
                .map(Map.Entry::getKey)
                .toList();

        ComponentInfo currentInfo = componentMap.get(componentKey);
        List<CPQ> updatedCPQs = new ArrayList<>(currentInfo.cpqs);  // existing CPQs
        boolean foundNewMatch = false;

        for (int i = 0; i < knownComponents.size(); i++) {
            for (int j = 0; j < knownComponents.size(); j++) {
                if (i == j) continue;

                Set<Edge> kc1 = knownComponents.get(i);
                Set<Edge> kc2 = knownComponents.get(j);

                Set<Edge> intersection = new HashSet<>(kc1);
                intersection.retainAll(kc2);
                if (!intersection.isEmpty()) continue;

                Set<Edge> combined = new HashSet<>(kc1);
                combined.addAll(kc2);
                if (!combined.equals(targetEdges)) continue;

                Set<String> sharedVars = new HashSet<>(componentMap.get(kc1).variables);
                sharedVars.retainAll(componentMap.get(kc2).variables);

                for (CPQ cpq1 : componentMap.get(kc1).cpqs) {
                    for (CPQ cpq2 : componentMap.get(kc2).cpqs) {

                        if (sharedVars.size() == 1) {
                            CPQ concat1 = new ConcatCPQ(List.of(cpq1, cpq2));

                            if (QueryUtils.isInjectiveHomomorphic(concat1, component, currentInfo.joinNodes)) {
                                if (!updatedCPQs.contains(concat1)) {
                                    updatedCPQs.add(concat1);
                                    foundNewMatch = true;
                                    System.out.println("Match found by concat (cpq1 + cpq2)");
                                    printEdgesFromCPQ(concat1);
                                }
                            }

                            CPQ concat2 = new ConcatCPQ(List.of(cpq2, cpq1));
                            if (QueryUtils.isInjectiveHomomorphic(concat2, component, currentInfo.joinNodes)) {
                                if (!updatedCPQs.contains(concat2)) {
                                    updatedCPQs.add(concat2);
                                    foundNewMatch = true;
                                    System.out.println("Match found by concat (cpq2 + cpq1)");
                                    printEdgesFromCPQ(concat2);
                                }
                            }

                        } else if (sharedVars.size() == 2) {
                            CPQ intersectionCPQ = new IntersectionCPQ(List.of(cpq1, cpq2));
                            if (QueryUtils.isInjectiveHomomorphic(intersectionCPQ, component, currentInfo.joinNodes)) {
                                if (!updatedCPQs.contains(intersectionCPQ)) {
                                    updatedCPQs.add(intersectionCPQ);
                                    foundNewMatch = true;
                                    System.out.println("Match found by structural intersection (cpq1 ∩ cpq2)");
                                    printEdgesFromCPQ(intersectionCPQ);
                                }
                            }

                            CPQ concat = new ConcatCPQ(List.of(cpq1, cpq2));
                            CPQ loopIntersection = new IntersectionCPQ(List.of(concat, CPQ.id()));
                            if (QueryUtils.isInjectiveHomomorphic(loopIntersection, component, currentInfo.joinNodes)) {
                                if (!updatedCPQs.contains(loopIntersection)) {
                                    updatedCPQs.add(loopIntersection);
                                    foundNewMatch = true;
                                    System.out.println("Match found by loop intersection ((cpq1 ∘ cpq2) ∩ id)");
                                    printEdgesFromCPQ(loopIntersection);
                                }
                            }
                        }
                    }
                }
            }
        }

        if (foundNewMatch) {
            ComponentInfo updated = new ComponentInfo(
                    currentInfo.cq,
                    updatedCPQs,
                    currentInfo.variables,
                    currentInfo.joinNodes,
                    true
            );
            componentMap.put(componentKey, updated);

            // Print all updated CPQs
            System.out.println("Updated CPQs for matched component:");
            for (CPQ cpq : updatedCPQs) {
                System.out.println("  CPQ: " + cpq.toString());
            }
        }
    }



    private static class PartitionSets {
        final List<List<List<Edge>>> filtered;
        final List<List<List<Edge>>> unfiltered;

        PartitionSets(List<List<List<Edge>>> filtered, List<List<List<Edge>>> unfiltered) {
            this.filtered = filtered;
            this.unfiltered = unfiltered;
        }
    }
}
