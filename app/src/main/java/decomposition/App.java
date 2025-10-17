package decomposition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import decomposition.Partitioning.Edge;
import decomposition.QueryUtils.ComponentInfo;
import static decomposition.QueryUtils.buildCQFromEdges;
import static decomposition.QueryUtils.extractFreeVariableNames;
import static decomposition.QueryUtils.getVarsFromEdges;
import static decomposition.QueryUtils.initializeKnownComponents;
import static decomposition.QueryUtils.printEdgesFromCPQ;
import static decomposition.QueryUtils.getNodesFromEdges;
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
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.util.graph.GraphPanel;

public class App {

    public static void main(String[] args) {
        // GraphPanel.show(CPQ.parse("1◦((1◦((1◦((1◦1⁻)∩id)◦((1◦1⁻)∩id)◦1⁻)∩id)◦1⁻)∩id)"));
        clearTempFolder("temp");
        CQ cq = Example.example3();
        processQuery(cq);
    }


    public static void processQuery(CQ cq) {
        Set<String> freeVars = extractFreeVariableNames(cq);

        // enumerate and filter partitions
        List<List<Set<Edge>>> allPartitions = Partitioning.enumerateConnectedEdgePartitions(cq);
        System.out.println("Total partitions found: " + allPartitions.size());
        exportFreeVarsToJson(freeVars);

        List<List<Set<Edge>>> filteredPartitions = Partitioning.filterPartitionsByJoinConstraint(allPartitions, 2, freeVars);
        System.out.println("Filtered partitions (≤ 2 join nodes/component): " + filteredPartitions.size());

        List<List<Set<Edge>>> unfiltered = new ArrayList<>(allPartitions);
        unfiltered.removeAll(filteredPartitions);
        exportAllPartitionsToJson(filteredPartitions, "filtered");
        exportAllPartitionsToJson(unfiltered, "unfiltered");

        //Initialize known components
        Map<Set<Edge>, ComponentInfo> knownComponents = initializeKnownComponents(filteredPartitions, freeVars);
        List<List<Set<Edge>>> cpqValidPartitions = new ArrayList<>();
        // printKnownComponents(knownComponents);


        for (int i = 0; i < filteredPartitions.size(); i++) {
        // for (int i = 0; i < 3; i++) {
            List<Set<Edge>> partition = filteredPartitions.get(i);
            System.out.println("\n========= Partition #" + (i + 1) + " =========");
            List<Set<String>> joinNodesPerComponent = Partitioning.getJoinNodesPerComponent(partition, freeVars);

            for (int j = 0; j < partition.size(); j++) {
                Set<Edge> component = partition.get(j);
                Set<Edge> componentKey = new HashSet<>(component);
                ComponentInfo info = knownComponents.get(componentKey);
                if (info != null && info.isKnown) continue;
                CQ subCq = buildCQFromEdges(component);
                Set<String> vars = getVarsFromEdges(component);
                Set<String> joinNodes = joinNodesPerComponent.get(j);

                knownComponents.put(componentKey, new ComponentInfo(subCq, new ArrayList<>(), vars, joinNodes, false));

                System.out.println("Processing Unknown Component:");
                component.forEach(e -> System.out.println("  " + e));
                match(component, knownComponents);
            }

            boolean allHaveCPQs = partition.stream()
                    .map(HashSet::new)
                    .allMatch(compSet -> {
                        ComponentInfo info = knownComponents.get(compSet);
                        return info != null && info.isKnown && !info.cpqs.isEmpty();
                    });

            if (allHaveCPQs) cpqValidPartitions.add(partition);
        }

        exportAllPartitionsToJson(cpqValidPartitions, "cpq");
        check(knownComponents);
    }

    public static void match(Set<Edge> component, Map<Set<Edge>, ComponentInfo> componentMap) {
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

                // Skip if they overlap
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
                            if (QueryUtils.isIsomorphic(concat1, component, currentInfo.joinNodes)) {
                                if (!updatedCPQs.contains(concat1)) {
                                    updatedCPQs.add(concat1);
                                    foundNewMatch = true;
                                    // System.out.println("Match found by concat (cpq1 + cpq2)");
                                    // printEdgesFromCPQ(concat1);
                                }
                            }

                        } else if (sharedVars.size() == 2) {


                            if (currentInfo.joinNodes.size() == 1){
                                // System.out.println(getNodesFromEdges(component));
                                // System.out.println(currentInfo.joinNodes);
                                CPQ concat = new ConcatCPQ(List.of(cpq1, cpq2));
                                CPQ loopIntersection = new IntersectionCPQ(List.of(concat, CPQ.id()));
                                if (QueryUtils.isIsomorphic(loopIntersection, component, currentInfo.joinNodes)) {


                                    if (!updatedCPQs.contains(loopIntersection)) {
                                        updatedCPQs.add(loopIntersection);
                                        foundNewMatch = true;
                                        // System.out.println("Match found by loop intersection ((cpq1 ∘ cpq2) ∩ id)");
                                        // printEdgesFromCPQ(loopIntersection);
                                    }
                                }
                            } else if (!cpq2.toString().equals(cpq1.toString())){
                                CPQ intersectionCPQ = new IntersectionCPQ(List.of(cpq1, cpq2));
                                if (QueryUtils.isIsomorphic(intersectionCPQ, component, currentInfo.joinNodes)) {
                                    if (!updatedCPQs.contains(intersectionCPQ)) {
                                        updatedCPQs.add(intersectionCPQ);
                                        foundNewMatch = true;
                                        // System.out.println("Match found by structural intersection (cpq1 ∩ cpq2)");
                                        // printEdgesFromCPQ(intersectionCPQ);
                                 
                                    }
                                }
                            }

                        }
                    }
                }
            }
        }

        if (!foundNewMatch) {
            List<CPQ> backtrackingCandidates = QueryUtils.generateBacktrackingCPQs(componentKey, currentInfo.joinNodes);
            for (CPQ candidate : backtrackingCandidates) {
                boolean alreadyPresent = updatedCPQs.stream()
                        .anyMatch(existing -> existing.toString().equals(candidate.toString()));
                if (alreadyPresent) continue;

                if (QueryUtils.isIsomorphic(candidate, component, currentInfo.joinNodes)) {
                    updatedCPQs.add(candidate);
                    foundNewMatch = true;
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
            // System.out.println("Updated CPQs for matched component:");
            // for (CPQ cpq : updatedCPQs) {
            //     System.out.println("  CPQ: " + cpq.toString());
            // }
        }
    }



    public static void check(Map<Set<Edge>, ComponentInfo> componentMap) {
        System.out.println("\n=== Internal CPQ Homomorphism Check ===");

        for (Map.Entry<Set<Edge>, ComponentInfo> entry : componentMap.entrySet()) {
            Set<Edge> component = entry.getKey();
            ComponentInfo info = entry.getValue();

            List<CPQ> cpqs = info.cpqs;

            System.out.println("\nComponent:");
            for (Edge e : component) {
                System.out.println("  " + e);
            }


            // Group into equivalence classes 
            List<List<CPQ>> equivalenceClasses = new ArrayList<>();
            outer:
            for (CPQ cpq : cpqs) {
                for (List<CPQ> cls : equivalenceClasses) {
                    CPQ rep = cls.get(0);
                    if (cpq.isHomomorphicTo(rep) && rep.isHomomorphicTo(cpq)) {
                        cls.add(cpq);
                        continue outer;
                    }
                }
                equivalenceClasses.add(new ArrayList<>(List.of(cpq)));
            }

            System.out.println(equivalenceClasses.size() + " unique CPQs:");
            for (int i = 0; i < equivalenceClasses.size(); i++) {
                CPQ representative = equivalenceClasses.get(i).get(0);
                System.out.printf("    Class %d (size %d): %s\n", i + 1, equivalenceClasses.get(i).size(), representative);
                printEdgesFromCPQ(representative);
            }
        }
    }
}
