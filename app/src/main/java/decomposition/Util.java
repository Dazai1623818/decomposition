package decomposition;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import decomposition.Partitioning.Edge;
import decomposition.QueryUtils.ComponentInfo;
import static decomposition.QueryUtils.printEdgesFromCPQ;

public class Util {

    private static final String OUTPUT_DIR = "temp";

    public static void exportAllPartitionsToJson(List<List<List<Edge>>> allPartitions, String subdir) {
        ensureTempDirectoryExists(subdir);

        for (int partitionIndex = 0; partitionIndex < allPartitions.size(); partitionIndex++) {
            List<List<Edge>> partition = allPartitions.get(partitionIndex);
            try {
                exportPartitionToJson(partitionIndex + 1, partition, subdir);
            } catch (IOException e) {
                System.err.println("Failed to export partition " + (partitionIndex + 1) + ": " + e.getMessage());
            }
        }
    }

    public static void exportPartitionToJson(int partitionId, List<List<Edge>> partition, String subdir) throws IOException {
        String fileName = OUTPUT_DIR + File.separator + subdir + File.separator + "partition_" + partitionId + ".json";
        List<Map<String, String>> edgeEntries = new ArrayList<>();

        for (int componentIndex = 0; componentIndex < partition.size(); componentIndex++) {
            List<Edge> component = partition.get(componentIndex);
            for (Edge edge : component) {
                Map<String, String> edgeMap = new HashMap<>();
                edgeMap.put("source", edge.source);
                edgeMap.put("target", edge.target);
                edgeMap.put("label", edge.predicate.getAlias());
                edgeMap.put("component", String.valueOf(componentIndex));
                edgeEntries.add(edgeMap);
            }
        }

        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(new Gson().toJson(edgeEntries));
        }
    }

    private static void ensureTempDirectoryExists(String subdir) {
        File dir = new File(OUTPUT_DIR + File.separator + subdir);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (created) {
                System.out.println("Created output directory: " + dir.getPath());
            } else {
                System.err.println("Failed to create output directory: " + dir.getPath());
            }
        }
    }

    public static void exportFreeVarsToJson(Set<String> freeVars) {
        String fileName = OUTPUT_DIR + "/free_vars.json";
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(new Gson().toJson(freeVars));
            System.out.println("Exported free variables to " + fileName);
        } catch (IOException e) {
            System.err.println("Failed to export free variables: " + e.getMessage());
        }
    }

    public static void clearTempFolder(String folderPath) {
        File folder = new File(folderPath);
        if (!folder.exists()) {
            System.out.println("Folder does not exist: " + folderPath);
            return;
        }

        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    clearTempFolder(file.getAbsolutePath()); // recursive
                }
                if (!file.delete()) {
                    System.err.println("Failed to delete: " + file.getAbsolutePath());
                }
            }
        }
    }



    public static void printKnownComponents(Map<Set<Edge>, ComponentInfo> knownComponents) {
        System.out.println("Known components so far:");
        int idx = 1;

        for (Map.Entry<Set<Edge>, ComponentInfo> entry : knownComponents.entrySet()) {
            Set<Edge> component = entry.getKey();
            ComponentInfo info = entry.getValue();

            System.out.println("  Component #" + idx++);
            for (Edge edge : component) {
                System.out.println("    " + edge);
            }

            System.out.println("    Known: " + info.isKnown);
            if (info.cpqs != null && !info.cpqs.isEmpty()) {
                for (int i = 0; i < info.cpqs.size(); i++) {
                    System.out.println("    CPQ[" + i + "]:");
                    printEdgesFromCPQ(info.cpqs.get(i));
                }
            } else {
                System.out.println("    CPQ: null or empty");
            }
        }
    }


}
