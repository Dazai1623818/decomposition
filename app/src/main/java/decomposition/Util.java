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

public class Util {

    private static final String OUTPUT_DIR = "temp";

    /**
     * Saves all given partitions to separate JSON files inside the `temp/` directory.
     */
    public static void exportAllPartitionsToJson(List<List<List<Edge>>> allPartitions, String subdir) {
        ensureTempDirectoryExists(subdir);

        for (int partitionIndex = 0; partitionIndex < allPartitions.size(); partitionIndex++) {
            List<List<Edge>> partition = allPartitions.get(partitionIndex);
            try {
                exportPartitionToJson(partitionIndex + 1, partition, subdir);  // number still starts from 1
            } catch (IOException e) {
                System.err.println("Failed to export partition " + (partitionIndex + 1) + ": " + e.getMessage());
            }
        }
    }


    /**
     * Saves a single partition (composed of multiple connected components) to a JSON file.
     * Each edge is annotated with the component index it belongs to.
     */
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

    /**
     * Creates the temp output directory if it does not exist.
     */
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
}
