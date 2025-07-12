package decomposition;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import decomposition.Partitioning.Edge;

public class Util {

    private static final String OUTPUT_DIR = "temp";

    /**
     * Saves all given partitions to separate JSON files inside the `temp/` directory.
     */
    public static void exportAllPartitionsToJson(List<List<List<Edge>>> allPartitions) {
        ensureTempDirectoryExists();

        for (int partitionIndex = 0; partitionIndex < allPartitions.size(); partitionIndex++) {
            List<List<Edge>> partition = allPartitions.get(partitionIndex);
            try {
                exportPartitionToJson(partitionIndex + 1, partition);
            } catch (IOException e) {
                System.err.println("❌ Failed to export partition " + (partitionIndex + 1) + ": " + e.getMessage());
            }
        }
    }

    /**
     * Saves a single partition (composed of multiple connected components) to a JSON file.
     * Each edge is annotated with the component index it belongs to.
     */
    public static void exportPartitionToJson(int partitionId, List<List<Edge>> partition) throws IOException {
        String fileName = OUTPUT_DIR + File.separator + "partition_" + partitionId + ".json";
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
    private static void ensureTempDirectoryExists() {
        File dir = new File(OUTPUT_DIR);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (created) {
                System.out.println("📁 Created output directory: " + OUTPUT_DIR);
            } else {
                System.err.println("⚠️ Failed to create output directory: " + OUTPUT_DIR);
            }
        }
    }
}
