package decomposition.cli;

import decomposition.examples.Example;
import dev.roanh.gmark.lang.cq.CQ;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/** Shared helpers for CLI argument parsing and CQ/CPQ loading. */
final class CliParsers {
  private static final Map<String, Supplier<CQ>> EXAMPLE_LOADERS = buildExampleLoaders();

  private CliParsers() {}

  static String nextValue(String[] args, int index, String option) {
    if (index >= args.length) {
      throw new IllegalArgumentException("Missing value for " + option);
    }
    return args[index];
  }

  static int parseInt(String raw, int defaultValue, String optionName) {
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid integer for " + optionName + ": " + raw);
    }
  }

  static long parseLong(String raw, long defaultValue, String optionName) {
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid long for " + optionName + ": " + raw);
    }
  }

  static Set<String> parseFreeVariables(String raw) {
    if (raw == null || raw.isBlank()) {
      return Set.of();
    }
    Set<String> vars = new LinkedHashSet<>();
    Arrays.stream(raw.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(vars::add);
    return vars;
  }

  static CQ loadExampleByName(String exampleName) {
    if (exampleName == null || exampleName.isBlank()) {
      throw new IllegalArgumentException("Unknown example: " + exampleName);
    }
    String normalized = exampleName.trim().toLowerCase(Locale.ROOT);
    Supplier<CQ> supplier = EXAMPLE_LOADERS.get(normalized);
    if (supplier == null) {
      throw new IllegalArgumentException("Unknown example: " + exampleName);
    }
    return supplier.get();
  }

  static CQ loadQueryFromFile(String queryFile) throws java.io.IOException {
    Path path = Path.of(queryFile);
    if (!Files.exists(path)) {
      throw new IllegalArgumentException("Query file not found: " + path);
    }
    String content = Files.readString(path);
    return CQ.parse(content);
  }

  static Path findProjectRoot() {
    Path cwd = Path.of("").toAbsolutePath().normalize();
    Path current = cwd;
    while (current != null) {
      Path settingsHere = current.resolve("settings.gradle");
      if (Files.exists(settingsHere)) {
        return current;
      }
      Path nestedAppSettings = current.resolve("app").resolve("settings.gradle");
      if (Files.exists(nestedAppSettings)) {
        return current.resolve("app");
      }
      current = current.getParent();
    }
    return cwd;
  }

  private static Map<String, Supplier<CQ>> buildExampleLoaders() {
    Map<String, Supplier<CQ>> loaders = new LinkedHashMap<>();
    loaders.put("example1", Example::example1);
    loaders.put("example2", Example::example2);
    loaders.put("example3", Example::example3);
    loaders.put("example4", Example::example4);
    loaders.put("example5", Example::example5);
    loaders.put("example6", Example::example6);
    loaders.put("example7", Example::example7);
    loaders.put("example8", Example::example8);
    return Map.copyOf(loaders);
  }
}
