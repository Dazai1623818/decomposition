package decomposition.eval.engine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Pure algorithmic implementation of Leapfrog Trie Join. Decoupled from CPQ specific logic. */
public final class LeapfrogJoiner {

  public List<Map<String, Integer>> join(List<RelationBinding> relations) {
    if (relations.isEmpty()) {
      return List.of();
    }

    // 1. Organize relations by variable
    Map<String, List<RelationBinding>> bindingsByVar = new HashMap<>();
    for (RelationBinding binding : relations) {
      binding.register(bindingsByVar);
    }

    // 2. Determine variable ordering (greedy heuristic: most constrained first)
    List<String> variableOrder =
        bindingsByVar.keySet().stream()
            .sorted(
                Comparator.comparingInt((String var) -> bindingsByVar.get(var).size())
                    .reversed()
                    .thenComparing(Comparator.naturalOrder()))
            .toList();

    // 3. Execute Recursive Search
    List<Map<String, Integer>> results = new ArrayList<>();
    search(variableOrder, 0, bindingsByVar, new LinkedHashMap<>(), results);
    return results;
  }

  private void search(
      List<String> order,
      int depth,
      Map<String, List<RelationBinding>> bindings,
      Map<String, Integer> assignment,
      List<Map<String, Integer>> results) {

    if (depth == order.size()) {
      results.add(new LinkedHashMap<>(assignment));
      return;
    }

    String variable = order.get(depth);
    List<RelationBinding> constraints = bindings.getOrDefault(variable, List.of());

    // Setup Leapfrog Intersect
    List<IntCursor> cursors = new ArrayList<>();
    for (RelationBinding binding : constraints) {
      int[] domain = binding.domainFor(variable, assignment);
      if (domain.length == 0) {
        return; // Prune branch
      }
      cursors.add(new IntCursor(domain));
    }

    LeapfrogIterator iterator = new LeapfrogIterator(cursors);
    iterator.init();

    while (!iterator.atEnd()) {
      assignment.put(variable, iterator.key());
      search(order, depth + 1, bindings, assignment, results);
      assignment.remove(variable);
      iterator.next();
    }
  }
}
