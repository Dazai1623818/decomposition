package decomposition.cpq;

import java.util.Set;

record RuleCacheKey(String signature, Set<String> joinNodes, int edgeCount) {
  RuleCacheKey {
    joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
  }
}

record ComponentCacheKey(
    String signature,
    Set<String> joinNodes,
    Set<String> freeVars,
    int componentSize,
    int totalComponents,
    int varContextHash) {

  ComponentCacheKey {
    joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
    freeVars = (freeVars == null || freeVars.isEmpty()) ? Set.of() : Set.copyOf(freeVars);
  }
}
