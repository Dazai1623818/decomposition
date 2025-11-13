package decomposition.cpq;

import java.util.Set;

public record ComponentCacheKey(
    String signature,
    Set<String> joinNodes,
    Set<String> freeVars,
    int componentSize,
    int totalComponents,
    int varContextHash) {

  public ComponentCacheKey {
    joinNodes = (joinNodes == null || joinNodes.isEmpty()) ? Set.of() : Set.copyOf(joinNodes);
    freeVars = (freeVars == null || freeVars.isEmpty()) ? Set.of() : Set.copyOf(freeVars);
  }
}
