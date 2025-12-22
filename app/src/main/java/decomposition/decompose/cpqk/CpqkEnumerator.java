package decomposition.decompose.cpqk;

import decomposition.core.CPQExpression;
import decomposition.core.Component;
import decomposition.core.ConjunctiveQuery;
import decomposition.eval.EvaluationRun;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import dev.roanh.gmark.util.graph.generic.UniqueGraph;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/** CPQ-k enumeration based decomposition (no tuple-level dedup). */
public final class CpqkEnumerator {

  private CpqkEnumerator() {}

  public static EvaluationRun decompose(ConjunctiveQuery query, int k, int limit) {
    Objects.requireNonNull(query, "query");
    if (k < 0) {
      throw new IllegalArgumentException("k must be >= 0");
    }

    EvaluationRun run = new EvaluationRun();
    long totalStart = System.nanoTime();

    Instance instance = Instance.from(query);

    long compStart = System.nanoTime();
    List<CpqkComponent> allComponents = enumerateCpqkComponents(instance, k);
    List<CpqkComponent> components = deduplicateComponents(allComponents);
    long compEnd = System.nanoTime();
    run.recordPhase(
        EvaluationRun.Phase.COMPONENT_ENUMERATION,
        (compEnd - compStart) / 1_000_000,
        components.size());

    long decompStart = System.nanoTime();
    List<List<CPQExpression>> out = new ArrayList<>();
    ComponentCache cache = new ComponentCache(instance);
    enumerateExactDecompositions(
        instance,
        components,
        limit,
        decomposition -> out.add(toExpressionTuple(decomposition, cache)));
    long decompEnd = System.nanoTime();
    run.recordPhase(
        EvaluationRun.Phase.PARTITION_GENERATION,
        (decompEnd - decompStart) / 1_000_000,
        out.size());
    run.setDecompositions(out);

    run.recordPhaseMs(EvaluationRun.Phase.TOTAL, (System.nanoTime() - totalStart) / 1_000_000);
    return run;
  }

  private record CpqkComponent(
      VarCQ s, VarCQ t, int diameter, BitSet mask, CPQ cpq, String canonical) {
    CpqkComponent {
      Objects.requireNonNull(s, "s");
      Objects.requireNonNull(t, "t");
      Objects.requireNonNull(mask, "mask");
      Objects.requireNonNull(cpq, "cpq");
      Objects.requireNonNull(canonical, "canonical");
      if (diameter < 0) {
        throw new IllegalArgumentException("diameter must be >= 0");
      }
      mask = (BitSet) mask.clone();
    }

    Set<VarCQ> endpoints() {
      return s.equals(t) ? Set.of(s) : Set.of(s, t);
    }
  }

  private static List<CpqkComponent> enumerateCpqkComponents(Instance instance, int k) {
    int edgeCount = instance.edges().size();

    Map<Key, CpqkComponent> best = new HashMap<>();
    List<CpqkComponent> all = new ArrayList<>();
    ArrayDeque<CpqkComponent> queue = new ArrayDeque<>();

    BitSet empty = new BitSet(edgeCount);
    for (VarCQ v : instance.vertices()) {
      addOrImprove(new CpqkComponent(v, v, 0, empty, CPQ.id(), "id"), best, all, queue);
    }

    for (Edge e : instance.edges()) {
      BitSet mask = new BitSet(edgeCount);
      mask.set(e.id());
      addOrImprove(
          new CpqkComponent(e.src(), e.trg(), 1, mask, CPQ.label(e.label()), e.label().getAlias()),
          best,
          all,
          queue);
      Predicate inv = e.label().getInverse();
      addOrImprove(
          new CpqkComponent(e.trg(), e.src(), 1, mask, CPQ.label(inv), inv.getAlias()),
          best,
          all,
          queue);
    }

    Map<VarCQ, BitSet> incidence = instance.incidence();

    while (!queue.isEmpty()) {
      CpqkComponent c = queue.removeFirst();
      if (!Objects.equals(best.get(keyOf(c)), c)) {
        continue;
      }

      for (int i = 0, size = all.size(); i < size; i++) {
        CpqkComponent other = all.get(i);
        if (!Objects.equals(best.get(keyOf(other)), other)) {
          continue;
        }
        tryConcat(other, c, k, best, all, queue, incidence);
        tryConcat(c, other, k, best, all, queue, incidence);
        tryIntersect(c, other, k, best, all, queue);
      }
    }

    return best.values().stream().sorted(COMPONENT_ORDER).toList();
  }

  private static void enumerateExactDecompositions(
      Instance instance,
      List<CpqkComponent> components,
      int limit,
      Consumer<List<CpqkComponent>> out) {
    Objects.requireNonNull(out, "out");

    int edgeCount = instance.edges().size();

    List<List<Integer>> cover = new ArrayList<>(edgeCount);
    for (int e = 0; e < edgeCount; e++) {
      cover.add(new ArrayList<>());
    }
    for (int idx = 0; idx < components.size(); idx++) {
      BitSet mask = components.get(idx).mask();
      for (int e = mask.nextSetBit(0); e >= 0; e = mask.nextSetBit(e + 1)) {
        cover.get(e).add(idx);
      }
    }
    for (int e = 0; e < edgeCount; e++) {
      Edge edge = instance.edges().get(e);
      cover
          .get(e)
          .sort(
              Comparator.comparingInt((Integer idx) -> components.get(idx).mask().cardinality())
                  .thenComparingInt(idx -> components.get(idx).diameter())
                  .thenComparingInt(idx -> prefersEdgeDirection(components.get(idx), edge))
                  .thenComparingInt(idx -> idx));
    }

    Set<VarCQ> isolated = isolatedFreeVars(instance.freeVars(), instance.edges());
    Set<VarCQ> required = new HashSet<>(instance.freeVars());
    required.removeAll(isolated);

    int max = limit <= 0 ? Integer.MAX_VALUE : limit;
    BitSet empty = new BitSet(edgeCount);

    class Dfs {
      int outputs = 0;

      void run(BitSet covered, Set<VarCQ> freeCovered, List<Integer> chosen) {
        if (outputs >= max) {
          return;
        }

        int nextEdge = covered.nextClearBit(0);
        if (nextEdge >= edgeCount) {
          if (!freeCovered.containsAll(required)) {
            return;
          }

          Set<VarCQ> missing = new HashSet<>(instance.freeVars());
          missing.removeAll(freeCovered);
          if (!isolated.containsAll(missing)) {
            return;
          }

          ArrayList<Integer> ordered = new ArrayList<>(chosen);
          ordered.sort(Integer::compareTo);
          ArrayList<CpqkComponent> decomposition = new ArrayList<>(ordered.size() + missing.size());
          for (int idx : ordered) {
            decomposition.add(components.get(idx));
          }
          for (VarCQ v : missing) {
            decomposition.add(new CpqkComponent(v, v, 0, empty, CPQ.id(), "id"));
          }

          out.accept(decomposition);
          outputs++;
          return;
        }

        for (int idx : cover.get(nextEdge)) {
          CpqkComponent c = components.get(idx);
          if (c.mask().intersects(covered)) {
            continue;
          }

          BitSet nextCovered = (BitSet) covered.clone();
          nextCovered.or(c.mask());

          Set<VarCQ> nextFreeCovered = new HashSet<>(freeCovered);
          if (required.contains(c.s())) {
            nextFreeCovered.add(c.s());
          }
          if (required.contains(c.t())) {
            nextFreeCovered.add(c.t());
          }

          chosen.add(idx);
          run(nextCovered, nextFreeCovered, chosen);
          chosen.remove(chosen.size() - 1);

          if (outputs >= max) {
            return;
          }
        }
      }
    }

    // Note: We intentionally do not deduplicate full tuples here. Equivalent CPQ
    // tuples can be
    // emitted multiple times, which means counts and evaluation work reflect raw
    // enumeration cost.
    new Dfs().run(new BitSet(edgeCount), new HashSet<>(), new ArrayList<>());
  }

  private static List<CPQExpression> toExpressionTuple(
      List<CpqkComponent> decomposition, ComponentCache cache) {
    List<CPQExpression> tuple = new ArrayList<>(decomposition.size());
    for (CpqkComponent component : decomposition) {
      Component core = cache.componentFor(component);
      String source = component.s().getName();
      String target = component.t().getName();
      tuple.add(new CPQExpression(component.cpq(), core, source, target, "CPQ-k component"));
    }
    return List.copyOf(tuple);
  }

  private static int prefersEdgeDirection(CpqkComponent component, Edge edge) {
    return component.s().equals(edge.src()) && component.t().equals(edge.trg()) ? 0 : 1;
  }

  private static final Comparator<CpqkComponent> COMPONENT_ORDER =
      Comparator.comparing((CpqkComponent c) -> c.s().getName())
          .thenComparing(c -> c.t().getName())
          .thenComparing((CpqkComponent c) -> c.mask().toLongArray(), Arrays::compare)
          .thenComparingInt(CpqkComponent::diameter)
          .thenComparing(CpqkComponent::canonical);

  private static void tryConcat(
      CpqkComponent left,
      CpqkComponent right,
      int k,
      Map<Key, CpqkComponent> best,
      List<CpqkComponent> all,
      ArrayDeque<CpqkComponent> queue,
      Map<VarCQ, BitSet> incidence) {
    if (!left.t().equals(right.s())) {
      return;
    }
    if ((long) left.diameter() + right.diameter() > k) {
      return;
    }

    CPQ cpq = CPQ.concat(left.cpq(), right.cpq());
    CpqkNormalizer.Normalized norm = CpqkNormalizer.normalize(cpq);
    if (norm.cpq().getDiameter() > k) {
      return;
    }

    BitSet mask = unionMask(left.mask(), right.mask());

    VarCQ pivot = left.t();
    boolean pivotIsEndpoint = pivot.equals(left.s()) || pivot.equals(right.t());
    if (!pivotIsEndpoint) {
      BitSet pivotIncidence = incidence.get(pivot);
      if (pivotIncidence != null) {
        BitSet missing = (BitSet) pivotIncidence.clone();
        missing.andNot(mask);
        if (!missing.isEmpty()) {
          return;
        }
      }
    }

    addOrImprove(
        new CpqkComponent(
            left.s(), right.t(), norm.cpq().getDiameter(), mask, norm.cpq(), norm.canonical()),
        best,
        all,
        queue);
  }

  private static void tryIntersect(
      CpqkComponent a,
      CpqkComponent b,
      int k,
      Map<Key, CpqkComponent> best,
      List<CpqkComponent> all,
      ArrayDeque<CpqkComponent> queue) {
    if (!a.s().equals(b.s()) || !a.t().equals(b.t())) {
      return;
    }
    if (a.mask().intersects(b.mask())) {
      return;
    }
    if (Math.max(a.diameter(), b.diameter()) > k) {
      return;
    }

    CPQ cpq = CPQ.intersect(a.cpq(), b.cpq());
    CpqkNormalizer.Normalized norm = CpqkNormalizer.normalize(cpq);
    if (norm.cpq().getDiameter() > k) {
      return;
    }

    BitSet mask = unionMask(a.mask(), b.mask());
    addOrImprove(
        new CpqkComponent(
            a.s(), a.t(), norm.cpq().getDiameter(), mask, norm.cpq(), norm.canonical()),
        best,
        all,
        queue);
  }

  private static void addOrImprove(
      CpqkComponent candidate,
      Map<Key, CpqkComponent> best,
      List<CpqkComponent> all,
      ArrayDeque<CpqkComponent> queue) {
    Key key = keyOf(candidate);
    CpqkComponent existing = best.get(key);
    if (existing != null && !isBetter(candidate, existing)) {
      return;
    }
    best.put(key, candidate);
    all.add(candidate);
    queue.addLast(candidate);
  }

  private static boolean isBetter(CpqkComponent candidate, CpqkComponent existing) {
    int cmp = Integer.compare(candidate.diameter(), existing.diameter());
    if (cmp != 0) {
      return cmp < 0;
    }
    return candidate.canonical().compareTo(existing.canonical()) < 0;
  }

  private static Key keyOf(CpqkComponent c) {
    return new Key(c.s(), c.t(), c.mask());
  }

  private static BitSet unionMask(BitSet a, BitSet b) {
    BitSet out = (BitSet) a.clone();
    out.or(b);
    return out;
  }

  private static Set<VarCQ> isolatedFreeVars(Set<VarCQ> freeVars, List<Edge> edges) {
    Set<VarCQ> incident = new HashSet<>();
    for (Edge e : edges) {
      incident.add(e.src());
      incident.add(e.trg());
    }

    Set<VarCQ> isolated = new HashSet<>(freeVars);
    isolated.removeAll(incident);
    return isolated;
  }

  private static List<CpqkComponent> deduplicateComponents(List<CpqkComponent> components) {
    Map<DedupKey, CpqkComponent> canonical = new HashMap<>();
    for (CpqkComponent c : components) {
      String s = c.s().getName();
      String t = c.t().getName();
      String endpoints = s.compareTo(t) <= 0 ? s + "|" + t : t + "|" + s;
      DedupKey key = new DedupKey(c.mask(), endpoints);

      canonical.merge(
          key,
          c,
          (existing, candidate) -> {
            int cmpExisting = existing.s().getName().compareTo(existing.t().getName());
            int cmpCandidate = candidate.s().getName().compareTo(candidate.t().getName());

            if (cmpExisting <= 0 && cmpCandidate > 0) return existing;
            if (cmpCandidate <= 0 && cmpExisting > 0) return candidate;

            return existing.canonical().compareTo(candidate.canonical()) <= 0
                ? existing
                : candidate;
          });
    }
    return new ArrayList<>(canonical.values());
  }

  private static String bitsetKey(BitSet bits) {
    StringBuilder sb = new StringBuilder();
    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      if (sb.length() > 0) sb.append(',');
      sb.append(i);
    }
    return sb.toString();
  }

  private static boolean isSubset(BitSet sub, BitSet sup) {
    BitSet diff = (BitSet) sub.clone();
    diff.andNot(sup);
    return diff.isEmpty();
  }

  private static final class ComponentCache {
    private final Instance instance;
    private final Map<String, Component> byMask = new HashMap<>();

    private ComponentCache(Instance instance) {
      this.instance = instance;
    }

    private Component componentFor(CpqkComponent component) {
      BitSet mask = component.mask();
      if (mask.isEmpty()) {
        String s = component.s().getName();
        String t = component.t().getName();
        Set<String> vertices = s.equals(t) ? Set.of(s) : Set.of(s, t);
        return new Component(mask, vertices, Set.of(), instance.varMap());
      }
      String key = bitsetKey(mask);
      Component cached = byMask.get(key);
      if (cached != null) {
        return cached;
      }
      Set<String> vertices = instance.vertexNames(mask);
      Set<String> joinNodes = instance.joinNodes(mask);
      Component created = new Component(mask, vertices, joinNodes, instance.varMap());
      byMask.put(key, created);
      return created;
    }
  }

  private record DedupKey(BitSet mask, String endpoints) {}

  private record Key(VarCQ s, VarCQ t, BitSet mask) {}

  private record Edge(int id, VarCQ src, VarCQ trg, Predicate label) {}

  private record Instance(
      List<VarCQ> vertices,
      List<Edge> edges,
      Set<VarCQ> freeVars,
      Map<VarCQ, BitSet> incidence,
      Map<String, String> varMap) {
    static Instance from(ConjunctiveQuery query) {
      CQ cq = query.gmarkCQ();
      UniqueGraph<VarCQ, AtomCQ> graph = query.graph();
      List<VarCQ> vertices = graph.getNodes().stream().map(node -> node.getData()).toList();

      List<Edge> edges = new ArrayList<>();
      List<UniqueGraph.GraphEdge<VarCQ, AtomCQ>> graphEdges = graph.getEdges();
      for (int i = 0; i < graphEdges.size(); i++) {
        AtomCQ atom = graphEdges.get(i).getData();
        edges.add(new Edge(i, atom.getSource(), atom.getTarget(), atom.getLabel()));
      }

      Map<VarCQ, BitSet> incidence = new HashMap<>();
      for (Edge e : edges) {
        incidence.computeIfAbsent(e.src(), v -> new BitSet(edges.size())).set(e.id());
        incidence.computeIfAbsent(e.trg(), v -> new BitSet(edges.size())).set(e.id());
      }

      Map<String, String> varMap = new LinkedHashMap<>();
      for (VarCQ v : vertices) {
        varMap.put(v.getName(), v.getName());
      }

      return new Instance(vertices, edges, cq.getFreeVariables(), incidence, varMap);
    }

    private Set<String> vertexNames(BitSet edgeBits) {
      Set<String> names = new LinkedHashSet<>();
      for (int e = edgeBits.nextSetBit(0); e >= 0; e = edgeBits.nextSetBit(e + 1)) {
        Edge edge = edges().get(e);
        names.add(edge.src().getName());
        names.add(edge.trg().getName());
      }
      return names;
    }

    private Set<String> joinNodes(BitSet edgeBits) {
      Set<VarCQ> vars = new LinkedHashSet<>();
      for (int e = edgeBits.nextSetBit(0); e >= 0; e = edgeBits.nextSetBit(e + 1)) {
        Edge edge = edges().get(e);
        vars.add(edge.src());
        vars.add(edge.trg());
      }

      Set<String> names = new LinkedHashSet<>();
      for (VarCQ v : vars) {
        if (freeVars().contains(v) || !isSubset(incidence().get(v), edgeBits)) {
          names.add(v.getName());
        }
      }
      return names;
    }
  }
}
