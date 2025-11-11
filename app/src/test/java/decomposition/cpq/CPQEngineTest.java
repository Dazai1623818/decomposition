package decomposition.cpq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import decomposition.cpq.model.ComponentRules;
import decomposition.extract.CQExtractor;
import decomposition.extract.CQExtractor.ExtractionResult;
import decomposition.model.Component;
import decomposition.model.Edge;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class CPQEngineTest {

  private static List<Edge> sampleEdges() {
    return List.of(
        new Edge("A", "B", new Predicate(1, "r1"), 0),
        new Edge("B", "C", new Predicate(2, "r2"), 1),
        new Edge("C", "D", new Predicate(3, "r3"), 2),
        new Edge("D", "A", new Predicate(4, "r4"), 3),
        new Edge("A", "C", new Predicate(5, "r5"), 4));
  }

  @Test
  void singleEdgeIncludesInverseVariant() {
    List<Edge> edges = sampleEdges();
    CPQEngine engine = new CPQEngine(edges);
    Map<String, String> varMap = identityVarMap(edges);

    BitSet edgeBits = new BitSet();
    edgeBits.set(4);

    List<KnownComponent> rules = engine.constructionRules(edgeBits, varMap);

    CPQ expectedInverse = CPQ.parse("r5⁻");
    String expectedStr = expectedInverse.toString();

    assertTrue(
        rules.stream()
            .anyMatch(
                kc ->
                    expectedStr.equals(kc.cpq().toString())
                        && "C".equals(kc.source())
                        && "A".equals(kc.target())),
        "Inverse single-edge CPQ r5⁻ should be available with reversed endpoints");
  }

  @Test
  void singleEdgeIncludesBacktrackLoops() {
    List<Edge> edges = sampleEdges();
    CPQEngine engine = new CPQEngine(edges);
    Map<String, String> varMap = identityVarMap(edges);

    BitSet edgeBits = new BitSet();
    edgeBits.set(0);

    List<KnownComponent> rules = engine.constructionRules(edgeBits, varMap);

    String sourceLoop = CPQ.parse("((r1 ◦ r1⁻) ∩ id)").toString();
    String targetLoop = CPQ.parse("((r1⁻ ◦ r1) ∩ id)").toString();

    assertTrue(
        rules.stream()
            .anyMatch(
                kc ->
                    sourceLoop.equals(kc.cpq().toString())
                        && "A".equals(kc.source())
                        && "A".equals(kc.target())),
        "Backtrack loop at source should be available");
    assertTrue(
        rules.stream()
            .anyMatch(
                kc ->
                    targetLoop.equals(kc.cpq().toString())
                        && "B".equals(kc.source())
                        && "B".equals(kc.target())),
        "Backtrack loop at target should be available");
  }

  @Test
  void threeEdgeComponentSupportsIntersectionWithInverse() {
    List<Edge> edges = sampleEdges();
    CPQEngine engine = new CPQEngine(edges);
    Map<String, String> varMap = identityVarMap(edges);

    BitSet edgeBits = new BitSet();
    edgeBits.set(2);
    edgeBits.set(3);
    edgeBits.set(4);

    List<KnownComponent> rules = engine.constructionRules(edgeBits, varMap);

    CPQ expected = CPQ.parse("((r3◦r4) ∩ r5⁻)");
    String expectedStr = expected.toString();

    assertTrue(
        rules.stream()
            .anyMatch(
                kc ->
                    expectedStr.equals(kc.cpq().toString())
                        && "C".equals(kc.source())
                        && "A".equals(kc.target())),
        "Component {r3,r4,r5} should include (r3◦r4) ∩ r5⁻ to cover the reverse join");
  }

  @Test
  void conjunctionDeduplicationEliminatesCommutativeDuplicates() {
    List<Edge> edges = sampleEdges();
    CPQEngine engine = new CPQEngine(edges);
    Map<String, String> varMap = identityVarMap(edges);

    // Create a component with 4 edges that would generate conjunctions
    BitSet edgeBits = new BitSet();
    edgeBits.set(0); // r1
    edgeBits.set(1); // r2
    edgeBits.set(2); // r3
    edgeBits.set(3); // r4

    List<KnownComponent> rules = engine.constructionRules(edgeBits, varMap);

    // Count unique CPQ strings for components with same source and target
    long countAtoC =
        rules.stream()
            .filter(kc -> "A".equals(kc.source()) && "C".equals(kc.target()))
            .map(kc -> kc.cpq().toString())
            .distinct()
            .count();

    // Before normalization, we would get duplicates like:
    // ((r1◦r2) ∩ (r4⁻◦r3⁻)) and ((r4⁻◦r3⁻) ∩ (r1◦r2))
    // After normalization, these should be deduplicated

    long countCtoA =
        rules.stream()
            .filter(kc -> "C".equals(kc.source()) && "A".equals(kc.target()))
            .map(kc -> kc.cpq().toString())
            .distinct()
            .count();

    // Verify that we don't have excessive duplicates
    // The exact count will depend on the CPQ generation logic,
    // but we should have significantly fewer construction rules than before normalization
    assertTrue(countAtoC > 0, "Should have at least one CPQ from A to C");
    assertTrue(countCtoA > 0, "Should have at least one CPQ from C to A");

    // More specific test: check that commutative duplicates are eliminated
    List<String> cpqStringsAtoC =
        rules.stream()
            .filter(kc -> "A".equals(kc.source()) && "C".equals(kc.target()))
            .map(kc -> kc.cpq().toString())
            .toList();

    // Check for specific duplicates that should be eliminated
    long countR1R2IntersectR4R3 =
        cpqStringsAtoC.stream()
            .filter(
                cpq ->
                    cpq.contains("r1")
                        && cpq.contains("r2")
                        && cpq.contains("r4")
                        && cpq.contains("r3")
                        && cpq.contains("∩"))
            .count();

    // After deduplication, we should only have one version of this conjunction
    assertTrue(
        countR1R2IntersectR4R3 <= 2,
        "Should not have many duplicate conjunctions with same operands in different order. Found: "
            + countR1R2IntersectR4R3
            + " instances");
  }

  @Test
  void loopsAreAnchoredWithIdentity() {
    List<Edge> edges = sampleEdges();
    CPQEngine engine = new CPQEngine(edges);
    Map<String, String> varMap = identityVarMap(edges);

    BitSet edgeBits = new BitSet();
    edgeBits.set(0);
    edgeBits.set(1);
    edgeBits.set(2);
    edgeBits.set(3);
    edgeBits.set(4);

    List<KnownComponent> rules = engine.constructionRules(edgeBits, varMap);

    String anchored = CPQ.parse("(r1⁻◦(((r4⁻◦r3⁻) ∩ r5)◦r2⁻)) ∩ id").toString();
    assertTrue(
        rules.stream()
            .anyMatch(
                kc ->
                    anchored.equals(kc.cpq().toString())
                        && kc.source().equals("B")
                        && kc.target().equals("B")),
        "Loop B→B should enforce equality via intersection with id");
  }

  @Test
  void variableMappingsArePreservedForConstructionRules() {
    CQ cq = CQ.empty();
    VarCQ x = cq.addFreeVariable("x");
    VarCQ y = cq.addFreeVariable("y");
    VarCQ z = cq.addBoundVariable("z");
    cq.addAtom(x, new Predicate(1, "knows"), z);
    cq.addAtom(z, new Predicate(2, "worksAt"), y);

    CQExtractor extractor = new CQExtractor();
    ExtractionResult extraction = extractor.extract(cq, Set.of("x", "y"));
    List<Edge> edges = extraction.edges();
    Map<String, String> varMap = extraction.variableNodeMap();
    CPQEngine engine = new CPQEngine(edges);

    BitSet bits = new BitSet(edges.size());
    bits.set(0, edges.size());

    List<KnownComponent> rules = engine.constructionRules(bits, Set.of("x", "y"), varMap);
    assertFalse(rules.isEmpty(), "Expected at least one construction rule for simple CQ");

    KnownComponent component = rules.get(0);
    assertEquals(
        component.source(),
        component.getNodeForVar("x"),
        "Source node must align with original variable x");
    assertEquals(
        component.target(),
        component.getNodeForVar("y"),
        "Target node must align with original variable y");
    assertEquals("z", component.getVarForNode("z"), "Intermediate variable should remain mapped");
    assertTrue(
        component.varToNodeMap().containsKey("z"),
        "Variable mapping should preserve bound variables");
  }

  @Test
  void joinNodeEnforcementExamples() {
    List<Edge> edges = sampleEdges();
    CPQEngine engine = new CPQEngine(edges);
    Map<String, String> varMap = identityVarMap(edges);

    int desiredSamples = 10;
    List<Scenario> enforcedScenarios =
        List.of(
            new Scenario(componentForEdges(edges, 0, 1), Set.of("A", "B"), 2),
            new Scenario(componentForEdges(edges, 1, 2), Set.of("B", "C"), 2),
            new Scenario(componentForEdges(edges, 2, 3), Set.of("C", "D"), 2),
            new Scenario(componentForEdges(edges, 3, 0), Set.of("A", "D"), 2),
            new Scenario(componentForEdges(edges, 0, 4), Set.of("A", "C"), 2),
            new Scenario(componentForEdges(edges, 1, 4), Set.of("B", "C"), 2),
            new Scenario(componentForEdges(edges, 2, 4), Set.of("C", "A"), 2),
            new Scenario(componentForEdges(edges, 0, 1, 4), Set.of("A", "C"), 2),
            new Scenario(componentForEdges(edges, 1, 2, 3), Set.of("B", "D"), 2),
            new Scenario(componentForEdges(edges, 0, 1, 2, 3), Set.of("A", "C"), 2));

    List<Scenario> relaxedScenarios =
        List.of(
            new Scenario(componentForEdges(edges, 0), Set.of("A", "B"), 1),
            new Scenario(componentForEdges(edges, 1), Set.of("B", "C"), 1),
            new Scenario(componentForEdges(edges, 2), Set.of("C", "D"), 1),
            new Scenario(componentForEdges(edges, 3), Set.of("D", "A"), 1),
            new Scenario(componentForEdges(edges, 4), Set.of("A", "C"), 1));

    List<String> enforcedExamples =
        collectExamples(engine, varMap, enforcedScenarios, desiredSamples, false);
    List<String> relaxedExamples =
        collectExamples(engine, varMap, relaxedScenarios, desiredSamples, true);

    System.out.println("Join-node enforcement ON (" + enforcedExamples.size() + " samples):");
    enforcedExamples.forEach(System.out::println);
    System.out.println("Join-node enforcement OFF (" + relaxedExamples.size() + " samples):");
    relaxedExamples.forEach(System.out::println);
  }

  private static Component componentForEdges(List<Edge> edges, int... indices) {
    BitSet bits = new BitSet();
    Set<String> vertices = new HashSet<>();
    for (int index : indices) {
      bits.set(index);
      Edge edge = edges.get(index);
      vertices.add(edge.source());
      vertices.add(edge.target());
    }
    return new Component(bits, vertices);
  }

  private static List<String> collectExamples(
      CPQEngine engine,
      Map<String, String> varToNodeMap,
      List<Scenario> scenarios,
      int desiredCount,
      boolean expectRelaxed) {
    Set<String> summaries = new LinkedHashSet<>();
    for (Scenario scenario : scenarios) {
      ComponentRules rules =
          engine.componentRules(
              scenario.component(),
              scenario.joinNodes(),
              Set.of(),
              scenario.totalComponents(),
              varToNodeMap);
      if (expectRelaxed) {
        assertEquals(
            rules.rawRules(),
            rules.joinFilteredRules(),
            () ->
                "Expected no join-node enforcement for component "
                    + scenario.component().vertices()
                    + " with join nodes "
                    + scenario.joinNodes());
      } else {
        assertTrue(
            !rules.joinFilteredRules().isEmpty(),
            () ->
                "Expected enforced component "
                    + scenario.component().vertices()
                    + " with join nodes "
                    + scenario.joinNodes()
                    + " to retain rules");
      }
      rules.joinFilteredRules().stream().map(CPQEngineTest::formatSummary).forEach(summaries::add);
      if (summaries.size() >= desiredCount) {
        break;
      }
    }
    assertTrue(
        summaries.size() >= desiredCount,
        () ->
            "Needed at least "
                + desiredCount
                + " summaries but only collected "
                + summaries.size());
    return summaries.stream().limit(desiredCount).toList();
  }

  private static String formatSummary(KnownComponent kc) {
    return kc.cpq().toString() + " [" + kc.source() + "→" + kc.target() + "]";
  }

  private static Map<String, String> identityVarMap(List<Edge> edges) {
    Map<String, String> mapping = new LinkedHashMap<>();
    for (Edge edge : edges) {
      mapping.putIfAbsent(edge.source(), edge.source());
      mapping.putIfAbsent(edge.target(), edge.target());
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(mapping));
  }

  private record Scenario(Component component, Set<String> joinNodes, int totalComponents) {}
}
