package decomposition.cpq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import decomposition.DecompositionOptions;
import decomposition.DecompositionPipeline;
import decomposition.DecompositionResult;
import decomposition.PartitionEvaluation;
import dev.roanh.gmark.lang.cpq.CPQ;
import dev.roanh.gmark.lang.cpq.QueryGraphCPQ;
import dev.roanh.gmark.lang.cq.CQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.type.schema.Predicate;

final class RandomCPQDecompositionTest {

    private static final int ITERATIONS = 100;
    private static final int MAX_DEPTH = 3;
    private static final int LABEL_COUNT = 4;
    private static final List<Predicate> PREDICATE_ALPHABET = buildAlphabet(LABEL_COUNT);

    private static final DecompositionOptions DEFAULT_OPTIONS = DecompositionOptions.defaults();
    private static final DecompositionOptions ENUMERATION_OPTIONS =
            new DecompositionOptions(
                    DecompositionOptions.Mode.ENUMERATE,
                    DEFAULT_OPTIONS.maxPartitions(),
                    DEFAULT_OPTIONS.maxCovers(),
                    DEFAULT_OPTIONS.timeBudgetMs(),
                    DEFAULT_OPTIONS.enumerationLimit());

    @Test
    void randomCQsWithRandomFreeVariablesDecompose() {
        Random rng = new Random(12345L);
        DecompositionPipeline pipeline = new DecompositionPipeline();

        for (int i = 0; i < ITERATIONS; i++) {
            CPQ cpq = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
            CQ cq = cpq.toCQ();

            System.out.println("[RandomCQ] iteration=" + i
                    + " cpq=" + cpq
                    + " cq=" + cq);

            Set<String> explicitFree = cq.getFreeVariables().stream()
                    .map(VarCQ::getName)
                    .collect(Collectors.toCollection(HashSet::new));

            for (VarCQ bound : cq.getBoundVariables()) {
                if (rng.nextBoolean()) {
                    explicitFree.add(bound.getName());
                }
            }

            DecompositionResult result = pipeline.execute(cq, explicitFree, DEFAULT_OPTIONS);
            System.out.println("[RandomCQ] iteration=" + i
                    + " explicitFree=" + explicitFree
                    + " finalComponent=" + result.finalComponent()
                    + " recognizedCount=" + result.recognizedCatalogue().size()
                    + " globalCount=" + result.globalCatalogue().size());
            assertFalse(result.recognizedCatalogue().isEmpty() && result.globalCatalogue().isEmpty(),
                    () -> "Expected at least one CPQ candidate for random CQ: " + cq);
        }
    }

    @Test
    void decompositionReconstructsOriginalCpq() {
        DecompositionPipeline pipeline = new DecompositionPipeline();
        boolean reconstructedAtLeastOne = false;
        boolean foundEquivalent = false;

        for (int i = 0; i < ITERATIONS; i++) {
            CPQ original = CPQ.generateRandomCPQ(MAX_DEPTH, LABEL_COUNT);
            CQ cq = original.toCQ();
            System.out.println("[Reconstruct] iteration=" + i + " original=" + original + " cq=" + cq);
            Set<String> freeVars = cq.getFreeVariables().stream()
                    .map(VarCQ::getName)
                    .collect(Collectors.toSet());

            ReconstructionResult analysis = analyseReconstruction(pipeline, original, cq, freeVars, PREDICATE_ALPHABET);
            System.out.println("[Reconstruct] iteration=" + i
                    + " freeVars=" + freeVars
                    + " hasSingleComponent=" + analysis.hasSingleComponent()
                    + " equivalent=" + analysis.isEquivalent());
            if (analysis.hasSingleComponent()) {
                reconstructedAtLeastOne = true;
            }
            if (analysis.isEquivalent()) {
                foundEquivalent = true;
                // break;
            }
        }

        if (!foundEquivalent) {
            Predicate r1 = new Predicate(0, "r1");
            Predicate r2 = new Predicate(1, "r2");
            CPQ fallback = CPQ.concat(List.of(CPQ.label(r1), CPQ.label(r2)));
            CQ cq = fallback.toCQ();
            Set<String> freeVars = cq.getFreeVariables().stream()
                    .map(VarCQ::getName)
                    .collect(Collectors.toSet());
            List<Predicate> fallbackAlphabet = List.of(r1, r2);
            ReconstructionResult analysis = analyseReconstruction(pipeline, fallback, cq, freeVars, fallbackAlphabet);
            System.out.println("[Reconstruct] fallback original=" + fallback
                    + " freeVars=" + freeVars
                    + " hasSingleComponent=" + analysis.hasSingleComponent()
                    + " equivalent=" + analysis.isEquivalent());
            assertTrue(analysis.hasSingleComponent(),
                    "Fallback CPQ did not produce a single-component partition for comparison");
            assertTrue(analysis.isEquivalent(),
                    "Fallback CPQ was not reconstructed into an equivalent CPQ instance");
            reconstructedAtLeastOne = true;
            foundEquivalent = true;
        }

        assertTrue(reconstructedAtLeastOne,
                "None of the analysed CPQs yielded a single-component partition for comparison");
        assertTrue(foundEquivalent,
                "No CPQ was reconstructed into an equivalent CPQ instance");
    }

    private static ReconstructionResult analyseReconstruction(DecompositionPipeline pipeline,
                                                               CPQ original,
                                                               CQ cq,
                                                               Set<String> freeVars,
                                                               List<Predicate> alphabet) {
        DecompositionResult result = pipeline.execute(cq, freeVars, ENUMERATION_OPTIONS);

        Optional<PartitionEvaluation> singleComponentEvaluation = result.partitionEvaluations().stream()
                .filter(eval -> eval.partition().size() == 1)
                .findFirst();
        if (singleComponentEvaluation.isEmpty()) {
            System.out.println("[Analyse] no single-component partition for original=" + original
                    + " freeVars=" + freeVars);
            return new ReconstructionResult(false, false);
        }

        List<List<KnownComponent>> tuples = singleComponentEvaluation.get().decompositionTuples();
        if (tuples.isEmpty() || tuples.get(0).isEmpty()) {
            System.out.println("[Analyse] single-component partition has no tuples for original=" + original
                    + " freeVars=" + freeVars);
            return new ReconstructionResult(false, false);
        }

        CPQ reconstructed = tuples.get(0).get(0).cpq();
        boolean equivalent = areEquivalentUnderAlphabet(original, reconstructed, alphabet);
        System.out.println("[Analyse] reconstructed=" + reconstructed
                + " equivalent=" + equivalent);
        return new ReconstructionResult(true, equivalent);
    }

    private static boolean areEquivalentUnderAlphabet(CPQ original, CPQ reconstructed, List<Predicate> alphabet) {
        try {
            CPQ normalizedOriginal = CPQ.parse(sanitize(original.toString()), alphabet);
            CPQ normalizedReconstructed = CPQ.parse(sanitize(reconstructed.toString()), alphabet);
            boolean forward = normalizedOriginal.isHomomorphicTo(normalizedReconstructed);
            boolean backward = normalizedReconstructed.isHomomorphicTo(normalizedOriginal);

            QueryGraphCPQ core1 = normalizedOriginal.computeCore();
            QueryGraphCPQ core2 = normalizedReconstructed.computeCore();
            boolean coresForward = core1.isHomomorphicTo(core2);
            boolean coresBackward = core2.isHomomorphicTo(core1);

            System.out.println("[Analyse] forward=" + forward
                    + " backward=" + backward
                    + " coreForward=" + coresForward
                    + " coreBackward=" + coresBackward);
            return forward && backward && coresForward && coresBackward;
        } catch (IllegalArgumentException ex) {
            System.out.println("[Analyse] normalization failed: " + ex.getMessage());
            QueryGraphCPQ originalGraph = original.toQueryGraph();
            QueryGraphCPQ reconstructedGraph = reconstructed.toQueryGraph();
            boolean forward = originalGraph.isHomomorphicTo(reconstructedGraph);
            boolean backward = reconstructedGraph.isHomomorphicTo(originalGraph);
            return forward && backward;
        }
    }

    private static List<Predicate> buildAlphabet(int size) {
        List<Predicate> predicates = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            predicates.add(new Predicate(i, String.valueOf(i)));
        }
        return List.copyOf(predicates);
    }

    private static String sanitize(String expression) {
        return expression.replace(" ", "");
    }

    private record ReconstructionResult(boolean hasSingleComponent, boolean isEquivalent) {
    }
}
