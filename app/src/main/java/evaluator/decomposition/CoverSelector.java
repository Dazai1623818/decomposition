package evaluator.decomposition;

import dev.roanh.gmark.lang.cq.AtomCQ;
import dev.roanh.gmark.lang.cq.VarCQ;
import dev.roanh.gmark.lang.cpq.CPQ;
import evaluator.decomposition.Decomposer.DecompositionTimeoutException;
import evaluator.cpq.Plan.Component;
import evaluator.cpq.Plan;
import evaluator.cpq.ConjunctiveQuery;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

final class CoverSelector {
	enum Order {
		COST,
		DIAMETER
	}

	private final int limit;
	private final Order order;
	private final ToLongFunction<CPQ> costFn;
	private final long deadlineNanos;

	CoverSelector(int limit) {
		this(limit, Order.COST, cpq -> 0L, Long.MAX_VALUE);
	}

	CoverSelector(int limit, Order order, ToLongFunction<CPQ> costFn) {
		this(limit, order, costFn, Long.MAX_VALUE);
	}

	CoverSelector(int limit, Order order, ToLongFunction<CPQ> costFn, long deadlineNanos) {
		if (limit < 0) {
			throw new IllegalArgumentException("limit must be >= 0");
		}
		this.limit = limit;
		this.order = Objects.requireNonNull(order, "order");
		this.costFn = costFn;
		this.deadlineNanos = deadlineNanos;
	}

	public Stream<Plan> select(ConjunctiveQuery query, List<Component> components) {
		List<Plan> results = new ArrayList<>();
		new Solver(query, components, order, costFn, deadlineNanos).search(limit, results::add);
		return results.stream();
	}

	private static final class Option {
		final Component comp;
		final int id;
		final BitSet mask;
		final BitSet vars;
		final BitSet endpoints;
		final long cost;

		Option(Component comp, int id, Map<VarCQ, Integer> varIndex, ConjunctiveQuery query, long cost) {
			this.comp = comp;
			this.id = id;
			this.mask = comp.maskUnsafe();
			this.vars = new BitSet(varIndex.size());
			this.endpoints = new BitSet(varIndex.size());
			this.cost = cost;

			// Inline variable mapping
			Integer sIdx = varIndex.get(comp.s());
			if (sIdx != null)
				endpoints.set(sIdx);

			Integer tIdx = varIndex.get(comp.t());
			if (tIdx != null)
				endpoints.set(tIdx);

			for (int e = mask.nextSetBit(0); e >= 0; e = mask.nextSetBit(e + 1)) {
				AtomCQ edge = query.edges().get(e);
				Integer u = varIndex.get(edge.getSource());
				if (u != null)
					vars.set(u);
				Integer v = varIndex.get(edge.getTarget());
				if (v != null)
					vars.set(v);
			}
		}

		/**
		 * Checks if this option respects the boundaries defined by the shared
		 * variables.
		 * Any shared variable used by this option MUST be one of its exposed endpoints.
		 */
		boolean isCompatible(BitSet shared) {
			if (!vars.intersects(shared)) {
				return true;
			}
			BitSet bad = (BitSet) vars.clone();
			bad.and(shared);
			bad.andNot(endpoints);
			return bad.isEmpty();
		}
	}

	private static final class Solver {
		private static final long SATURATED_COST = Long.MAX_VALUE;

		private final ConjunctiveQuery query;
		private final int numEdges;
		private final int numVars;
		private final Set<VarCQ> requiredFree;
		private final List<List<Option>> cover;
		private final Order order;
		private final Set<String> seenCoverKeys = new HashSet<>();
		private final long deadlineNanos;

		private int outputs;
		private int maxOutputs;
		private Consumer<Plan> out;
		private PriorityQueue<ScoredPlan> bestPlans;

		Solver(
				ConjunctiveQuery query,
				List<Component> components,
				Order order,
				ToLongFunction<CPQ> costFn,
				long deadlineNanos) {
			this.query = query;
			List<VarCQ> vertices = query.vertices();
			List<AtomCQ> edges = query.edges();
			this.numEdges = edges.size();
			this.numVars = vertices.size();
			this.requiredFree = new HashSet<>(query.freeVariables());
			this.order = Objects.requireNonNull(order, "order");
			this.deadlineNanos = deadlineNanos;

			Map<VarCQ, Integer> varIndex = new HashMap<>(numVars);
			for (int i = 0; i < numVars; i++) {
				varIndex.put(vertices.get(i), i);
			}

			List<Option> allOptions = new ArrayList<>(components.size());
			for (int i = 0; i < components.size(); i++) {
				Component comp = components.get(i);
				long cost = costFn == null ? 0L : costFn.applyAsLong(comp.cpq());
				allOptions.add(new Option(comp, i, varIndex, query, cost));
			}

			this.cover = new ArrayList<>(numEdges);
			for (int i = 0; i < numEdges; i++) {
				cover.add(new ArrayList<>());
			}
			for (Option opt : allOptions) {
				for (int e = opt.mask.nextSetBit(0); e >= 0; e = opt.mask.nextSetBit(e + 1)) {
					cover.get(e).add(opt);
				}
			}

			for (int e = 0; e < numEdges; e++) {
				cover.get(e).sort(optionOrder());
			}
		}

		private Comparator<Option> optionOrder() {
			return switch (order) {
				case COST -> Comparator
						.comparingLong((Option opt) -> normalizeCost(opt.cost))
						.thenComparing(
								Comparator.comparingInt((Option opt) -> opt.mask.cardinality()).reversed())
						.thenComparingInt(opt -> opt.id);
				case DIAMETER -> Comparator
						.comparingInt((Option opt) -> opt.comp.diameter()).reversed()
						.thenComparing(
								Comparator.comparingInt((Option opt) -> opt.mask.cardinality()).reversed())
						.thenComparingLong(opt -> opt.cost)
						.thenComparingInt(opt -> opt.id);
			};
		}

		void search(int limit, Consumer<Plan> out) {
			checkDeadline();
			this.out = out;
			this.outputs = 0;
			this.maxOutputs = limit == 0 ? Integer.MAX_VALUE : limit;
			this.seenCoverKeys.clear();
			this.bestPlans = null;

			// Preserve exhaustive enumeration semantics for unbounded output.
			if (this.maxOutputs == Integer.MAX_VALUE) {
				visit(new BitSet(numEdges), new HashSet<>(), new ArrayList<>());
				return;
			}

			// For bounded COST/DIAMETER selection, keep the top-k covers by score
			// and prune subtrees whose optimistic bound cannot beat the current worst.
			this.bestPlans = new PriorityQueue<>(this.maxOutputs, this::compareScoredPlansWorstFirst);
			visitWithBranchAndBound(
					new BitSet(numEdges),
					new HashSet<>(),
					new ArrayList<>(),
					0L,
					0,
					0);

			List<ScoredPlan> ordered = new ArrayList<>(bestPlans);
			ordered.sort((left, right) -> {
				int cmp = compareScore(left.score(), right.score());
				if (cmp != 0) {
					return cmp;
				}
				return left.coverKey().compareTo(right.coverKey());
			});
			for (ScoredPlan plan : ordered) {
				out.accept(plan.plan());
				outputs++;
			}
		}

		private void visit(BitSet covered, Set<VarCQ> freeCovered, ArrayList<Option> chosen) {
			checkDeadline();
			if (outputs >= maxOutputs) {
				return;
			}
			if (!hasFeasibleContinuation(covered)) {
				return;
			}

			int nextEdge = covered.nextClearBit(0);
			if (nextEdge >= numEdges) {
				if (isValid(chosen, freeCovered)) {
					String coverKey = coverKey(chosen);
					if (seenCoverKeys.add(coverKey)) {
						out.accept(buildPlan(chosen));
						outputs++;
					}
				}
				return;
			}

			for (Option opt : cover.get(nextEdge)) {
				checkDeadline();
				if (opt.mask.intersects(covered)) {
					continue;
				}

				BitSet nextCovered = (BitSet) covered.clone();
				nextCovered.or(opt.mask);

				Set<VarCQ> nextFree = new HashSet<>(freeCovered);
				if (requiredFree.contains(opt.comp.s()))
					nextFree.add(opt.comp.s());
				if (requiredFree.contains(opt.comp.t()))
					nextFree.add(opt.comp.t());

				chosen.add(opt);
				visit(nextCovered, nextFree, chosen);
				chosen.remove(chosen.size() - 1);

				if (outputs >= maxOutputs) {
					return;
				}
			}
		}

		private void visitWithBranchAndBound(
				BitSet covered,
				Set<VarCQ> freeCovered,
				ArrayList<Option> chosen,
				long totalCost,
				int maxDiameter,
				int maxMaskCardinality) {
			checkDeadline();

			Score lowerBound = lowerBound(
					covered,
					chosen.size(),
					totalCost,
					maxDiameter,
					maxMaskCardinality);
			if (lowerBound == null) {
				return;
			}
			if (bestPlans.size() == maxOutputs) {
				ScoredPlan worst = bestPlans.peek();
				if (worst != null && compareScore(lowerBound, worst.score()) >= 0) {
					return;
				}
			}

			int nextEdge = covered.nextClearBit(0);
			if (nextEdge >= numEdges) {
				if (!isValid(chosen, freeCovered)) {
					return;
				}
				String coverKey = coverKey(chosen);
				if (!seenCoverKeys.add(coverKey)) {
					return;
				}
				Plan plan = buildPlan(chosen);
				Score score = score(chosen.size(), totalCost, maxDiameter, maxMaskCardinality);
				offerBest(new ScoredPlan(plan, score, coverKey));
				return;
			}

			for (Option opt : cover.get(nextEdge)) {
				checkDeadline();
				if (opt.mask.intersects(covered)) {
					continue;
				}

				BitSet nextCovered = (BitSet) covered.clone();
				nextCovered.or(opt.mask);

				Set<VarCQ> nextFree = new HashSet<>(freeCovered);
				if (requiredFree.contains(opt.comp.s())) {
					nextFree.add(opt.comp.s());
				}
				if (requiredFree.contains(opt.comp.t())) {
					nextFree.add(opt.comp.t());
				}

				chosen.add(opt);
				visitWithBranchAndBound(
						nextCovered,
						nextFree,
						chosen,
						safeAddCost(totalCost, opt.cost),
						Math.max(maxDiameter, opt.comp.diameter()),
						Math.max(maxMaskCardinality, opt.mask.cardinality()));
				chosen.remove(chosen.size() - 1);
			}
		}

		private void offerBest(ScoredPlan candidate) {
			if (bestPlans.size() < maxOutputs) {
				bestPlans.add(candidate);
				return;
			}
			ScoredPlan worst = bestPlans.peek();
			if (worst == null) {
				bestPlans.add(candidate);
				return;
			}
			int cmp = compareScore(candidate.score(), worst.score());
			if (cmp < 0 || (cmp == 0 && candidate.coverKey().compareTo(worst.coverKey()) < 0)) {
				bestPlans.poll();
				bestPlans.add(candidate);
			}
		}

		private Score lowerBound(
				BitSet covered,
				int chosenCount,
				long totalCost,
				int maxDiameter,
				int maxMaskCardinality) {
			int uncoveredEdges = numEdges - covered.cardinality();
			if (uncoveredEdges == 0) {
				return score(chosenCount, totalCost, maxDiameter, maxMaskCardinality);
			}

			int optimisticMaxDiameter = maxDiameter;
			int optimisticMaxMask = maxMaskCardinality;
			int maxCoverableEdges = 0;
			long minAdditionalCost = SATURATED_COST;

			for (int edge = covered.nextClearBit(0); edge >= 0
					&& edge < numEdges; edge = covered.nextClearBit(edge + 1)) {
				boolean feasible = false;
				for (Option option : cover.get(edge)) {
					if (option.mask.intersects(covered)) {
						continue;
					}
					feasible = true;
					optimisticMaxDiameter = Math.max(optimisticMaxDiameter, option.comp.diameter());
					optimisticMaxMask = Math.max(optimisticMaxMask, option.mask.cardinality());
					maxCoverableEdges = Math.max(maxCoverableEdges, option.mask.cardinality());
					minAdditionalCost = Math.min(minAdditionalCost, normalizeCost(option.cost));
				}
				if (!feasible) {
					return null;
				}
			}

			if (maxCoverableEdges == 0) {
				return null;
			}

			int minAdditionalComponents = (uncoveredEdges + maxCoverableEdges - 1) / maxCoverableEdges;
			long optimisticCost = totalCost;
			if (order == Order.COST && minAdditionalCost != SATURATED_COST) {
				optimisticCost = safeAddCost(totalCost, safeMultiplyCost(minAdditionalCost, minAdditionalComponents));
			}
			return score(
					chosenCount + minAdditionalComponents,
					optimisticCost,
					optimisticMaxDiameter,
					optimisticMaxMask);
		}

		private boolean hasFeasibleContinuation(BitSet covered) {
			for (int edge = covered.nextClearBit(0); edge >= 0
					&& edge < numEdges; edge = covered.nextClearBit(edge + 1)) {
				boolean feasible = false;
				for (Option option : cover.get(edge)) {
					if (!option.mask.intersects(covered)) {
						feasible = true;
						break;
					}
				}
				if (!feasible) {
					return false;
				}
			}
			return true;
		}

		private static long normalizeCost(long rawCost) {
			if (rawCost < 0L) {
				return 0L;
			}
			return rawCost;
		}

		private static long safeAddCost(long left, long right) {
			long normalizedRight = normalizeCost(right);
			if (left >= SATURATED_COST || normalizedRight >= SATURATED_COST) {
				return SATURATED_COST;
			}
			if (left > SATURATED_COST - normalizedRight) {
				return SATURATED_COST;
			}
			return left + normalizedRight;
		}

		private static long safeMultiplyCost(long value, int factor) {
			long normalized = normalizeCost(value);
			if (factor <= 0) {
				return 0L;
			}
			if (normalized >= SATURATED_COST) {
				return SATURATED_COST;
			}
			if (normalized > SATURATED_COST / factor) {
				return SATURATED_COST;
			}
			return normalized * factor;
		}

		private Score score(
				int components,
				long totalCost,
				int maxDiameter,
				int maxMaskCardinality) {
			return new Score(totalCost, components, maxMaskCardinality, maxDiameter);
		}

		private Plan buildPlan(List<Option> chosen) {
			ArrayList<Option> sorted = new ArrayList<>(chosen);
			sorted.sort(Comparator.comparingInt(opt -> opt.id));
			ArrayList<Component> comps = new ArrayList<>(sorted.size());
			for (Option opt : sorted) {
				comps.add(opt.comp);
			}
			return new Plan(query, comps);
		}

		/**
		 * Comparator over score tuples where lower is better.
		 */
		private int compareScore(Score left, Score right) {
			return switch (order) {
				case COST -> compareScoreCost(left, right);
				case DIAMETER -> compareScoreDiameter(left, right);
			};
		}

		private static int compareScoreCost(Score left, Score right) {
			int cmp = Long.compare(left.totalCost(), right.totalCost());
			if (cmp != 0) {
				return cmp;
			}
			return Integer.compare(right.maxMaskCardinality(), left.maxMaskCardinality());
		}

		private static int compareScoreDiameter(Score left, Score right) {
			int cmp = Integer.compare(right.maxDiameter(), left.maxDiameter());
			if (cmp != 0) {
				return cmp;
			}
			cmp = Integer.compare(right.maxMaskCardinality(), left.maxMaskCardinality());
			if (cmp != 0) {
				return cmp;
			}
			cmp = Long.compare(left.totalCost(), right.totalCost());
			if (cmp != 0) {
				return cmp;
			}
			cmp = Integer.compare(left.components(), right.components());
			if (cmp != 0) {
				return cmp;
			}
			return 0;
		}

		private int compareScoredPlansWorstFirst(ScoredPlan left, ScoredPlan right) {
			int cmp = compareScore(right.score(), left.score());
			if (cmp != 0) {
				return cmp;
			}
			return right.coverKey().compareTo(left.coverKey());
		}

		private record Score(
				long totalCost,
				int components,
				int maxMaskCardinality,
				int maxDiameter) {
		}

		private record ScoredPlan(
				Plan plan,
				Score score,
				String coverKey) {
		}

		private void checkDeadline() {
			if (Thread.currentThread().isInterrupted()) {
				throw new DecompositionTimeoutException("Cover selection interrupted");
			}
			if (deadlineNanos != Long.MAX_VALUE && System.nanoTime() >= deadlineNanos) {
				throw new DecompositionTimeoutException("Cover selection timed out");
			}
		}

		private boolean isValid(List<Option> chosen, Set<VarCQ> freeCovered) {
			if (!freeCovered.containsAll(requiredFree)) {
				return false;
			}
			if (numVars == 0 || chosen.size() < 2) {
				return true;
			}

			int[] counts = new int[numVars];
			for (Option opt : chosen) {
				BitSet vs = opt.vars;
				for (int v = vs.nextSetBit(0); v >= 0; v = vs.nextSetBit(v + 1)) {
					counts[v]++;
				}
			}

			BitSet shared = new BitSet(numVars);
			for (int v = 0; v < numVars; v++) {
				if (counts[v] >= 2) {
					shared.set(v);
				}
			}

			if (shared.isEmpty()) {
				return true;
			}

			for (Option opt : chosen) {
				if (!opt.isCompatible(shared)) {
					return false;
				}
			}
			return true;
		}

		private static String coverKey(List<Option> chosen) {
			ArrayList<String> signatures = new ArrayList<>(chosen.size());
			for (Option option : chosen) {
				signatures.add(option.comp.signature());
			}
			signatures.sort(String::compareTo);
			return String.join("|", signatures);
		}
	}
}
