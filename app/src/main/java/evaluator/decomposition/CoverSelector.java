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
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

final class CoverSelector {
	private static final String STRATEGY_PROPERTY = "cpq.coverSelector.strategy";
	private static final int MIN_BEAM_WIDTH = 128;

	enum Order {
		COST,
		MAX_COLLAPSE
	}

	private enum Strategy {
		BASELINE,
		ORDERED,
		BEAM
	}

	private final int limit;
	private final Order order;
	private final ToLongFunction<CPQ> costFn;
	private final Predicate<Plan> planFilter;
	private final long deadlineNanos;
	private final Strategy strategy;

	CoverSelector(int limit) {
		this(limit, Order.COST, cpq -> 0L, null, Long.MAX_VALUE);
	}

	CoverSelector(int limit, Order order, ToLongFunction<CPQ> costFn) {
		this(limit, order, costFn, null, Long.MAX_VALUE);
	}

	CoverSelector(int limit, Order order, ToLongFunction<CPQ> costFn, long deadlineNanos) {
		this(limit, order, costFn, null, deadlineNanos);
	}

	CoverSelector(
			int limit,
			Order order,
			ToLongFunction<CPQ> costFn,
			Predicate<Plan> planFilter,
			long deadlineNanos) {
		if (limit < 0) {
			throw new IllegalArgumentException("limit must be >= 0");
		}
		this.limit = limit;
		this.order = Objects.requireNonNull(order, "order");
		this.costFn = costFn;
		this.planFilter = planFilter;
		this.deadlineNanos = deadlineNanos;
		this.strategy = parseStrategy();
	}

	public Stream<Plan> select(ConjunctiveQuery query, List<Component> components) {
		List<Plan> results = new ArrayList<>();
		new Solver(query, components, order, costFn, planFilter, deadlineNanos, strategy).search(limit, results::add);
		return results.stream();
	}

	private static Strategy parseStrategy() {
		String raw = System.getProperty(STRATEGY_PROPERTY);
		if (raw == null || raw.isBlank()) {
			return Strategy.BASELINE;
		}
		return switch (raw.trim().toLowerCase()) {
			case "baseline" -> Strategy.BASELINE;
			case "ordered" -> Strategy.ORDERED;
			case "beam" -> Strategy.BEAM;
			default -> throw new IllegalArgumentException(
					"Unknown cover selector strategy '" + raw + "' in system property " + STRATEGY_PROPERTY);
		};
	}

	private static final class Option {
		final Component comp;
		final int id;
		final BitSet mask;
		final BitSet vars;
		final BitSet endpoints;
		final int[] varIndexes;
		final int[] requiredFreeEndpointIndexes;
		final long cost;

		Option(
				Component comp,
				int id,
				Map<VarCQ, Integer> varIndex,
				ConjunctiveQuery query,
				BitSet requiredFreeMask,
				long cost) {
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
			BitSet requiredFreeEndpoints = (BitSet) endpoints.clone();
			requiredFreeEndpoints.and(requiredFreeMask);
			this.varIndexes = toIndexes(vars);
			this.requiredFreeEndpointIndexes = toIndexes(requiredFreeEndpoints);
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

		private static int[] toIndexes(BitSet bits) {
			int[] indexes = new int[bits.cardinality()];
			int cursor = 0;
			for (int bit = bits.nextSetBit(0); bit >= 0; bit = bits.nextSetBit(bit + 1)) {
				indexes[cursor++] = bit;
			}
			return indexes;
		}
	}

	private static final class Solver {
		private final ConjunctiveQuery query;
		private final int numEdges;
		private final int numVars;
		private final int requiredFreeCount;
		private final List<List<Option>> cover;
		private final Order order;
		private final Predicate<Plan> planFilter;
		private final Set<String> seenCoverKeys = new HashSet<>();
		private final long deadlineNanos;
		private final Strategy strategy;
		private final int[] varUseCounts;
		private final int[] freeCoverCounts;

		private int maxOutputs;
		private Consumer<Plan> out;
		private List<ScoredPlan> scoredPlans;

		Solver(
				ConjunctiveQuery query,
				List<Component> components,
				Order order,
				ToLongFunction<CPQ> costFn,
				Predicate<Plan> planFilter,
				long deadlineNanos,
				Strategy strategy) {
			this.query = query;
			List<VarCQ> vertices = query.vertices();
			List<AtomCQ> edges = query.edges();
			this.numEdges = edges.size();
			this.numVars = vertices.size();
			Map<VarCQ, Integer> varIndex = new HashMap<>(numVars);
			for (int i = 0; i < numVars; i++) {
				varIndex.put(vertices.get(i), i);
			}
			BitSet requiredFreeMask = new BitSet(numVars);
			for (VarCQ variable : query.freeVariables()) {
				Integer bit = varIndex.get(variable);
				if (bit != null) {
					requiredFreeMask.set(bit);
				}
			}
			this.requiredFreeCount = requiredFreeMask.cardinality();
			this.order = Objects.requireNonNull(order, "order");
			this.planFilter = planFilter;
			this.deadlineNanos = deadlineNanos;
			this.strategy = Objects.requireNonNull(strategy, "strategy");
			this.varUseCounts = new int[numVars];
			this.freeCoverCounts = new int[numVars];

			List<Option> allOptions = new ArrayList<>(components.size());
			for (int i = 0; i < components.size(); i++) {
				Component comp = components.get(i);
				long cost = costFn == null ? 0L : costFn.applyAsLong(comp.cpq());
				allOptions.add(new Option(comp, i, varIndex, query, requiredFreeMask, cost));
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
			if (strategy != Strategy.BASELINE) {
				for (int e = 0; e < numEdges; e++) {
					cover.get(e).sort(optionOrder());
				}
			}
		}

		void search(int limit, Consumer<Plan> out) {
			checkDeadline();
			this.out = out;
			this.maxOutputs = limit == 0 ? Integer.MAX_VALUE : limit;
			this.seenCoverKeys.clear();
			this.scoredPlans = new ArrayList<>();
			java.util.Arrays.fill(varUseCounts, 0);
			java.util.Arrays.fill(freeCoverCounts, 0);
			if (strategy == Strategy.BEAM && maxOutputs != Integer.MAX_VALUE) {
				searchBeam();
				emitPlans(scoredPlans);
				return;
			}
			visit(new BitSet(numEdges), new BitSet(numVars), new ArrayList<>(), 0L);
			emitPlans(scoredPlans);
		}

		private Comparator<Option> optionOrder() {
			return switch (order) {
				case COST -> Comparator
						.comparingLong((Option opt) -> normalizeCost(opt.cost))
						.thenComparing(
								Comparator.comparingInt((Option opt) -> opt.mask.cardinality()).reversed())
						.thenComparingInt(opt -> opt.id);
				case MAX_COLLAPSE -> Comparator
						.comparingInt((Option opt) -> opt.comp.diameter()).reversed()
						.thenComparing(
								Comparator.comparingInt((Option opt) -> opt.mask.cardinality()).reversed())
						.thenComparingLong(opt -> normalizeCost(opt.cost))
						.thenComparingInt(opt -> opt.id);
			};
		}

		private void visit(
				BitSet covered,
				BitSet freeCovered,
				ArrayList<Option> chosen,
				long totalCost) {
			checkDeadline();
			if (scoredPlans.size() >= maxOutputs) {
				return;
			}
			if (!hasFeasibleContinuation(covered)) {
				return;
			}

			int nextEdge = covered.nextClearBit(0);
			if (nextEdge >= numEdges) {
				if (!isValid(chosen, freeCovered)) {
					return;
				}
				Plan plan = buildPlan(chosen);
				if (planFilter != null && !planFilter.test(plan)) {
					return;
				}
				String coverKey = coverKey(chosen);
				if (!seenCoverKeys.add(coverKey)) {
					return;
				}
				scoredPlans.add(new ScoredPlan(
						plan,
						score(chosen, totalCost),
						coverKey));
				return;
			}

			for (Option opt : cover.get(nextEdge)) {
				checkDeadline();
				if (opt.mask.intersects(covered)) {
					continue;
				}
				applyOption(opt, covered, freeCovered);
				chosen.add(opt);
				visit(
						covered,
						freeCovered,
						chosen,
						safeAddCost(totalCost, opt.cost));
				chosen.remove(chosen.size() - 1);
				undoOption(opt, covered, freeCovered);
			}
		}

		/**
		 * Heuristic beam search that keeps only the most promising partial covers
		 * at each expansion depth. This is intentionally approximate.
		 */
		private void searchBeam() {
			List<BeamState> frontier = List.of(new BeamState(
					new BitSet(numEdges),
					new BitSet(numVars),
					List.of(),
					new int[numVars],
					0L,
					0));
			int beamWidth = Math.max(maxOutputs, MIN_BEAM_WIDTH);
			while (!frontier.isEmpty() && scoredPlans.size() < maxOutputs) {
				checkDeadline();
				ArrayList<BeamState> nextFrontier = new ArrayList<>();
				for (BeamState state : frontier) {
					checkDeadline();
					if (!hasFeasibleContinuation(state.covered())) {
						continue;
					}
					int nextEdge = state.covered().nextClearBit(0);
					if (nextEdge >= numEdges) {
						maybeRecordComplete(state);
						if (scoredPlans.size() >= maxOutputs) {
							return;
						}
						continue;
					}
					for (Option opt : cover.get(nextEdge)) {
						if (opt.mask.intersects(state.covered())) {
							continue;
						}
						nextFrontier.add(extend(state, opt));
					}
				}
				if (nextFrontier.isEmpty()) {
					return;
				}
				nextFrontier.sort(this::compareBeamState);
				if (nextFrontier.size() > beamWidth) {
					nextFrontier.subList(beamWidth, nextFrontier.size()).clear();
				}
				frontier = nextFrontier;
			}
		}

		private void maybeRecordComplete(BeamState state) {
			if (!isValid(state.chosen(), state.freeCovered(), state.varUseCounts())) {
				return;
			}
			Plan plan = buildPlan(state.chosen());
			if (planFilter != null && !planFilter.test(plan)) {
				return;
			}
			String coverKey = coverKey(state.chosen());
			if (!seenCoverKeys.add(coverKey)) {
				return;
			}
			scoredPlans.add(new ScoredPlan(plan, score(state.chosen(), state.totalCost()), coverKey));
		}

		private BeamState extend(BeamState state, Option opt) {
			BitSet nextCovered = (BitSet) state.covered().clone();
			nextCovered.or(opt.mask);
			BitSet nextFreeCovered = (BitSet) state.freeCovered().clone();
			for (int endpoint : opt.requiredFreeEndpointIndexes) {
				nextFreeCovered.set(endpoint);
			}
			int[] nextVarUseCounts = java.util.Arrays.copyOf(state.varUseCounts(), numVars);
			for (int var : opt.varIndexes) {
				nextVarUseCounts[var]++;
			}
			ArrayList<Option> nextChosen = new ArrayList<>(state.chosen().size() + 1);
			nextChosen.addAll(state.chosen());
			nextChosen.add(opt);
			return new BeamState(
					nextCovered,
					nextFreeCovered,
					List.copyOf(nextChosen),
					nextVarUseCounts,
					safeAddCost(state.totalCost(), opt.cost),
					Math.max(state.maxDiameter(), opt.comp.diameter()));
		}

		private int compareBeamState(BeamState left, BeamState right) {
			int cmp = Integer.compare(right.covered().cardinality(), left.covered().cardinality());
			if (cmp != 0) {
				return cmp;
			}
			return switch (order) {
				case COST -> {
					cmp = Long.compare(left.totalCost(), right.totalCost());
					if (cmp != 0) {
						yield cmp;
					}
					yield Integer.compare(right.maxDiameter(), left.maxDiameter());
				}
				case MAX_COLLAPSE -> {
					cmp = Integer.compare(right.maxDiameter(), left.maxDiameter());
					if (cmp != 0) {
						yield cmp;
					}
					yield Long.compare(left.totalCost(), right.totalCost());
				}
			};
		}

		private void applyOption(Option opt, BitSet covered, BitSet freeCovered) {
			covered.or(opt.mask);
			for (int var : opt.varIndexes) {
				varUseCounts[var]++;
			}
			for (int endpoint : opt.requiredFreeEndpointIndexes) {
				if (freeCoverCounts[endpoint]++ == 0) {
					freeCovered.set(endpoint);
				}
			}
		}

		private void undoOption(Option opt, BitSet covered, BitSet freeCovered) {
			covered.andNot(opt.mask);
			for (int var : opt.varIndexes) {
				varUseCounts[var]--;
			}
			for (int endpoint : opt.requiredFreeEndpointIndexes) {
				if (--freeCoverCounts[endpoint] == 0) {
					freeCovered.clear(endpoint);
				}
			}
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
			if (left >= Long.MAX_VALUE || normalizedRight >= Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			if (left > Long.MAX_VALUE - normalizedRight) {
				return Long.MAX_VALUE;
			}
			return left + normalizedRight;
		}

		private Score score(List<Option> chosen, long totalCost) {
			int[] diameters = new int[chosen.size()];
			for (int i = 0; i < chosen.size(); i++) {
				diameters[i] = chosen.get(i).comp.diameter();
			}
			java.util.Arrays.sort(diameters);
			reverse(diameters);
			return new Score(totalCost, chosen.size(), diameters);
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

		private void emitPlans(Iterable<ScoredPlan> plans) {
			ArrayList<ScoredPlan> ordered = new ArrayList<>();
			for (ScoredPlan plan : plans) {
				ordered.add(plan);
			}
			ordered.sort((left, right) -> {
				int cmp = compareScore(left.score(), right.score());
				if (cmp != 0) {
					return cmp;
				}
				return left.coverKey().compareTo(right.coverKey());
			});
			int emitted = 0;
			for (ScoredPlan plan : ordered) {
				if (emitted >= maxOutputs) {
					return;
				}
				out.accept(plan.plan());
				emitted++;
			}
		}

		private int compareScore(Score left, Score right) {
			return switch (order) {
				case COST -> compareCostScore(left, right);
				case MAX_COLLAPSE -> compareMaxCollapseScore(left, right);
			};
		}

		private static int compareCostScore(Score left, Score right) {
			return Long.compare(left.totalCost(), right.totalCost());
		}

		private static int compareMaxCollapseScore(Score left, Score right) {
			int cmp = Integer.compare(left.components(), right.components());
			if (cmp != 0) {
				return cmp;
			}
			cmp = compareDiameters(left.diameters(), right.diameters());
			if (cmp != 0) {
				return cmp;
			}
			return Long.compare(left.totalCost(), right.totalCost());
		}

		private static int compareDiameters(int[] left, int[] right) {
			int size = Math.min(left.length, right.length);
			for (int i = 0; i < size; i++) {
				int cmp = Integer.compare(right[i], left[i]);
				if (cmp != 0) {
					return cmp;
				}
			}
			return Integer.compare(left.length, right.length);
		}

		private static void reverse(int[] values) {
			for (int left = 0, right = values.length - 1; left < right; left++, right--) {
				int swap = values[left];
				values[left] = values[right];
				values[right] = swap;
			}
		}

		private void checkDeadline() {
			if (Thread.currentThread().isInterrupted()) {
				throw new DecompositionTimeoutException("Cover selection interrupted");
			}
			if (deadlineNanos != Long.MAX_VALUE && System.nanoTime() >= deadlineNanos) {
				throw new DecompositionTimeoutException("Cover selection timed out");
			}
		}

		private boolean isValid(List<Option> chosen, BitSet freeCovered) {
			return isValid(chosen, freeCovered, varUseCounts);
		}

		private boolean isValid(List<Option> chosen, BitSet freeCovered, int[] variableUseCounts) {
			if (freeCovered.cardinality() < requiredFreeCount) {
				return false;
			}
			if (numVars == 0 || chosen.size() < 2) {
				return true;
			}

			BitSet shared = new BitSet(numVars);
			for (int v = 0; v < numVars; v++) {
				if (variableUseCounts[v] >= 2) {
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

		private record BeamState(
				BitSet covered,
				BitSet freeCovered,
				List<Option> chosen,
				int[] varUseCounts,
				long totalCost,
				int maxDiameter) {
		}

		private record Score(
				long totalCost,
				int components,
				int[] diameters) {
		}

		private record ScoredPlan(
				Plan plan,
				Score score,
				String coverKey) {
		}
	}
}
