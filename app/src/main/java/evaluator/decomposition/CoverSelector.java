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
	enum Order {
		COST,
		MAX_COLLAPSE
	}

	private final int limit;
	private final Order order;
	private final ToLongFunction<CPQ> costFn;
	private final Predicate<Plan> planFilter;
	private final long deadlineNanos;

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
	}

	public Stream<Plan> select(ConjunctiveQuery query, List<Component> components) {
		List<Plan> results = new ArrayList<>();
		new Solver(query, components, order, costFn, planFilter, deadlineNanos)
				.search(limit, results::add);
		return results.stream();
	}

	private static final class Option {
		private static final long UNSET_COST = Long.MIN_VALUE;

		final Component comp;
		final int id;
		final BitSet mask;
		final int[] varIndexes;
		final int[] interiorVarIndexes;
		final int[] requiredFreeEndpointIndexes;
		private long cachedCost = UNSET_COST;

		Option(
				Component comp,
				int id,
				Map<VarCQ, Integer> varIndex,
				ConjunctiveQuery query,
				BitSet requiredFreeMask) {
			this.comp = comp;
			this.id = id;
			this.mask = comp.maskUnsafe();
			BitSet vars = new BitSet(varIndex.size());
			BitSet endpoints = new BitSet(varIndex.size());

			// Inline variable mapping
			Integer sIdx = varIndex.get(comp.s());
			if (sIdx != null) {
				endpoints.set(sIdx);
			}

			Integer tIdx = varIndex.get(comp.t());
			if (tIdx != null) {
				endpoints.set(tIdx);
			}

			for (int e = mask.nextSetBit(0); e >= 0; e = mask.nextSetBit(e + 1)) {
				AtomCQ edge = query.edges().get(e);
				Integer u = varIndex.get(edge.getSource());
				if (u != null) {
					vars.set(u);
				}
				Integer v = varIndex.get(edge.getTarget());
				if (v != null) {
					vars.set(v);
				}
			}
			BitSet interiorVars = (BitSet) vars.clone();
			interiorVars.andNot(endpoints);
			BitSet requiredFreeEndpoints = (BitSet) endpoints.clone();
			requiredFreeEndpoints.and(requiredFreeMask);
			this.varIndexes = toIndexes(vars);
			this.interiorVarIndexes = toIndexes(interiorVars);
			this.requiredFreeEndpointIndexes = toIndexes(requiredFreeEndpoints);
		}

		long cost(ToLongFunction<CPQ> costFn) {
			if (cachedCost != UNSET_COST) {
				return cachedCost;
			}
			long rawCost = costFn == null ? 0L : costFn.applyAsLong(comp.cpq());
			cachedCost = rawCost < 0L ? 0L : rawCost;
			return cachedCost;
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
		private final ToLongFunction<CPQ> costFn;
		private final Predicate<Plan> planFilter;
		private final Set<String> seenCoverKeys = new HashSet<>();
		private final long deadlineNanos;
		private final int[] varUseCounts;
		private final int[] interiorUseCounts;
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
				long deadlineNanos) {
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
			this.costFn = costFn;
			this.planFilter = planFilter;
			this.deadlineNanos = deadlineNanos;
			this.varUseCounts = new int[numVars];
			this.interiorUseCounts = new int[numVars];
			this.freeCoverCounts = new int[numVars];

			List<Option> allOptions = new ArrayList<>(components.size());
			for (int i = 0; i < components.size(); i++) {
				Component comp = components.get(i);
				Option option = new Option(comp, i, varIndex, query, requiredFreeMask);
				allOptions.add(option);
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
		}

		void search(int limit, Consumer<Plan> out) {
			checkDeadline();
			this.out = out;
			this.maxOutputs = limit == 0 ? Integer.MAX_VALUE : limit;
			this.seenCoverKeys.clear();
			this.scoredPlans = new ArrayList<>();
			java.util.Arrays.fill(varUseCounts, 0);
			java.util.Arrays.fill(interiorUseCounts, 0);
			java.util.Arrays.fill(freeCoverCounts, 0);
			visit(new BitSet(numEdges), new BitSet(numVars), new ArrayList<>());
			emitPlans(scoredPlans);
		}

		private void visit(
				BitSet covered,
				BitSet freeCovered,
				ArrayList<Option> chosen) {
			checkDeadline();
			if (scoredPlans.size() >= maxOutputs) {
				return;
			}
			if (!hasFeasibleContinuation(covered)) {
				return;
			}
			int nextEdge = covered.nextClearBit(0);
			if (nextEdge >= numEdges) {
				if (!isValid(freeCovered)) {
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
						structuralScore(chosen),
						coverKey,
						chosen.toArray(Option[]::new)));
				return;
			}

			for (Option opt : cover.get(nextEdge)) {
				checkDeadline();
				if (opt.mask.intersects(covered)) {
					continue;
				}
				applyOption(opt, covered, freeCovered);
				if (violatesSharedBoundary(opt)) {
					undoOption(opt, covered, freeCovered);
					continue;
				}
				chosen.add(opt);
				visit(covered, freeCovered, chosen);
				chosen.remove(chosen.size() - 1);
				undoOption(opt, covered, freeCovered);
			}
		}

		private void applyOption(Option opt, BitSet covered, BitSet freeCovered) {
			covered.or(opt.mask);
			for (int var : opt.varIndexes) {
				varUseCounts[var]++;
			}
			for (int interiorVar : opt.interiorVarIndexes) {
				interiorUseCounts[interiorVar]++;
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
			for (int interiorVar : opt.interiorVarIndexes) {
				interiorUseCounts[interiorVar]--;
			}
			for (int endpoint : opt.requiredFreeEndpointIndexes) {
				if (--freeCoverCounts[endpoint] == 0) {
					freeCovered.clear(endpoint);
				}
			}
		}

		private boolean violatesSharedBoundary(Option opt) {
			for (int var : opt.varIndexes) {
				if (varUseCounts[var] >= 2 && interiorUseCounts[var] > 0) {
					return true;
				}
			}
			return false;
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

		// TODO: is it posssible to get negative cost?
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

		private Score structuralScore(List<Option> chosen) {
			int[] diameters = new int[chosen.size()];
			for (int i = 0; i < chosen.size(); i++) {
				diameters[i] = chosen.get(i).comp.diameter();
			}
			java.util.Arrays.sort(diameters);
			reverse(diameters);
			return new Score(chosen.size(), diameters);
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
				int cmp = compareScore(left, right);
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

		private int compareScore(ScoredPlan left, ScoredPlan right) {
			return switch (order) {
				case COST -> compareCostScore(left, right);
				case MAX_COLLAPSE -> compareMaxCollapseScore(left, right);
			};
		}

		private int compareCostScore(ScoredPlan left, ScoredPlan right) {
			return Long.compare(left.totalCost(), right.totalCost());
		}

		private int compareMaxCollapseScore(ScoredPlan left, ScoredPlan right) {
			int cmp = Integer.compare(left.score().components(), right.score().components());
			if (cmp != 0) {
				return cmp;
			}
			cmp = compareDiameters(left.score().diameters(), right.score().diameters());
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

		private boolean isValid(BitSet freeCovered) {
			if (freeCovered.cardinality() < requiredFreeCount) {
				return false;
			}
			for (int v = 0; v < numVars; v++) {
				if (varUseCounts[v] >= 2 && interiorUseCounts[v] > 0) {
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

		private record Score(
				int components,
				int[] diameters) {
		}

		private final class ScoredPlan {
			private final Plan plan;
			private final Score score;
			private final String coverKey;
			private final Option[] options;
			private long totalCost = Option.UNSET_COST;

			private ScoredPlan(Plan plan, Score score, String coverKey, Option[] options) {
				this.plan = plan;
				this.score = score;
				this.coverKey = coverKey;
				this.options = options;
			}

			private Plan plan() {
				return plan;
			}

			private Score score() {
				return score;
			}

			private String coverKey() {
				return coverKey;
			}

			private long totalCost() {
				if (totalCost != Option.UNSET_COST) {
					return totalCost;
				}
				long computed = 0L;
				for (Option option : options) {
					computed = safeAddCost(computed, option.cost(costFn));
				}
				totalCost = computed;
				return computed;
			}
		}
	}
}
