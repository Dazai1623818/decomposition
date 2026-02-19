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
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

final class CoverSelector {
	enum Order {
		DEFAULT,
		COST,
		DIAMETER
	}

	private final int limit;
	private final Order order;
	private final ToLongFunction<CPQ> costFn;
	private final long deadlineNanos;

	CoverSelector(int limit) {
		this(limit, Order.DEFAULT, null, Long.MAX_VALUE);
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
				AtomCQ edge = edges.get(e);
				cover.get(e).sort(optionOrder(edge));
			}
		}

		private Comparator<Option> optionOrder(AtomCQ edge) {
			return switch (order) {
				case COST -> Comparator
						.comparingInt((Option opt) -> opt.comp.diameter()).reversed()
						.thenComparing(
								Comparator.comparingInt((Option opt) -> opt.mask.cardinality()).reversed())
						.thenComparingLong(opt -> opt.cost)
						.thenComparingInt(opt -> directionPreference(opt, edge))
						.thenComparingInt(opt -> opt.id);
				case DIAMETER -> Comparator
						.comparingInt((Option opt) -> opt.comp.diameter()).reversed()
						.thenComparing(
								Comparator.comparingInt((Option opt) -> opt.mask.cardinality()).reversed())
						.thenComparingLong(opt -> opt.cost)
						.thenComparingInt(opt -> directionPreference(opt, edge))
						.thenComparingInt(opt -> opt.id);
				case DEFAULT -> Comparator
						.comparingInt((Option opt) -> opt.mask.cardinality()).reversed()
						.thenComparing(Comparator.comparingInt((Option opt) -> opt.comp.diameter()).reversed())
						.thenComparingInt(opt -> directionPreference(opt, edge))
						.thenComparingInt(opt -> opt.id);
			};
		}

		private static int directionPreference(Option opt, AtomCQ edge) {
			return opt.comp.s().equals(edge.getSource()) && opt.comp.t().equals(edge.getTarget()) ? 0 : 1;
		}

		void search(int limit, Consumer<Plan> out) {
			checkDeadline();
			this.out = out;
			this.outputs = 0;
			this.maxOutputs = limit == 0 ? Integer.MAX_VALUE : limit;
			this.seenCoverKeys.clear();
			visit(new BitSet(numEdges), new HashSet<>(), new ArrayList<>());
		}

		private void visit(BitSet covered, Set<VarCQ> freeCovered, ArrayList<Option> chosen) {
			checkDeadline();
			if (outputs >= maxOutputs) {
				return;
			}

			int nextEdge = covered.nextClearBit(0);
			if (nextEdge >= numEdges) {
				if (isValid(chosen, freeCovered)) {
					String coverKey = coverKey(chosen);
					if (seenCoverKeys.add(coverKey)) {
						// Inline build() logic
						ArrayList<Option> sorted = new ArrayList<>(chosen);
						sorted.sort(Comparator.comparingInt(opt -> opt.id));
						ArrayList<Component> comps = new ArrayList<>(sorted.size());
						for (Option opt : sorted) {
							comps.add(opt.comp);
						}
						out.accept(new Plan(query, comps));
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
