package dev.roanh.gmark.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Minimal implementation of the legacy {@code dev.roanh.gmark.util.UniqueGraph} API expected by the
 * CPQ native index. The behaviour is intentionally close to the original, but it is implemented in
 * terms of simple adjacency structures to avoid bundling the historical gMark binaries.
 */
public final class UniqueGraph<V, E> {
  private final Map<V, GraphNode> nodeMap = new LinkedHashMap<>();
  private final List<GraphNode> nodes = new ArrayList<>();
  private final List<GraphEdge> edges = new ArrayList<>();
  private int nextNodeId = 0;

  public UniqueGraph() {}

  private UniqueGraph(List<GraphNode> nodes, List<GraphEdge> edges, int nextNodeId) {
    for (GraphNode node : nodes) {
      node.graph = this;
      this.nodeMap.put(node.data, node);
      this.nodes.add(node);
    }
    for (GraphEdge edge : edges) {
      edge.attach(this);
      this.edges.add(edge);
    }
    this.nextNodeId = nextNodeId;
  }

  public List<GraphNode> removeNodeIf(Predicate<GraphNode> predicate) {
    Objects.requireNonNull(predicate, "predicate");
    List<GraphNode> removed = new ArrayList<>();
    for (GraphNode node : new ArrayList<>(nodes)) {
      if (predicate.test(node)) {
        node.remove();
        removed.add(node);
      }
    }
    return removed;
  }

  public List<GraphNode> getNodes() {
    return Collections.unmodifiableList(nodes);
  }

  public List<GraphEdge> getEdges() {
    List<GraphEdge> active = new ArrayList<>();
    for (GraphEdge edge : edges) {
      if (edge.active) {
        active.add(edge);
      }
    }
    return Collections.unmodifiableList(active);
  }

  public GraphNode getNode(V value) {
    return nodeMap.get(value);
  }

  public GraphNode addUniqueNode(V value) {
    return nodeMap.computeIfAbsent(
        value,
        key -> {
          GraphNode node = new GraphNode(nextNodeId++, this, key);
          nodes.add(node);
          return node;
        });
  }

  public int getEdgeCount() {
    return (int) edges.stream().filter(edge -> edge.active).count();
  }

  public int getNodeCount() {
    return nodes.size();
  }

  public GraphEdge getEdge(V source, V target) {
    GraphNode node = getNode(source);
    return node == null ? null : node.getEdgeTo(target);
  }

  public GraphEdge getEdge(V source, V target, E data) {
    GraphNode node = getNode(source);
    return node == null ? null : node.getEdgeTo(target, data);
  }

  public void addUniqueEdge(V source, V target, E data) {
    addUniqueEdge(addUniqueNode(source), addUniqueNode(target), data);
  }

  public void addUniqueEdge(GraphNode source, V target, E data) {
    addUniqueEdge(source, addUniqueNode(target), data);
  }

  public void addUniqueEdge(V source, GraphNode target, E data) {
    addUniqueEdge(addUniqueNode(source), target, data);
  }

  public void addUniqueEdge(V source, V target) {
    addUniqueEdge(addUniqueNode(source), addUniqueNode(target), null);
  }

  public void addUniqueEdge(GraphNode source, V target) {
    addUniqueEdge(source, addUniqueNode(target), null);
  }

  public void addUniqueEdge(V source, GraphNode target) {
    addUniqueEdge(addUniqueNode(source), target, null);
  }

  public void addUniqueEdge(GraphNode source, GraphNode target) {
    addUniqueEdge(source, target, null);
  }

  public void addUniqueEdge(GraphNode source, GraphNode target, E data) {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(target, "target");
    source.addUniqueEdgeTo(target, data);
  }

  public int[][] toAdjacencyList() {
    int[][] adjacency = new int[nodes.size()][];
    for (GraphNode node : nodes) {
      List<Integer> targets = new ArrayList<>();
      for (GraphEdge edge : node.out) {
        if (edge.active) {
          targets.add(edge.target.getID());
        }
      }
      adjacency[node.getID()] = targets.stream().mapToInt(Integer::intValue).toArray();
    }
    return adjacency;
  }

  public UniqueGraph<V, E> copy() {
    Map<GraphNode, GraphNode> copies = new IdentityHashMap<>();
    List<GraphNode> nodeCopies = new ArrayList<>(nodes.size());
    for (GraphNode node : nodes) {
      GraphNode clone = node.shallowCopy();
      copies.put(node, clone);
      nodeCopies.add(clone);
    }
    List<GraphEdge> edgeCopies = new ArrayList<>();
    for (GraphEdge edge : edges) {
      if (!edge.active) {
        continue;
      }
      GraphEdge cloned = new GraphEdge(copies.get(edge.source), copies.get(edge.target), edge.data);
      edgeCopies.add(cloned);
    }
    UniqueGraph<V, E> clone = new UniqueGraph<>(nodeCopies, edgeCopies, nextNodeId);
    for (GraphEdge edge : clone.edges) {
      edge.attach(clone);
      edge.source.out.add(edge);
      edge.target.in.add(edge);
    }
    return clone;
  }

  private void registerEdge(GraphEdge edge) {
    edges.add(edge);
  }

  private void unregisterEdge(GraphEdge edge) {
    edges.remove(edge);
  }

  public final class GraphNode implements IDable {
    private int id;
    private UniqueGraph<V, E> graph;
    private final Set<GraphEdge> out = new LinkedHashSet<>();
    private final Set<GraphEdge> in = new LinkedHashSet<>();
    private V data;

    private GraphNode(int id, UniqueGraph<V, E> graph, V data) {
      this.id = id;
      this.graph = graph;
      this.data = data;
    }

    private GraphNode shallowCopy() {
      return new GraphNode(id, null, data);
    }

    public void rename(GraphNode other) {
      Objects.requireNonNull(other, "other");
      this.data = other.data;
    }

    @Override
    public int getID() {
      return id;
    }

    public void remove() {
      for (GraphEdge edge : new ArrayList<>(out)) {
        edge.remove();
      }
      for (GraphEdge edge : new ArrayList<>(in)) {
        edge.remove();
      }
      graph.nodes.remove(this);
      graph.nodeMap.remove(data);
    }

    public GraphEdge getEdgeTo(V target) {
      for (GraphEdge edge : out) {
        if (edge.active && Objects.equals(edge.target.data, target)) {
          return edge;
        }
      }
      return null;
    }

    public GraphEdge getEdgeTo(V target, E data) {
      for (GraphEdge edge : out) {
        if (edge.active
            && Objects.equals(edge.target.data, target)
            && Objects.equals(edge.data, data)) {
          return edge;
        }
      }
      return null;
    }

    public int getOutCount() {
      return (int) out.stream().filter(edge -> edge.active).count();
    }

    public int getInCount() {
      return (int) in.stream().filter(edge -> edge.active).count();
    }

    public Set<GraphEdge> getOutEdges() {
      return Collections.unmodifiableSet(out);
    }

    public Set<GraphEdge> getInEdges() {
      return Collections.unmodifiableSet(in);
    }

    public void addUniqueEdgeFrom(GraphNode source, E label) {
      Objects.requireNonNull(source, "source");
      source.addUniqueEdgeTo(this, label);
    }

    public void addUniqueEdgeFrom(V source, E label) {
      addUniqueEdgeFrom(graph.addUniqueNode(source), label);
    }

    public void addUniqueEdgeFrom(GraphNode source) {
      addUniqueEdgeFrom(source, null);
    }

    public void addUniqueEdgeFrom(V source) {
      addUniqueEdgeFrom(source, null);
    }

    public void addUniqueEdgeTo(GraphNode target, E label) {
      Objects.requireNonNull(target, "target");
      GraphEdge existing = getEdgeTo(target.data, label);
      if (existing != null) {
        return;
      }
      GraphEdge edge = new GraphEdge(this, target, label);
      out.add(edge);
      target.in.add(edge);
      graph.registerEdge(edge);
    }

    public void addUniqueEdgeTo(V target, E label) {
      addUniqueEdgeTo(graph.addUniqueNode(target), label);
    }

    public void addUniqueEdgeTo(GraphNode target) {
      addUniqueEdgeTo(target, null);
    }

    public void addUniqueEdgeTo(V target) {
      addUniqueEdgeTo(target, null);
    }

    public V getData() {
      return data;
    }

    @Override
    public String toString() {
      return String.valueOf(data);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof UniqueGraph<?, ?>.GraphNode other)) {
        return false;
      }
      return id == other.id && Objects.equals(graph, other.graph);
    }

    @Override
    public int hashCode() {
      return Objects.hash(graph, id);
    }
  }

  public final class GraphEdge {
    private GraphNode source;
    private GraphNode target;
    private E data;
    private boolean active = true;

    private GraphEdge(GraphNode source, GraphNode target, E data) {
      this.source = source;
      this.target = target;
      this.data = data;
    }

    private void attach(UniqueGraph<V, E> graph) {
      if (source.graph != graph) {
        source.graph = graph;
      }
      if (target.graph != graph) {
        target.graph = graph;
      }
    }

    public void remove() {
      if (!active) {
        return;
      }
      active = false;
      source.out.remove(this);
      target.in.remove(this);
      UniqueGraph.this.unregisterEdge(this);
    }

    public boolean restore() {
      if (active) {
        return false;
      }
      active = true;
      source.out.add(this);
      target.in.add(this);
      UniqueGraph.this.registerEdge(this);
      return true;
    }

    public GraphNode getSourceNode() {
      return source;
    }

    public GraphNode getTargetNode() {
      return target;
    }

    public V getSource() {
      return source.getData();
    }

    public V getTarget() {
      return target.getData();
    }

    public E getData() {
      return data;
    }

    @Override
    public String toString() {
      return source + " -[" + data + "]-> " + target;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof UniqueGraph<?, ?>.GraphEdge other)) {
        return false;
      }
      return Objects.equals(source, other.source)
          && Objects.equals(target, other.target)
          && Objects.equals(data, other.data);
    }

    @Override
    public int hashCode() {
      return Objects.hash(source, target, data);
    }
  }
}
