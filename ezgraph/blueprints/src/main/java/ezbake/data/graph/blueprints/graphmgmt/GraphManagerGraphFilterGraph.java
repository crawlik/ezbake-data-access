/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.data.graph.blueprints.graphmgmt;

import java.util.Collections;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;

/**
 * When wrapped with GraphManagerGraphFilterGraph, a graph becomes a store of graph metadata, changes to which result in
 * corresponding changes to the data it describes.
 * <p/>
 * This class provides a way for an user to manage a collection of Graph via a typical Blueprints interface; in the case
 * of EzGraph, the Rexster API can be used to create and destroy graphs. When appropriately configured, Rexster is able
 * to retrieve a Graph called {@link #GRAPH_MANAGER_GRAPH_NAME} and cause the creation or removal of graphs via
 * operations on that graph.
 * <p/>
 * The 'GraphManagerGraph' AKA 'graph management graph' is a graph used to manage a store of graphs. In this case,
 * vertices are used to represent actual graph instances. If my GraphManagerGraph had a vertex "graph1" and a vertex
 * "graph2" it is expected that two graphs exist in our backend that can be retrieved using those names.  The {@code
 * GraphManagerGraphFilterGraph} wraps a Blueprints enabled graph (the GraphManagerGraph) and creates and destroys graphs
 * based on changes to that graph via the {@link GraphManager} interface.  For example, if I call {@code
 * addVertex("graph1")} on an instance of {@code GraphManagedGraphFilter}, it will call {@code addGraph("graph1")} on
 * the {@code GraphManager} provided via its constructor, as well as adding that vertex to the GraphManagerGraph.
 * Currently removal, addition, retrieval, and element-level visibility management of graphs (as vertices in the graph
 * management graph) are supported.
 * <p/>
 * Note that Vertex retrieval does not provide access to the graph (that can only be accomplished via the {@code
 * GraphManager} but should instead be used for managing the graph's properties, removing that graph, or verifying the
 * existence of the graph it represents.
 */
public class GraphManagerGraphFilterGraph implements Graph, WrapperGraph {

    /**
     * Name used for the graph management graph/GraphManagerGraph. Important for retrieving this graph via Rexster.
     */
    public static final String GRAPH_MANAGER_GRAPH_NAME = "manage";

    /**
     * Error message used multiple times.
     */
    static final String EDGES_UNSUPPORTED_MSG = "Edges not supported on GraphManagerGraph!";

    /**
     * Blueprints enabled graph this filter it wraps.
     */
    private final Graph wrappedGraph;

    /**
     * Manager with which changes to this graph are synchronized, e.g calling {@code addVertex(...)} on this will also
     * result in a call to {addGraph(...)} on the manager.
     */
    private final GraphManager graphManager;

    /**
     * Constructs a new GraphManagerGraphFilterGraph wrapping the given graph and using the given graph manager to maintain
     * available graphs.
     *
     * @param wrappedGraph the graph to wrap
     * @param graphManager manager with which to regulate available graphs
     */
    public GraphManagerGraphFilterGraph(Graph wrappedGraph, GraphManager graphManager) {
        this.wrappedGraph = wrappedGraph;
        this.graphManager = graphManager;
    }

    @Override
    public Graph getBaseGraph() {
        return wrappedGraph;
    }

    @Override
    public Features getFeatures() {
        final Features features = wrappedGraph.getFeatures().copyFeatures();
        features.isWrapper = true;

        return features;
    }

    @Override
    public Vertex addVertex(Object id) {
        graphManager.addGraph((String) id);

        return asGraphManagerGraphFilterVertex(wrappedGraph.addVertex(id));
    }

    @Override
    public Vertex getVertex(Object id) {

        return asGraphManagerGraphFilterVertex(wrappedGraph.getVertex(id));
    }

    @Override
    public void removeVertex(Vertex vertex) {
        if (vertex.getClass() != GraphManagerGraphFilterVertex.class) {
            vertex = new GraphManagerGraphFilterVertex(vertex, graphManager);
        }
        vertex.remove();
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return asGraphManagerGraphFilterVertices(wrappedGraph.getVertices());
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return asGraphManagerGraphFilterVertices(wrappedGraph.getVertices(key, value));
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        throw new UnsupportedOperationException(EDGES_UNSUPPORTED_MSG);
    }

    /**
     * Edges aren't supported in the GraphManagerGraph, so this method always returns null. An exception is not thrown
     * as some Vertex operations may attempt to call this method.
     */
    @Override
    public Edge getEdge(Object id) {
        return null;
    }

    @Override
    public void removeEdge(Edge edge) {
        throw new UnsupportedOperationException(EDGES_UNSUPPORTED_MSG);
    }

    /**
     * Edges are not supported in the GraphManagerGraph, so an empty list is returned only to support certain Vertex
     * operations.
     *
     * @return an empty list
     */
    @Override
    public Iterable<Edge> getEdges() {
        return Collections.emptyList();
    }

    /**
     * Edges are not supported in the GraphManagerGraph, so an empty list is returned only to support certain Vertex
     * operations.
     *
     * @return an empty list
     */
    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return Collections.emptyList();
    }

    @Override
    public GraphQuery query() {
        return new GraphManagerGraphFilterGraphQuery(wrappedGraph.query(), this);
    }

    @Override
    public void shutdown() {
        wrappedGraph.shutdown();
    }

    /**
     * Converts an iterable of Vertex to an iterable of GraphManagerGraphFilterVertex.
     *
     * @param vertices iterable of vertices to convert
     * @return iterable of {@link GraphManagerGraphFilterVertex}
     */
    public <T extends Vertex> Iterable<Vertex> asGraphManagerGraphFilterVertices(Iterable<T> vertices) {
        return Iterables.transform(
                vertices, new Function<T, Vertex>() {
                    @Override
                    public Vertex apply(T t) {
                        return asGraphManagerGraphFilterVertex(t);
                    }
                });
    }

    /**
     * Wraps any given Vertex in a GraphManagerGraphFilterVertex.
     *
     * @param vertex the vertex to wrap in a GraphManagerGraphFilterVertex
     * @return the given vertex wrapped in a GraphManagerGraphFilterVertex
     */
    public GraphManagerGraphFilterVertex asGraphManagerGraphFilterVertex(Vertex vertex) {
        if (vertex == null) {
            return null;
        }

        if (vertex instanceof GraphManagerGraphFilterVertex
                && ((GraphManagerGraphFilterVertex) vertex).getGraphManager() == graphManager) {
            return (GraphManagerGraphFilterVertex) vertex;
        } else {
            return new GraphManagerGraphFilterVertex(vertex, graphManager);
        }
    }
}
