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
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

import ezbake.data.graph.blueprints.visibility.ElementFilter;

/**
 * A Vertex wrapper for a vertex that is meant to represent a graph. Operations on this vertex are expected to affect
 * the graph it represents. For example, if this vertex is removed, the graph it represents should also be removed. See
 * {@link GraphManagerGraphFilterGraph} for more on how this class is used for graph management.
 */
public class GraphManagerGraphFilterVertex implements Vertex {

    /**
     * Vertex being wrapped.
     */
    private final Vertex wrappedVertex;

    /**
     * GraphManager which keeps track of the graph this Vertex represents.
     */
    private final GraphManager graphManager;

    /**
     * Constructs a new GraphManagerGraphFilterVertex wrapping the given Vertex and using the given GraphManager to
     * affect the available graphs.
     *
     * @param wrappedVertex vertex to wrap
     * @param graphManager graph manager used to handle changes to available graphs when this is modified
     */
    public GraphManagerGraphFilterVertex(Vertex wrappedVertex, GraphManager graphManager) {
        this.graphManager = graphManager;
        this.wrappedVertex = wrappedVertex;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        return Collections.emptyList();
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        //getting vertices from a vertex requires traversing edges, so this is not supported
        throw new UnsupportedOperationException(GraphManagerGraphFilterGraph.EDGES_UNSUPPORTED_MSG);
    }

    @Override
    public VertexQuery query() {
        throw new UnsupportedOperationException("Query Vertex not supported on GraphManagerGraph!");
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        throw new UnsupportedOperationException(GraphManagerGraphFilterGraph.EDGES_UNSUPPORTED_MSG);
    }

    @Override
    public <T> T getProperty(String key) {
        return wrappedVertex.getProperty(key);
    }

    @Override
    public Set<String> getPropertyKeys() {
        return wrappedVertex.getPropertyKeys();
    }

    @Override
    public void setProperty(String key, Object value) {
        if (key.equals(ElementFilter.VISIBILITY_PROPERTY_KEY)) {
            wrappedVertex.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, value);
        } else {
            throw new UnsupportedOperationException(
                    "Setting any property but element level visibility is not supported in GraphManagerGraph!");
        }
    }

    @Override
    public <T> T removeProperty(String key) {
        throw new UnsupportedOperationException(
                "Removing properties is not supported in GraphManagerGraph!");
    }

    @Override
    public void remove() {
        graphManager.removeGraph((String) getId());
        wrappedVertex.remove();
    }

    @Override
    public Object getId() {
        return wrappedVertex.getId();
    }

    public GraphManager getGraphManager() {
        return graphManager;
    }
}
