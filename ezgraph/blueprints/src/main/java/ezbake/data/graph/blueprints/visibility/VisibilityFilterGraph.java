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

package ezbake.data.graph.blueprints.visibility;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;

import ezbake.base.thrift.Permission;

/**
 * Wrapper for vertices that implements visibility controls.
 */
public class VisibilityFilterGraph implements Graph, WrapperGraph {

    /** Wrapped graph */
    private final Graph baseGraph;

    /** Context for evaluating permissions on elements and properties */
    private final PermissionContext context;

    /**
     * Construct a new wrapper for a graph that implements visibility controls.
     *
     * @param baseGraph graph to wrap
     * @param context context for evaluating permissions on elements and properties
     */
    public VisibilityFilterGraph(Graph baseGraph, PermissionContext context) {
        this.baseGraph = baseGraph;
        this.context = context;
    }

    @Override
    public Graph getBaseGraph() {
        return baseGraph;
    }

    @Override
    public Features getFeatures() {
        Features features = baseGraph.getFeatures().copyFeatures();
        features.isWrapper = true;

        return features;
    }

    @Override
    public Vertex addVertex(Object id) {
        return context.asVisibilityFilterVertex(baseGraph.addVertex(id));
    }

    /**
     * Return the vertex referenced by the provided object identifier.
     *
     * If no vertex is referenced by that identifier, or if the vertex is
     * neither readable nor discoverable, return null.
     *
     * @param id the identifier of the vertex to retrieved from the graph
     * @return the vertex referenced by the provided identifier or null
     */
    @Override
    public Vertex getVertex(Object id) {
        Vertex b = baseGraph.getVertex(id);
        if (b == null) {
            return null;
        }

        VisibilityFilterVertex v = context.asVisibilityFilterVertex(b);
        if (!v.hasAnyPermission(Permission.DISCOVER, Permission.READ)) {
            return null;
        }

        return v;
    }

    /**
     * Remove the provided vertex from the graph.
     *
     * The vertex is removable only if it is writable, all of its properties
     * are writable, and all of its incident edges are also removable.
     *
     * @param vertex the vertex to remove from the graph
     */
    @Override
    public void removeVertex(Vertex vertex) {
        context.asVisibilityFilterVertex(vertex).remove();
    }

    /**
     * Return an iterable to all the vertices in the graph.
     *
     * Only vertices that are readable or discoverable will be returned.
     *
     * @return iterable of readable or discoverable vertices.
     */
    @Override
    public Iterable<Vertex> getVertices() {
        return context.getElementFilter().filterVertices(baseGraph.getVertices(), Permission.DISCOVER, Permission.READ);
    }

    /**
     * Return an iterable to all the vertices in the graph that have a
     * particular key/value property.
     *
     * Only vertices that are readable or discoverable with the given property
     * also readable or discoverable will be returned.
     *
     * @param key key of vertex
     * @param value value of vertex
     * @return readable or discoverable vertices with a key/value property
     */
    @Override
    public Iterable<Vertex> getVertices(final String key, final Object value) {
        return query().has(key, value).vertices();
    }

    /**
     * Add an edge to the graph.
     *
     * If an edge already exists with the given id, an error will be thrown
     * even if the edge is not readable or discoverable. No permissions on the
     * tail or head vertices are required.
     */
    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        Vertex ov;
        Vertex iv;

        // Avoid a class cast exception if we're passing a wrapped vertex, but the base graph expects an unwrapped one

        if (outVertex instanceof VisibilityFilterVertex) {
            ov = ((VisibilityFilterVertex) outVertex).getBaseVertex();
        } else {
            ov = outVertex;
        }

        if (inVertex instanceof VisibilityFilterVertex) {
            iv = ((VisibilityFilterVertex) inVertex).getBaseVertex();
        } else {
            iv = inVertex;
        }

        return context.asVisibilityFilterEdge(baseGraph.addEdge(id, ov, iv, label));
    }

    /**
     * Return the edge referenced by the provided object identifier.
     *
     * If no edge is referenced by that identifier, or if the edge is
     * neither readable nor discoverable, return null.
     *
     * @param id the identifier of the edge to retrieved from the graph
     * @return the edge referenced by the provided identifier or null
     */
    @Override
    public Edge getEdge(Object id) {
        Edge b = baseGraph.getEdge(id);
        if (b == null) {
            return null;
        }

        VisibilityFilterEdge e = context.asVisibilityFilterEdge(b);
        if (!e.hasAnyPermission(Permission.DISCOVER, Permission.READ)) {
            return null;
        }

        return e;
    }

    /**
     * Remove the provided edge from the graph.
     *
     * The edge is removable only if it is writable and all of its properties
     * are writable.
     *
     * @param edge the edge to remove from the graph
     */
    @Override
    public void removeEdge(Edge edge) {
        context.asVisibilityFilterEdge(edge).remove();
    }

    /**
     * Return an iterable to all the edges in the graph.
     *
     * Only edges that are readable or discoverable will be returned.
     *
     * @return iterable of readable or discoverable edges.
     */
    @Override
    public Iterable<Edge> getEdges() {
        return context.getElementFilter().filterEdges(baseGraph.getEdges(), Permission.DISCOVER, Permission.READ);
    }

    /**
     * Return an iterable to all the edges in the graph that have a
     * particular key/value property.
     *
     * Only edges that are readable or discoverable with the given property
     * also readable or discoverable will be returned.
     *
     * @param key key of edge
     * @param value value of edge
     * @return readable or discoverable edges with a key/value property
     */
    @Override
    public Iterable<Edge> getEdges(final String key, final Object value) {
        return query().has(key, value).edges();
    }

    @Override
    public GraphQuery query() {
        return new VisibilityFilterGraphQuery(baseGraph.query(), context);
    }

    @Override
    public void shutdown() {
        baseGraph.shutdown();
    }
}
