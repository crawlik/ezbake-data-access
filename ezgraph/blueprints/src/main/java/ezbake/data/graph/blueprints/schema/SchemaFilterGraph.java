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

package ezbake.data.graph.blueprints.schema;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;

/**
 * Wrapper for Graph that is compatible with schema filter classes.  One SchemaFilterGraph will know about a single
 * {@link SchemaContext} that knows about many {@link PropertySchema}s via a {@link PropertySchemaManager}. Any of the
 * schemas that can be found in the SchemaContext can be added to elements in this graph.
 */
public class SchemaFilterGraph implements Graph, WrapperGraph {

    /**
     * Graph wrapped by this.
     */
    private final Graph baseGraph;

    /**
     * SchemaContext in which this graph is operating.
     */
    private final SchemaContext schemaContext;

    /**
     * Constructs a new SchemaFilterGraph wrapping the given Graph in the given SchemaContext.
     *
     * @param baseGraph the graph to be wrapped
     * @param schemaContext the context in which this graph is operating
     */
    public SchemaFilterGraph(Graph baseGraph, SchemaContext schemaContext) {
        this.baseGraph = baseGraph;
        this.schemaContext = schemaContext;
    }

    /**
     * Unwraps a vertex if it is a {@link SchemaFilterVertex}.
     *
     * @param vertex the Vertex to unwrap
     * @return the base Vertex wrapped by the schema filter
     */
    private static Vertex unwrap(Vertex vertex) {
        if (vertex instanceof SchemaFilterVertex) {
            return ((SchemaFilterVertex) vertex).getBaseVertex();
        }
        return vertex;
    }

    /**
     * Unwraps an Edge if it is a {@link SchemaFilterEdge}.
     *
     * @param edge the Edge to unwrap
     * @return the base Edge wrapped by a {@link SchemaFilterEdge} or the Edge itself
     */
    private static Edge unwrap(Edge edge) {
        if (edge instanceof SchemaFilterEdge) {
            return ((SchemaFilterEdge) edge).getBaseEdge();
        }
        return edge;
    }

    @Override
    public Features getFeatures() {
        final Features features = baseGraph.getFeatures().copyFeatures();
        features.isWrapper = true;

        return features;
    }

    @Override
    public Vertex addVertex(Object id) {
        return schemaContext.asSchemaFilterVertex(baseGraph.addVertex(id));
    }

    @Override
    public Vertex getVertex(Object id) {
        return schemaContext.asSchemaFilterVertex(baseGraph.getVertex(id));
    }

    @Override
    public void removeVertex(Vertex vertex) {
        baseGraph.removeVertex(unwrap(vertex));
    }

    @Override
    public Iterable<Vertex> getVertices() {
        return schemaContext.asSchemaFilterVertices(baseGraph.getVertices());
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return schemaContext.asSchemaFilterVertices(baseGraph.getVertices(key, value));
    }

    @Override
    public Edge addEdge(
            Object id, Vertex outVertex, Vertex inVertex, String label) {
        return schemaContext.asSchemaFilterEdge(baseGraph.addEdge(id, unwrap(outVertex), unwrap(inVertex), label));
    }

    @Override
    public Edge getEdge(Object id) {
        return schemaContext.asSchemaFilterEdge(baseGraph.getEdge(id));
    }

    @Override
    public void removeEdge(Edge edge) {
        baseGraph.removeEdge(unwrap(edge));
    }

    @Override
    public Iterable<Edge> getEdges() {
        return schemaContext.asSchemaFilterEdges(baseGraph.getEdges());
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return schemaContext.asSchemaFilterEdges(baseGraph.getEdges(key, value));
    }

    @Override
    public GraphQuery query() {
        return new SchemaFilterGraphQuery(baseGraph.query(), schemaContext);
    }

    @Override
    public void shutdown() {
        baseGraph.shutdown();
    }

    @Override
    public Graph getBaseGraph() {
        return baseGraph;
    }
}
