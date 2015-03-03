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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * Wraps a Vertex and is compatible with the schema filter classes.
 */
public class SchemaFilterVertex extends SchemaFilterElement implements Vertex {

    /**
     * Vertex wrapped by this.
     */
    private final Vertex baseVertex;

    /**
     * Constructs a new SchemaFilterVertex with the given Vertex that operates in the given SchemaContext.
     *
     * @param vertex the vertex to wrap
     * @param context the context in which this SchemaFilterVertex is operating
     */
    public SchemaFilterVertex(Vertex vertex, SchemaContext context) {
        super(vertex, context);
        baseVertex = vertex;
    }

    /**
     * Gets the Vertex this class wraps.
     *
     * @return the Vertex this class wraps
     */
    public Vertex getBaseVertex() {
        return baseVertex;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        return getContext().asSchemaFilterEdges(baseVertex.getEdges(direction, labels));
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return getContext().asSchemaFilterVertices(baseVertex.getVertices(direction, labels));
    }

    @Override
    public VertexQuery query() {
        return new SchemaFilterVertexQuery(baseVertex.query(), getContext());
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return getContext().asSchemaFilterEdge(baseVertex.addEdge(label, inVertex));
    }
}
