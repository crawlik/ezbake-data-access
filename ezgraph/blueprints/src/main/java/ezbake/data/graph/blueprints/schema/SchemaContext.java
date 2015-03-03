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

import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Current context against which to check operations involving schemas. Each {@link SchemaFilterGraph} has one schema
 * context. The schema context can know about many different schemas via a {@link PropertySchemaManager}. Any schema
 * known to this context can be added to an element in the graph to which this context belongs (in a typical
 * implementation).
 */
public abstract class SchemaContext {

    /**
     * Wraps any given Edge in a SchemaFilterEdge.
     *
     * @param edge the edge to wrap in a SchemaFilterEdge
     * @return the given edge wrapped in a SchemaFilterEdge
     */
    public SchemaFilterEdge asSchemaFilterEdge(Edge edge) {
        if (edge == null) {
            return null;
        }

        if (edge instanceof SchemaFilterEdge && ((SchemaFilterEdge) edge).getContext() == this) {
            return (SchemaFilterEdge) edge;
        } else {
            return new SchemaFilterEdge(edge, this);
        }
    }

    /**
     * Wraps any given Vertex in a SchemaFilterVertex.
     *
     * @param vertex the vertex to wrap in a SchemaFilterVertex
     * @return the given vertex wrapped in a SchemaFilterVertex
     */
    public SchemaFilterVertex asSchemaFilterVertex(Vertex vertex) {
        if (vertex == null) {
            return null;
        }

        if (vertex instanceof SchemaFilterVertex && ((SchemaFilterVertex) vertex).getContext() == this) {
            return (SchemaFilterVertex) vertex;
        } else {
            return new SchemaFilterVertex(vertex, this);
        }
    }

    /**
     * Converts an iterable of edges to schema filter edges.
     *
     * @param edges iterable of edges to convert
     * @return iterable of {@link ezbake.data.graph.blueprints.schema.SchemaFilterEdge}
     */
    public <T extends Edge> Iterable<Edge> asSchemaFilterEdges(Iterable<T> edges) {
        return Iterables.transform(
                edges, new Function<T, Edge>() {
                    @Override
                    public Edge apply(T t) {
                        return asSchemaFilterEdge(t);
                    }
                });
    }

    /**
     * Converts an iterable of vertices to schema filter vertices.
     *
     * @param vertices iterable of vertices to convert
     * @return iterable of {@link ezbake.data.graph.blueprints.schema.SchemaFilterVertex}
     */
    public <T extends Vertex> Iterable<Vertex> asSchemaFilterVertices(Iterable<T> vertices) {
        return Iterables.transform(
                vertices, new Function<T, Vertex>() {
                    @Override
                    public Vertex apply(T t) {
                        return asSchemaFilterVertex(t);
                    }
                });
    }

    /**
     * Validates that a property key exists in a schema.
     *
     * @param identifier the identifier with which to look for the schema
     * @param propertyName the name of the property to check for on the schema
     * @throws SchemaViolationException if the schema/property combination cannot be found
     */
    public abstract void validateSchemaKey(String identifier, String propertyName) throws SchemaViolationException;

    /**
     * Validates a property using a schema with the given identifier.
     *
     * @param identifier the identifier of the schema with which to validate the property
     * @param propertyName the name of the property in the schema
     * @param value the value of the property
     * @throws SchemaViolationException thrown if schema does not exist or the property is not valid within the schema
     */
    public abstract void validateSchemaKeyValue(String identifier, String propertyName, Object value)
            throws SchemaViolationException;

    /**
     * Validates a change in this context. This may be necessary when setting the {@linkplain
     * SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property}  to a new value.
     *
     * @param newSchemaValues the value of the schema property after being changed
     * @throws SchemaViolationException thrown if the new value fails any checks on the schema property values
     */
    public abstract void validateSchemaUpdate(List<Map<String, Object>> newSchemaValues)
            throws SchemaViolationException;

    /**
     * Get a helper object for parsing schema-enabled property keys and checking property key syntax.
     *
     * @return An object with methods capable of parsing 'schema-enabled' property keys into schema identifier and
     * property name. Also checks property key syntax.
     */
    public abstract PropertyKeyParser getPropertyKeyParser();
}
