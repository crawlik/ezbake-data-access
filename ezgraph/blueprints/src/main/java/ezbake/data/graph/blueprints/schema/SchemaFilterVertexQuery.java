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
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.wrappers.WrapperVertexQuery;

/**
 * Wraps a {@link com.tinkerpop.blueprints.VertexQuery} and is compatible with schema filter classes.
 */
public class SchemaFilterVertexQuery extends WrapperVertexQuery {

    /**
     * VertexQuery to be wrapped.
     */
    private final VertexQuery baseQuery;

    /**
     * SchemaContext under which this operates.
     */
    private final SchemaContext schemaContext;

    /**
     * Constructs a new SchemaFilterVertexQuery wrapping the given VertexQuery and operates in the given SchemaContext.
     *
     * @param baseQuery query to wrap
     * @param schemaContext context in which to operate
     */
    public SchemaFilterVertexQuery(VertexQuery baseQuery, SchemaContext schemaContext) {
        super(baseQuery);

        this.baseQuery = baseQuery;
        this.schemaContext = schemaContext;
    }

    @Override
    public Iterable<Edge> edges() {
        return schemaContext.asSchemaFilterEdges(baseQuery.edges());
    }

    @Override
    public Iterable<Vertex> vertices() {
        return schemaContext.asSchemaFilterVertices(baseQuery.vertices());
    }
}
