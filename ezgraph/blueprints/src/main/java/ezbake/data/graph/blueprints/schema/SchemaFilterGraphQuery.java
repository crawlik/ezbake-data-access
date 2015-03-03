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
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.WrappedGraphQuery;

/**
 * Wrapper for {@link com.tinkerpop.blueprints.GraphQuery} compatible with the schema filter classes.
 */
public class SchemaFilterGraphQuery extends WrappedGraphQuery {

    /**
     * Query wrapped by this.
     */
    private final GraphQuery baseQuery;

    /**
     * SchemaContext under which this query operates.
     */
    private final SchemaContext schemaContext;

    /**
     * Constructs a new SchemaFilterGraphQuery that wraps the given GraphQuery and runs under the given SchemaContext.
     *
     * @param query the graph query to be wrapped
     * @param schemaContext the context under which this query runs
     */
    public SchemaFilterGraphQuery(GraphQuery query, SchemaContext schemaContext) {
        super(query);
        this.baseQuery = query;
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
