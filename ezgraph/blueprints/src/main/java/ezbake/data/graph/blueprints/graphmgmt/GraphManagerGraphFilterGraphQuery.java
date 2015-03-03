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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.WrappedGraphQuery;

/**
 * Query used by {@link GraphManagerGraphFilterGraph}.  This supports GraphManagerGraphFilterGraph operations by wrapping
 * vertices in {@link GraphManagerGraphFilterVertex} and returning an empty list for edge queries, as edges are not supported in
 * graphs wrapped by the GraphManagerGraphFilterGraph.
 */
public class GraphManagerGraphFilterGraphQuery extends WrappedGraphQuery {

    /**
     * GraphQuery wrapped by this.
     */
    private final GraphQuery query;

    /**
     * Used for access to some helper methods, specifically converting Iterables of {@link
     * com.tinkerpop.blueprints.Vertex} to Iterables of {@link GraphManagerGraphFilterVertex}.
     */
    private final GraphManagerGraphFilterGraph graphManagerGraphFilterGraph;

    /**
     * Constructs a new GraphManagerGraphFilterGraphQuery wrapping the given query and in the context of the given
     * GraphManagerGraphFilterGraph.
     *
     * @param query query to wrap
     * @param graphManagerGraphFilterGraph GraphManagerGraphFilterGraph in whose context this belongs
     */
    public GraphManagerGraphFilterGraphQuery(GraphQuery query,
            GraphManagerGraphFilterGraph graphManagerGraphFilterGraph) {
        super(query);
        this.query = query;
        this.graphManagerGraphFilterGraph = graphManagerGraphFilterGraph;
    }

    /**
     * Edges are not supported in the GraphManagerGraph, so an empty list here is returned only to support certain
     * Vertex operations.
     *
     * @return an empty list
     */
    @Override
    public Iterable<Edge> edges() {
        return Collections.emptyList();
    }

    @Override
    public Iterable<Vertex> vertices() {
        return graphManagerGraphFilterGraph.asGraphManagerGraphFilterVertices(query.vertices());
    }
}
