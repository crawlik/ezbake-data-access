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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

import ezbake.base.thrift.Permission;

/**
 * Wrapper for vertex queries that implements visibility controls.
 */
public class VisibilityFilterVertexQuery extends VisibilityFilterQuery implements VertexQuery {

    /**
     * Wrapped query.
     */
    private final VertexQuery baseQuery;

    /**
     * Vertex from which this query was created.
     */
    private final Vertex parentVertex;

    /**
     * Create a new vertex query wrapper.
     *
     * @param baseQuery wrapped query
     * @param ctx permission context
     * @param parent vertex from which this query was created
     */
    public VisibilityFilterVertexQuery(VertexQuery baseQuery, PermissionContext ctx, Vertex parent) {
        super(baseQuery, ctx);

        this.baseQuery = baseQuery;
        this.parentVertex = parent;
    }

    @Override
    public VertexQuery direction(Direction direction) {
        baseQuery.direction(direction);

        return this;
    }

    @Override
    public VertexQuery labels(String... labels) {
        baseQuery.labels(labels);

        return this;
    }

    @Override
    public long count() {
        return Iterables.size(edges());
    }

    @Override
    public Object vertexIds() {
        // I have no idea why the interface specifies Object here instead of Iterable<Object>. DefaultVertexQuery
        // returns List<Object>.
        return Iterables.transform(vertices(), new Function<Vertex, Object>() {
            @Override
            public Object apply(Vertex vertex) {
                return vertex.getId();
            }
        });
    }

    @Override
    public VertexQuery has(String key) {
        super.has(key);

        return this;
    }

    @Override
    public VertexQuery hasNot(String key) {
        super.hasNot(key);

        return this;
    }

    @Override
    public VertexQuery has(String key, Object value) {
        super.has(key, value);

        return this;
    }

    @Override
    public VertexQuery hasNot(String key, Object value) {
        super.hasNot(key, value);

        return this;
    }

    @Override
    public VertexQuery has(String key, com.tinkerpop.blueprints.Predicate predicate, Object value) {
        super.has(key, predicate, value);

        return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T extends Comparable<T>> VertexQuery has(String key, T value, Compare compare) {
        super.has(key, value, compare);

        return this;
    }

    @Override
    public <T extends Comparable<?>> VertexQuery interval(String key, T startValue, T endValue) {
        super.interval(key, startValue, endValue);

        return this;
    }

    @Override
    public VertexQuery limit(int i) {
        super.limit(i);

        return this;
    }

    @Override
    public Iterable<Vertex> vertices() {
        // Can't treat these the same way as a regular query for vertices since we need to check if edges are readable.
        Iterable<Edge> readableEdges = Iterables.filter(baseQuery.edges(), getPermissionContext().getElementFilter().hasAnyPermissionPredicate(Permission.READ));

        Iterable<Vertex> it = Iterables.transform(readableEdges, new Function<Edge, Vertex>() {
            @Override
            public Vertex apply(Edge edge) {
                // Grab whichever vertex isn't the one that owns this query.
                Vertex v = edge.getVertex(Direction.OUT);
                if (parentVertex.getId().equals(v.getId())) {
                    return edge.getVertex(Direction.IN);
                } else {
                    return v;
                }
            }
        });

        it = filterElements(it);
        it = limitElements(it);

        return getPermissionContext().getElementFilter().asVisibilityFilterVertices(it);
    }
}
