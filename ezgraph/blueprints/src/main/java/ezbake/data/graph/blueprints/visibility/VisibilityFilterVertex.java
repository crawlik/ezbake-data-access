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

import java.util.Collections;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

import ezbake.base.thrift.Permission;

/**
 * Wrapper for vertices that implements visibility controls.
 */
public class VisibilityFilterVertex extends VisibilityFilterElement implements Vertex {

    /**
     * Wrapped vertex.
     */
    private final Vertex vertex;

    /**
     * Construct a new filtered vertex.
     *
     * @param vertex vertex to wrap
     * @param ctx permission context to obtain permissions on vertex
     */
    public VisibilityFilterVertex(Vertex vertex, PermissionContext ctx) {
        super(vertex, ctx);

        this.vertex = vertex;
    }

    /**
     * Get the vertex this wrapper delegates its operations to.
     *
     * @return vertex this wrapper delegates its operations to
     */
    public Vertex getBaseVertex() {
        return vertex;
    }

    /**
     * Return the edges incident to the vertex according to the provided
     * direction and edge labels.
     *
     * If the vertex is not readable, no edges are returned. Returns only
     * edges that are readable or discoverable.
     *
     * @param direction the direction of the edges to retrieve
     * @param labels the labels of the edges to retrieve
     * @return an iterable of incident edges
     */
    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        if (!hasAnyPermission(Permission.READ)) {
            return Collections.emptyList();
        }

        return getPermissionContext().getElementFilter().filterEdges(vertex.getEdges(direction, labels),
                Permission.DISCOVER, Permission.READ);
    }

    /**
     * Return the vertices adjacent to the vertex according to the provided
     * direction and edge labels.
     *
     * If the vertex is not readable, no edges are returned. Returns only
     * vertices who are readable or discoverable and whose edge with this
     * vertex is readable.
     *
     * @param direction the direction of the edges of the adjacent vertices
     * @param labels the labels of the edges of the adjacent vertices
     * @return an iterable of adjacent vertices
     */
    @Override
    public Iterable<Vertex> getVertices(final Direction direction, String... labels) {
        if (!hasAnyPermission(Permission.READ)) {
            return Collections.emptyList();
        }

        Iterable<Edge> edges = vertex.getEdges(direction, labels);
        Iterable<Vertex> vertices = Iterables.transform(edges,
                new Function<Edge, Vertex>() {
                    @Override
                    public Vertex apply(Edge edge) {
                        VisibilityFilterEdge ve = getPermissionContext().asVisibilityFilterEdge(edge);
                        if (!ve.hasAnyPermission(Permission.READ)) {
                            return null;
                        }

                        // Flip the direction since if we ask for vertices on outgoing edges from a vertex, then they
                        // are the incoming vertex with respect to the edge.
                        VisibilityFilterVertex vv = getPermissionContext().asVisibilityFilterVertex(edge.getVertex(direction.opposite()));
                        if (!vv.hasAnyPermission(Permission.DISCOVER, Permission.READ)) {
                            return null;
                        }

                        return vv;
                    }
                });

        return Iterables.filter(vertices, Predicates.<Vertex>notNull());
    }

    @Override
    public VertexQuery query() {
        return new VisibilityFilterVertexQuery(vertex.query(), getPermissionContext(), this);
    }

    /**
     * Add a new outgoing edge from this vertex to the parameter vertex with
     * provided edge label.
     *
     * No permissions are required for this operation, but if the vertex is not
     * discoverable or readable, the edge may not be able to be queried.
     *
     * @param label the label of the edge
     * @param inVertex the vertex to connect to with an incoming edge
     * @return the newly created edge
     */
    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return getPermissionContext().asVisibilityFilterEdge(vertex.addEdge(label, inVertex));
    }

    /**
     * Remove a vertex from the graph.
     *
     * A vertex is removable only if the context has permission to write the
     * vertex and every edge incident to the wrapped vertex. This means that a
     * remove operation will fail with a permission denied exception if there
     * are edges that the context cannot read or write.
     */
    @Override
    public void remove() {
        assertAnyPermission(Permission.WRITE);

        for (Edge e : vertex.getEdges(Direction.BOTH)) {
            if (!getPermissionContext().asVisibilityFilterEdge(e).isRemovable()) {
                throw VisibilityFilterExceptionFactory.permissionDenied();
            }
        }

        super.remove();
    }
}
