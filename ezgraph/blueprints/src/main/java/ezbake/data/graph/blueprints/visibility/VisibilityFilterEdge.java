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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import ezbake.base.thrift.Permission;

/**
 * Wrapper for edges that implements visibility controls.
 */
public class VisibilityFilterEdge extends VisibilityFilterElement implements Edge {

    /** Wrapped (base) edge */
    private final Edge edge;

    /**
     * Construct a new filtered edge.
     *
     * @param edge edge to wrap
     * @param ctx permission context to obtain permissions on edge
     */
    public VisibilityFilterEdge(Edge edge, PermissionContext ctx) {
        super(edge, ctx);

        this.edge = edge;
    }

    /**
     * Get the edge this wrapper delegates its operations to.
     *
     * @return edge this wrapper delegates its operations to
     */
    public Edge getBaseEdge() {
        return edge;
    }

    /**
     * Return the tail/out or head/in vertex.
     *
     * Requires read permission on the edge and discover or read permission on
     * the target vertex. Returns null if otherwise.
     *
     * @param direction whether to return the tail/out or head/in vertex
     * @return the tail/out or head/in vertex
     * @throws IllegalArgumentException is thrown if a direction of both is provided
     */
    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (!hasAnyPermission(Permission.READ)) {
            return null;
        }

        VisibilityFilterVertex v = getPermissionContext().asVisibilityFilterVertex(edge.getVertex(direction));
        if (v.hasAnyPermission(Permission.DISCOVER, Permission.READ)) {
            return v;
        } else {
            return null;
        }
    }

    /**
     * Return the label associated with the edge.
     *
     * Requires read permission on the edge.
     *
     * @return the label associated with the edge
     */
    @Override
    public String getLabel() {
        assertAnyPermission(Permission.READ);

        return edge.getLabel();
    }
}
