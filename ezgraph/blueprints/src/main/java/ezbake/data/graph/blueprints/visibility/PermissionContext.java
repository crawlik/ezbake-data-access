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

import java.util.Set;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;

/**
 * Context for obtaining permissions from visibilities.
 */
public abstract class PermissionContext {

    /**
     * Element filter associated with this context.
     */
    private ElementFilter elementFilter;

    /**
     * Property filter associated with this context.
     */
    private PropertyFilter propertyFilter;

    /**
     * Create a new, abstract permission context.
     */
    protected PermissionContext() {
        this.elementFilter = new ElementFilter(this);
        this.propertyFilter = new PropertyFilter(this);
    }

    /**
     * Return permissions allowed for a given visibility.
     *
     * @param visibility platform visibility object
     * @return permissions allowed for the visibility
     */
    public abstract Set<Permission> getPermissions(final Visibility visibility);

    /**
     * Return the deserializer used for element-level visibility.
     */
    public abstract VisibilityDeserializer getElementVisibilityDeserializer();

    /**
     * Return the deserializer used for property-level visibility.
     */
    public abstract VisibilityDeserializer getPropertyVisibilityDeserializer();

    /**
     * Return true if the context has any of the given permissions on the
     * visibility.
     */
    public boolean hasAnyPermission(final Visibility visibility, Permission... permissions) {
        // Because each permission is stored in a separate vector in the visibility, it's faster to check individual
        // permissions than it is to get the set of all permissions. However, PermissionEvaluator doesn't expose that
        // method from PermissionUtils and using the cache with the slower method is still faster than the uncached
        // faster method.
        Set<Permission> ps = getPermissions(visibility);
        for (Permission p : permissions) {
            if (ps.contains(p)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Return element filter associated with this context.
     *
     * @return element filter associated with this context.
     */
    public ElementFilter getElementFilter() {
        return elementFilter;
    }

    /**
     * Return property filter associated with this context.
     *
     * @return property filter associated with this context
     */
    public PropertyFilter getPropertyFilter() {
        return propertyFilter;
    }

    /**
     * Return visibility filtered view of edge.
     *
     * If the edge already is filtered with an equivalent permission context
     * (via .equals), then return the edge. Otherwise, wrap the edge in a new
     * edge filter.
     *
     * @param edge edge to filter
     * @return filtered edge
     */
    public VisibilityFilterEdge asVisibilityFilterEdge(Edge edge) {
        if (edge == null) {
            return null;
        }

        if (edge instanceof VisibilityFilterEdge &&
                ((VisibilityFilterEdge) edge).getPermissionContext().equals(this)) {
            return (VisibilityFilterEdge) edge;
        } else {
            return new VisibilityFilterEdge(edge, this);
        }
    }

    /**
     * Return visibility filtered view of vertex.
     *
     * If the vertex already is filtered with an equivalent permission context
     * (via .equals), then return the vertex. Otherwise, wrap the vertex in a
     * new vertex filter.
     *
     * @param vertex vertex to filter
     * @return filtered vertex
     */
    public VisibilityFilterVertex asVisibilityFilterVertex(Vertex vertex) {
        if (vertex == null) {
            return null;
        }

        if (vertex instanceof VisibilityFilterVertex &&
                ((VisibilityFilterVertex) vertex).getPermissionContext().equals(this)) {
            return (VisibilityFilterVertex) vertex;
        } else {
            return new VisibilityFilterVertex(vertex, this);
        }
    }
}
