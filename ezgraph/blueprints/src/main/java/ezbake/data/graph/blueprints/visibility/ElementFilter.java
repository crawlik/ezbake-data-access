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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;

/**
 * Filters elements and collections of elements based on element-level
 * visibilities.
 */
public class ElementFilter {

    /**
     * Element property key for element visibility.
     */
    public static final String VISIBILITY_PROPERTY_KEY = "ezbake_visibility";

    /**
     * Permission context associated with this filter.
     */
    private final PermissionContext context;

    /**
     * Construct a new element filter.
     *
     * @param ctx context for evaluating permissions on the element.
     */
    public ElementFilter(PermissionContext ctx) {
        this.context = ctx;
    }

    /**
     * Return the set of permissions the associated context has on the element.
     *
     * If an element does not have a visibility property, then return all
     * permissions. If an element has a visibility property, but its contents
     * are malformed, throw an error.
     *
     * @param element element whose permissions to check
     * @return set of permissions the associated context has on the element
     */
    public Set<Permission> getPermissions(Element element) {
        return context.getPermissions(getVisibility(element));
    }

    /**
     * Return true if the element's visibility grants any of the given permissions.
     *
     * If the element does not have a visibility, then returns true. If it has
     * a visibility property, but that property does not contain a base64
     * encoded serialized Visibility object, returns false.
     *
     * @param element element whose visibility to check
     * @param permissions permissions to check
     * @return true if the element's visibility grants any of the permissions
     */
    public boolean hasAnyPermissions(Element element, final Permission... permissions) {
        return context.hasAnyPermission(getVisibility(element), permissions);
    }

    /**
     * Filter vertices retaining only those for which the context has
     * permissions.
     *
     * Elements in the resulting iterable are instances of
     * VisibilityFilterVertex.
     *
     * @param iterable iterable of vertices to filter
     * @return iterable of vertices that the security token can read
     */
    public Iterable<Vertex> filterVertices(Iterable<Vertex> iterable, final Permission... permissions) {
        Iterable<Vertex> it = Iterables.filter(iterable, hasAnyPermissionPredicate(permissions));

        return asVisibilityFilterVertices(it);
    }

    /**
     * Filter edges retaining only those that the context can read.
     *
     * Elements in the resulting iterable are instances of
     * VisibilityFilterEdge.
     *
     * @param iterable iterable of edges to filter
     * @return iterable of edges that the security token can read
     */
    public Iterable<Edge> filterEdges(Iterable<Edge> iterable, Permission... permissions) {
        Iterable<Edge> it = Iterables.filter(iterable, hasAnyPermissionPredicate(permissions));

        return asVisibilityFilterEdges(it);
    }

    /**
     * Convert an iterable of vertices to filtered vertices.
     *
     * @param vertices iterable of vertices to convert
     * @return iterable of {@link ezbake.data.graph.blueprints.visibility.VisibilityFilterVertex}
     */
    public <T extends Vertex> Iterable<Vertex> asVisibilityFilterVertices(Iterable<T> vertices) {
        return Iterables.transform(vertices, new Function<T, Vertex>() {
            @Override
            public Vertex apply(T t) {
                return context.asVisibilityFilterVertex(t);
            }
        });
    }

    /**
     * Convert an iterable of edges to filtered edges.
     *
     * @param edges iterable of edges to convert
     * @return iterable of {@link ezbake.data.graph.blueprints.visibility.VisibilityFilterEdge}
     */
    public <T extends Edge> Iterable<Edge> asVisibilityFilterEdges(Iterable<T> edges) {
        return Iterables.transform(edges, new Function<T, Edge>() {
            @Override
            public Edge apply(T t) {
                return context.asVisibilityFilterEdge(t);
            }
        });
    }

    /**
     * Create a predicate that checks if an element has any of the given
     * permissions.
     *
     * The predicate returns true if any of context has any of the given
     * permissions.
     *
     * @param permissions permissions to check
     * @return predicate returning true if an element has any given permissions
     */
    public <T extends Element> Predicate<T> hasAnyPermissionPredicate(final Permission... permissions) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T t) {
                return hasAnyPermissions(t, permissions);
            }
        };
    }

    /**
     * Get visibility from hidden property on element.
     *
     * @param element graph element to get visibility for
     * @return element's visibility or empty visibility if no visibility
     * property is present
     */
    private Visibility getVisibility(Element element) {
        Object o = element.getProperty(VISIBILITY_PROPERTY_KEY);
        if (o == null) {
            return new Visibility();
        }

        return context.getElementVisibilityDeserializer().deserialize(o);
    }
}
