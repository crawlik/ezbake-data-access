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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;

import ezbake.base.thrift.Permission;

/**
 * Wrapper for queries that implements visibility controls.
 */
public abstract class VisibilityFilterQuery implements Query {

    /**
     * Wrapped query.
     */
    private final Query baseQuery;

    /**
     * Context for evaluating element and property permissions.
     */
    private final PermissionContext permissionContext;

    /**
     * List of predicates that the query must satisfy to return an element.
     */
    private final List<Predicate<Element>> predicates;

    /**
     * Limit on number of returned elements. If the limit is < 0, then all matching elements are returned.
     */
    private int limit = -1;

    /**
     * Create a new query wrapper.
     *
     * @param query query to wrap
     * @param ctx permissions context
     */
    public VisibilityFilterQuery(Query query, PermissionContext ctx) {
        this.baseQuery = query;
        this.permissionContext = ctx;
        this.predicates = new ArrayList<>();
        this.predicates.add(ctx.getElementFilter().hasAnyPermissionPredicate(Permission.DISCOVER, Permission.READ));
    }

    /**
     * Return the permission context used by this query.
     *
     * @return permission context used by this query
     */
    protected PermissionContext getPermissionContext() {
        return permissionContext;
    }

    @Override
    public Query has(final String key) {
        predicates.add(hasKeyPredicate(key));

        return this;
    }

    @Override
    public Query hasNot(final String key) {
        predicates.add(Predicates.not(hasKeyPredicate(key)));

        return this;
    }

    @Override
    public Query has(final String key, final Object value) {
        predicates.add(blueprintsPredicate(key, com.tinkerpop.blueprints.Compare.EQUAL, value));

        return this;
    }

    @Override
    public Query hasNot(final String key, final Object value) {
        predicates.add(Predicates.not(blueprintsPredicate(key, com.tinkerpop.blueprints.Compare.EQUAL, value)));

        return this;
    }

    @Override
    public Query has(String key, com.tinkerpop.blueprints.Predicate predicate, Object value) {
        predicates.add(blueprintsPredicate(key, com.tinkerpop.blueprints.Compare.EQUAL, value));

        return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T extends Comparable<T>> Query has(String s, T t, Compare compare) {
        throw new UnsupportedOperationException("Deprecation");
    }

    @Override
    public <T extends Comparable<?>> Query interval(String key, T startValue, T endValue) {
        predicates.add(blueprintsPredicate(key, com.tinkerpop.blueprints.Compare.GREATER_THAN_EQUAL, startValue));
        predicates.add(blueprintsPredicate(key, com.tinkerpop.blueprints.Compare.LESS_THAN, endValue));

        return this;
    }

    @Override
    public Query limit(int i) {
        limit = i;

        return this;
    }

    /**
     * Filter an iterable of elements against the combination of all
     * predicates on this query.
     *
     * @param it iterable of elements
     * @return elements that match every predicated on this query
     */
    protected <T extends Element> Iterable<T> filterElements(Iterable<T> it) {
        return Iterables.filter(it, Predicates.and(predicates));
    }

    /**
     * If a limit is set on the query, limit the number of items returned.
     *
     * @param it iterable to limit
     * @return iterable with at most a certain number of elements
     */
    protected <T> Iterable<T> limitElements(Iterable<T> it) {
        if (limit >= 0) {
            return Iterables.limit(it, limit);
        } else {
            return it;
        }
    }

    @Override
    public Iterable<Edge> edges() {
        Iterable<Edge> it = baseQuery.edges();
        it = filterElements(it);
        it = limitElements(it);

        return permissionContext.getElementFilter().asVisibilityFilterEdges(it);
    }

    @Override
    public Iterable<Vertex> vertices() {
        Iterable<Vertex> it = baseQuery.vertices();
        it = filterElements(it);
        it = limitElements(it);

        return permissionContext.getElementFilter().asVisibilityFilterVertices(it);
    }

    private <T extends Element> Predicate<T> hasKeyPredicate(final String key) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T element) {
                Iterable<Map<String, Object>> values = permissionContext.getPropertyFilter().filter(element.getProperty(key), Permission.DISCOVER, Permission.READ);

                return values != null && !Iterables.isEmpty(values);
            }
        };
    }

    /**
     * Create a Guava predicate from a Blueprints predicate.
     *
     * @param key element property key to check
     * @param predicate predicate to check element property value
     * @param value value as other argument to predicate
     * @return Guava predicate wrapping Blueprints predicate
     */
    private <T extends Element> Predicate<T> blueprintsPredicate(final String key,
                                                          final com.tinkerpop.blueprints.Predicate predicate,
                                                          final Object value) {
        return new Predicate<T>() {
            @Override
            public boolean apply(T element) {
                List<Map<String, Object>> values = element.getProperty(key);
                if (values == null) {
                    return false;
                }

                Iterable<Map<String, Object>> it = Iterables.filter(values, Predicates.and(
                        permissionContext.getPropertyFilter().hasAnyPermissionPredicate(Permission.DISCOVER, Permission.READ),
                        blueprintsValuePredicate(predicate, value)));

                return !Iterables.isEmpty(it);
            }
        };
    }

    /**
     * Create a Guava predicate for a single property value from a Blueprints
     * predicate.
     *
     * @param predicate Blueprints predicate on property value
     * @param value other operand to Blueprints predicate
     * @return Guava predicate wrapping Blueprints predicate
     */
    private static Predicate<Map<String, Object>> blueprintsValuePredicate(final com.tinkerpop.blueprints.Predicate predicate,
                                                                           final Object value) {
        return new Predicate<Map<String, Object>>() {
            @Override
            public boolean apply(Map<String, Object> propertyValueMap) {
                Object o = propertyValueMap.get(PropertyFilter.VALUE_KEY);

                return predicate.evaluate(o, value);
            }
        };
    }
}
