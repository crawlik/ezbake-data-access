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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;

//@Charles TODO: move methods "cast(...)" and "validate(...)" into new class in shared package.

/**
 * Filters property value lists on the basis of the visibilities embedded into
 * property values.
 */
public class PropertyFilter {

    /**
     * Key for value field in property value.
     */
    public static final String VALUE_KEY = "value";

    /**
     * Key for visibility field in property value.
     */
    public static final String VISIBILITY_KEY = "visibility";

    /**
     * Key for delete field in property value.
     */
    public static final String DELETE_KEY = "delete";

    /**
     * Context for evaluating property permissions.
     */
    private final PermissionContext context;

    /**
     * Construct a new property value filter.
     *
     * @param context permission context
     */
    public PropertyFilter(PermissionContext context) {
        this.context = context;
    }

    /**
     * Validate property value object to have the correct types and structure.
     * <p/>
     * In particular, the object must be a non-null list of maps from string to
     * object. The map must contain keys "value" mapped to an arbitrary value,
     * a key "visibility" mapped to an EzBake visibility object, and optionally
     * a key "delete" mapped to a boolean. The map must not have any other
     * keys.
     *
     * @param object property value object
     * @throws NullPointerException     if the object is null
     * @throws IllegalArgumentException if the object has incorrect
     *                                            type or structure
     */
    public void validate(Object object) {
        if (object == null) {
            throw new NullPointerException("Cannot validate null object");
        }

        if (!(object instanceof List)) {
            throw new IllegalArgumentException("Property values must be list of maps");
        }

        List list = (List) object;
        for (Object element : list) {
            if (!(element instanceof Map)) {
                throw new IllegalArgumentException("Property values must be list of maps");
            }

            Map map = (Map) element;
            if (!map.containsKey(VALUE_KEY)) {
                throw new IllegalArgumentException(String.format("Property value map must contain a \"%s\" field", VALUE_KEY));
            }

            if (!map.containsKey(VISIBILITY_KEY)) {
                throw new IllegalArgumentException(String.format("Property value map must contain a \"%s\" field", VISIBILITY_KEY));
            }

            for (Object key : map.keySet()) {
                if (!(key instanceof String)) {
                    throw new IllegalArgumentException("Property value keys must be strings");
                }

                String keyString = (String) key;
                switch (keyString) {
                    case VALUE_KEY:
                        break;
                    case VISIBILITY_KEY:
                        context.getPropertyVisibilityDeserializer().deserialize(map.get(VISIBILITY_KEY));
                        break;
                    case DELETE_KEY:
                        if (!(map.get(DELETE_KEY) instanceof Boolean)) {
                            throw new IllegalArgumentException(String.format("\"%s\" field must be boolean", DELETE_KEY));
                        }
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("Invalid property value key: %s", keyString));
                }
            }
        }
    }

    /**
     * Cast an object containing a list of property value maps to an actual
     * list of property value maps.
     *
     * Used so that unchecked cast warnings only have to be suppressed in a
     * single place.
     *
     * @param object object to cast
     * @return a list of property value maps
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> cast(Object object) {
        if (object == null) {
            return Collections.emptyList();
        }

        validate(object);

        return (List<Map<String, Object>>) object;
    }

    /**
     * Filter an iterable of property values, returning the values for which the
     * contained context has any of the given permissions.
     *
     * @param object iterable of values to check
     * @param permissions list of permissions
     * @return iterable of property values this token can access
     */
    public Iterable<Map<String, Object>> filter(Object object, Permission... permissions) {
        return filter(cast(object), permissions);
    }

    /**
     * Filter an iterable of properties, returning the values for which the
     * contained context has any of the given permissions.
     *
     * @param values iterable of values
     * @param permissions list of permissions
     * @return iterable of properties that this token can read
     */
    public Iterable<Map<String, Object>> filter(Iterable<Map<String, Object>> values, Permission... permissions) {
        return Iterables.filter(values, hasAnyPermissionPredicate(permissions));
    }

    /**
     * Filter an iterable of properties, rejecting the values for which the
     * contained context has any of the given permissions. Return an iterable
     * of values that this token does <em>not</em> have any of the given
     * permissions on.
     *
     * @param values iterable of values
     * @param permissions list of permissions to check
     * @return iterable of values that this token does not have any of the
     * permissions for.
     */
    public Iterable<Map<String, Object>> reject(Iterable<Map<String, Object>> values, Permission... permissions) {
        return Iterables.filter(values, Predicates.not(hasAnyPermissionPredicate(permissions)));
    }

    /**
     * Modify a list of values by adding/deleting additional values.
     *
     * @param oldObject existing values
     * @param newObject new values to add or delete
     * @return modified list of values
     */
    public List<Map<String, Object>> modify(Object oldObject, Object newObject) {
        List<Map<String, Object>> oldValues = cast(oldObject);
        List<Map<String, Object>> newValues = cast(newObject);

        List<Map<String, Object>> modifiedValues = new ArrayList<>(oldValues);
        for (Map<String, Object> x : newValues) {
            Visibility visibility = getValueVisibility(x);
            if (hasAnyPermission(visibility, Permission.WRITE)) {
                Object d = x.get(DELETE_KEY);

                if (d != null && d instanceof Boolean && (Boolean) d) {
                    removeValue(modifiedValues, x);
                } else {
                    addValue(modifiedValues, x);
                }
            }
        }

        return modifiedValues;
    }

    /**
     * Construct a new predicate that evaluates true if any of the given
     * permissions are satisfied a property value map.
     *
     * @param permissions permissions to check
     * @return predicate that evaluates true if any of the give permissions
     * are satisfied a property value map
     */
    public Predicate<Map<String, Object>> hasAnyPermissionPredicate(final Permission... permissions) {
        return new Predicate<Map<String, Object>>() {
            @Override
            public boolean apply(Map<String, Object> propertyValueMap) {
                return hasAnyPermission(getValueVisibility(propertyValueMap), permissions);
            }
        };
    }

    /**
     * Return true if two property value maps are equal.
     *
     * @param x first property value map
     * @param y second property value map
     * @return true if the two property value maps are equal
     */
    private static boolean valuesEquals(final Map<String, Object> x, final Map<String, Object> y) {
        return x.get(VALUE_KEY).equals(y.get(VALUE_KEY)) && x.get(VISIBILITY_KEY).equals(y.get(VISIBILITY_KEY));
    }

    /**
     * Add a property value to a list if an element with matching value and
     * visibility is not already in the list. If the element exists, do
     * nothing.
     *
     * @param list list of property values
     * @param val property value to attempt to add to the list
     */
    private void addValue(List<Map<String, Object>> list, Map<String, Object> val) {
        for (Map<String, Object> e : list) {
            if (valuesEquals(e, val)) {
                return;
            }
        }

        list.add(val);
    }

    /**
     * Attempt to remove an element from a list of elements. If the element
     * does not exist in the list, do nothing.
     *
     * @param list list of property values
     * @param val property value to attempt to remove from the list
     */
    private void removeValue(List<Map<String, Object>> list, Map<String, Object> val) {
        Iterator<Map<String, Object>> it = list.iterator();
        while (it.hasNext()) {
            Map<String, Object> e = it.next();
            if (valuesEquals(e, val)) {
                it.remove();
            }
        }
    }

    /**
     * Return true if the security token has permission on a given visibility.
     *
     * @param visibility platform visibility object
     * @param permissions permissions to check
     * @return true if the security token has permission on a given visibility
     */
    private boolean hasAnyPermission(Visibility visibility, Permission... permissions) {
        Set<Permission> ps = context.getPermissions(visibility);
        for (Permission p : permissions) {
            if (ps.contains(p)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Deserialize visibility from property value. The property value should
     * have passed through
     * {@link ezbake.data.graph.blueprints.visibility.PropertyFilter#validate}.
     *
     * @param val property value
     * @return deserialized visibility object
     * @throws IllegalArgumentException if the object could not be
     *                                         deserialized. Because the object
     *                                         should have been passed through
     *                                         validate, the object should
     *                                         always be able to be
     *                                         deserialized.
     */
    private Visibility getValueVisibility(Map<String, Object> val) {
        Object o = val.get(VISIBILITY_KEY);

        return context.getPropertyVisibilityDeserializer().deserialize(o);
    }
}
