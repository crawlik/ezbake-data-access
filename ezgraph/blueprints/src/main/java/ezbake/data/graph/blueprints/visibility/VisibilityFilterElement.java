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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Element;

import ezbake.base.thrift.Permission;

/**
 * Wrapper for elements that implements visibility controls.
 */
public class VisibilityFilterElement implements Element {

    /**
     * Wrapped element.
     */
    private final Element element;

    /**
     * Context for evaluating permissions on element.
     */
    private final PermissionContext context;

    /**
     * Construct a new wrapper for elements that implements visibility
     * controls.
     *
     * @param element wrapped element
     * @param ctx context for evaluating permissions on the element
     */
    public VisibilityFilterElement(Element element, PermissionContext ctx) {
        this.element = element;
        this.context = ctx;
    }

    /**
     * Get the base element wrapped by this element wrapper.
     *
     * @return the base element wrapped by this element wrapper.
     */
    public Element getBaseElement() {
        return element;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object getProperty(String key) {
        if (isVisibilityKey(key)) {
            return element.getProperty(key);
        }

        if (!hasAnyPermission(Permission.READ)) {
            return null;
        }

        Object o = element.getProperty(key);
        if (o == null) {
            return null;
        } else {
            List values = Lists.newArrayList(context.getPropertyFilter().filter(o, Permission.READ));
            if (values.isEmpty()) {
                return null;
            } else {
                return values;
            }
        }
    }

    @Override
    public Set<String> getPropertyKeys() {
        if (!hasAnyPermission(Permission.READ)) {
            return Collections.emptySet();
        }

        Set<String> allKeys = element.getPropertyKeys();
        Set<String> filteredKeys = new HashSet<>();
        for (String k : allKeys) {
            if (isVisibilityKey(k)) {
                filteredKeys.add(k);
            } else {
                Iterable<Map<String, Object>> values = context.getPropertyFilter().filter(element.getProperty(k),
                        Permission.READ);
                if (!Iterables.isEmpty(values)) {
                    filteredKeys.add(k);
                }
            }
        }

        return filteredKeys;
    }

    @Override
    public void setProperty(String key, Object value) {
        if (isVisibilityKey(key)) {
            assertAnyPermission(Permission.MANAGE_VISIBILITY);
            assertValidVisibilityObject(value);
            element.setProperty(key, value);
        } else {
            assertAnyPermission(Permission.WRITE);

            Object oldValues = element.getProperty(key);
            element.setProperty(key, context.getPropertyFilter().modify(oldValues, value));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object removeProperty(String s) {
        assertAnyPermission(Permission.WRITE);

        if (isVisibilityKey(s)) {
            throw VisibilityFilterExceptionFactory.visibilityCanNotBeRemoved();
        }

        List<Map<String, Object>> oldValues = element.getProperty(s);
        List<Map<String, Object>> newValues = Lists.newArrayList(context.getPropertyFilter().reject(oldValues, Permission.WRITE));
        element.setProperty(s, newValues);

        // Move to PropertyFilter somewhere?
        List<Map<String, Object>> removedValues = new ArrayList<>();
        for (Map<String, Object> m : oldValues) {
            if (!newValues.contains(m)) {
                removedValues.add(m);
            }
        }

        return removedValues;
    }

    @Override
    public void remove() {
        if (!isRemovable()) {
            throw VisibilityFilterExceptionFactory.permissionDenied();
        }

        element.remove();
    }

    /**
     * Return true if the element's context has permission to remove the
     * element.
     *
     * An element is removable if it and all of its property values are
     * writable.
     *
     * @return true if the element's context has permission to remove the
     * element
     */
    protected boolean isRemovable() {
        if (!hasAnyPermission(Permission.WRITE)) {
            return false;
        }

        // Check that all of the properties are removable. We can remove the element only if we can remove all of its
        // properties.
        Set<String> propertyKeys = element.getPropertyKeys();
        for (String k : propertyKeys) {
            if (!isVisibilityKey(k)) {
                List<Map<String, Object>> values = element.getProperty(k);

                if (!Iterables.isEmpty(context.getPropertyFilter().reject(values, Permission.WRITE))) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public Object getId() {
        if (hasAnyPermission(Permission.DISCOVER, Permission.READ)) {
            return element.getId();
        } else {
            return null;
        }
    }

    /**
     * Return true if the element's context has any of the given permissions on
     * the element.
     *
     * @param permissions permissions to check
     * @return true if the element's context has any of the given permission on
     * the element
     */
    public boolean hasAnyPermission(Permission... permissions) {
        return context.getElementFilter().hasAnyPermissions(element, permissions);
    }

    /**
     * Assert that the context has any of the given permissions on the element.
     * Throw an exception to indicate that permission is denied otherwise.
     *
     * @param permissions permissions to check
     * @throws IllegalArgumentException if the context does not have
     *                                            any of the given permissions
     *                                            on the element
     */
    protected void assertAnyPermission(Permission... permissions) throws IllegalArgumentException {
        if (!hasAnyPermission(permissions)) {
            throw VisibilityFilterExceptionFactory.permissionDenied();
        }
    }

    /**
     * Assert that an object contains a valid, serialized visibility object.
     *
     * @param object object to check
     * @throws IllegalArgumentException if the context does not have
     *                                            any of the given permissions
     *                                            on the element
     */
    private void assertValidVisibilityObject(Object object) throws IllegalArgumentException {
        context.getElementVisibilityDeserializer().deserialize(object);
    }

    /**
     * Return true if the property key is the key of the property containing
     * the element's visibility.
     *
     * @param key property key
     * @return true if the property key is the key of the property containing
     * the element's visibility
     */
    private static boolean isVisibilityKey(final String key) {
        return key.equals(ElementFilter.VISIBILITY_PROPERTY_KEY);
    }

    /**
     * Get the element's permission evaluation context.
     *
     * @return element's permission evaluation context
     */
    protected PermissionContext getPermissionContext() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VisibilityFilterElement that = (VisibilityFilterElement) o;

        return Objects.equal(element, that.element) && Objects.equal(context, that.context);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(element, context);
    }
}
