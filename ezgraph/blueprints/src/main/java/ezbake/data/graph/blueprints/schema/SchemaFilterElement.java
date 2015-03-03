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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Element;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.data.graph.blueprints.visibility.NullVisibilityDeserializer;
import ezbake.data.graph.blueprints.visibility.PermissionContext;
import ezbake.data.graph.blueprints.visibility.PropertyFilter;
import ezbake.data.graph.blueprints.visibility.VisibilityDeserializer;

/**
 * Wrapper for Element  that works with schema filter classes. A lot of the action in the schema-filter happens here.
 * The schema filter's primary function is to limit what properties can be set on an element. For that purpose, a
 * significant amount of logic is in the {@code setProperty(...)} method.
 */
public class SchemaFilterElement implements Element {

    /**
     * Reserved property key. This key is handled differently than other properties.
     */
    public static final String VISIBILITY_PROPERTY_KEY = "ezbake_visibility";

    /**
     * Reserved property key. This key is handled differently than other properties and affects what other properties
     * can be set on an element.
     */
    public static final String SCHEMA_PROPERTY_KEY = "ezbake_schema";

    /**
     * Element wrapped by this.
     */
    private final Element element;

    /**
     * SchemaContext in which this SchemaFilterElement operates.
     */
    private final SchemaContext context;

    /**
     * PropertyFilter used for validating property structure and other useful methods.
     */
    //TODO: @charles moves shared methods into new class in shared package.
    private final PropertyFilter propertyFilter;

    /**
     * Constructs a new SchemaFilterElement wrapping the given Element and in the given SchemaContext.
     *
     * @param element the element to wrap with this
     * @param context the context this SchemaFilterElement is operating in
     */
    public SchemaFilterElement(Element element, SchemaContext context) {
        this.element = element;
        this.context = context;

        propertyFilter = new PropertyFilter(
                new PermissionContext() {
                    @Override
                    public Set<Permission> getPermissions(Visibility visibility) {
                        return null;
                    }

                    @Override
                    public VisibilityDeserializer getElementVisibilityDeserializer() {
                        return new NullVisibilityDeserializer();
                    }

                    @Override
                    public VisibilityDeserializer getPropertyVisibilityDeserializer() {
                        return new NullVisibilityDeserializer();
                    }
                });
    }

    /**
     * Converts a {@code List<Map<String,Object>>} that contains arbitrary values into a {@code Set<Object>} of the
     * contained property values, duplicate values removed. The values under {@linkplain PropertyFilter#VALUE_KEY the
     * value key} in the maps correspond to the property value.
     *
     * @param value a property value in the form {@code List<Map<String,Object>>} from which to get a set of values
     * contained in the listed maps
     * @return a set containing all values (duplicates removed) tied to an element
     */
    @SuppressWarnings("unchecked")
    private static Set<Object> getPropertyValues(Object value) {
        final List<Map<String, Object>> propertyValue = (List<Map<String, Object>>) value;
        final Set<Object> propertyValues = new HashSet<>();
        for (final Map<String, Object> val : propertyValue) {
            propertyValues.add(val.get(PropertyFilter.VALUE_KEY));
        }
        return propertyValues;
    }

    /**
     * Converts an {@code List<Map<String,Object>>} that contains schema identifiers into a {@code Set<String>} of the
     * contained schema identifiers. The values under {@linkplain PropertyFilter#VALUE_KEY the value key} in the maps
     * correspond to the schema identifiers.
     *
     * @param value a value appropriate for or from the {@linkplain ezbake.data.graph.blueprints.schema.SchemaFilterElement#SCHEMA_PROPERTY_KEY schema}
     * property from which to get a set of schema identifiers
     * @return a set containing all of schema identifiers tied to an element
     */
    @SuppressWarnings("unchecked")
    private static Set<String> getSchemaIdentifiers(Object value) {
        final List<Map<String, Object>> schemaPropertyValue = (List<Map<String, Object>>) value;
        final Set<String> schemaIdentifiers = new HashSet<>();
        if (schemaPropertyValue != null) {

            for (final Map<String, Object> val : schemaPropertyValue) {
                schemaIdentifiers.add((String) val.get(PropertyFilter.VALUE_KEY));
            }
        }
        return schemaIdentifiers;
    }

    /**
     * Checks if a given property key is the visibility key.
     *
     * @param key key to check
     * @return true if the given key is the visibility key
     */
    private static boolean isVisibilityKey(final String key) {
        return key.equals(VISIBILITY_PROPERTY_KEY);
    }

    /**
     * Checks if a given property key is the schema key.
     *
     * @param key key to check
     * @return true if the given key is the schema key
     */
    private static boolean isSchemaKey(final String key) {
        return key.equals(SCHEMA_PROPERTY_KEY);
    }

    /**
     * Validates that a schema exists on a given element.
     *
     * @param schemaPropValue value of the {@linkplain ezbake.data.graph.blueprints.schema.SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property}
     * @param identifier the identifier of the schema to validate
     * @throws SchemaViolationException thrown if the schema in question is not on the given element
     */
    private static void validateSchemaWithElement(List<Map<String, Object>> schemaPropValue, String identifier)
            throws SchemaViolationException {
        final Set<String> availableSchemas = getSchemaIdentifiers(schemaPropValue);
        if (!availableSchemas.contains(identifier)) {
            throw new SchemaViolationException(String.format("Element does not contain schema: %s", identifier));
        }
    }

    @Override
    public <T> T getProperty(String key) {
        return element.getProperty(key);
    }

    @Override
    public Set<String> getPropertyKeys() {
        return element.getPropertyKeys();
    }

    /**
     * Sets a property on this element. The schema filter classes regulate which properties can be set on an {@link
     * com.tinkerpop.blueprints.Element}. Checks for validity of a property within the {@link SchemaContext} take place
     * here when setting a property.
     *
     * @param key key to check for validity within current SchemaContext before adding
     * @param value value to check for validity within current SchemaContext before adding
     */
    @SuppressWarnings("unchecked")
    @Override
    public void setProperty(String key, Object value) {
        //@blame Charles
        if (!VISIBILITY_PROPERTY_KEY.equals(key)) {
            propertyFilter.validate(value);
        }

        if (isSchemaKey(key)) {
            context.validateSchemaUpdate((List<Map<String, Object>>) value);
        } else if (!isVisibilityKey(key)) {
            //All other keys, except visibility which is schema independent:
            final PropertyKeyParser parser = context.getPropertyKeyParser();

            final String schemaIdentifier = parser.getSchemaIdentifier(key);
            final String propertyName = parser.getPropertyName(key);

            validateSchemaWithElement(
                    (List<Map<String, Object>>) element.getProperty(SCHEMA_PROPERTY_KEY), schemaIdentifier);

            context.validateSchemaKey(parser.getSchemaIdentifier(key), parser.getPropertyName(key));
            for (final Object aValue : getPropertyValues(value)) {
                context.validateSchemaKeyValue(schemaIdentifier, propertyName, aValue);
            }
        }
        element.setProperty(key, value);
    }

    @Override
    public <T> T removeProperty(String key) {
        return element.removeProperty(key);
    }

    @Override
    public void remove() {
        element.remove();
    }

    @Override
    public Object getId() {
        return element.getId();
    }

    /**
     * Gets the SchemaContext under which this SchemaFilterElement is operating.
     *
     * @return the context in which this SchemaFilterElement is
     */
    protected SchemaContext getContext() {
        return context;
    }
}
