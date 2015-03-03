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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;

import ezbake.base.thrift.Visibility;
import ezbake.data.graph.blueprints.visibility.PropertyFilter;
import ezbake.thrift.ThriftUtils;

/**
 * A typical SchemaContext that uses a {@link PropertySchemaManager} to keep track of schemas. It is intended that
 * schema identifiers will be of an RDF styled format, for example: {@code <uri>#<propertyName>} and so 'URI' is used
 * synonymously with schema's 'identifier'.
 */
public class DefaultSchemaContext extends SchemaContext {

    /**
     * Deletes are not allowed in schemas.  Trying to set the value with this value in any {@linkplain
     * PropertyFilter#DELETE_KEY delete field} will result in an exception.
     */
    private static final Boolean DELETE_FLAG_TRUE = true;

    /**
     * SchemaManager that keeps track of schemas available in this context.
     */
    private final PropertySchemaManager schemaManager;

    /**
     * An empty visibility for ensuring only empty visibilities are used anywhere in the {@linkplain
     * SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property}.
     */
    private final String emptyVisibility;

    /**
     * Constructs a new DefaultSchemaContext with the specified PropertySchemaManager
     *
     * @param propertySchemaManager the SchemaManager used to keep track of schemas in this context
     */
    public DefaultSchemaContext(PropertySchemaManager propertySchemaManager) {
        this.schemaManager = propertySchemaManager;
        try {
            emptyVisibility = ThriftUtils.serializeToBase64(new Visibility());
        } catch (final TException e) {
            throw new RuntimeException(
                    "Unexpected thrift error occurred initializing DefaultSchemaContext. Serialization of empty "
                            + "visibility failed.");
        }
    }

    @Override
    public void validateSchemaKey(String identifier, String propertyName) throws SchemaViolationException {
        schemaManager.validateSchemaExists(identifier);
        schemaManager.getSchema(identifier).validateKey(propertyName);
    }

    @Override
    public void validateSchemaKeyValue(String identifier, String propertyName, Object value)
            throws SchemaViolationException {
        schemaManager.validateSchemaExists(identifier);
        schemaManager.getSchema(identifier).validateKeyValuePair(propertyName, value);
    }

    @Override
    public void validateSchemaUpdate(List<Map<String, Object>> schemaValue) throws SchemaViolationException {
        final List<String> missingSchemas = new ArrayList<>();

        for (final Map<String, Object> map : schemaValue) {

            final Object singleSchemaValue = map.get(PropertyFilter.VALUE_KEY);

            if (singleSchemaValue == null) {
                throw new SchemaViolationException("Null schema values are not allowed!");
            }

            String schemaUri = null;
            if (singleSchemaValue.getClass().equals(String.class)) {
                schemaUri = (String) singleSchemaValue;
            } else {
                throw new SchemaViolationException("Schema value must be a String!");
            }

            if (StringUtils.isBlank(schemaUri)) {
                throw new SchemaViolationException("Empty schema values are not allowed!");
            }

            if (DELETE_FLAG_TRUE.equals(map.get(PropertyFilter.DELETE_KEY))) {
                throw new SchemaViolationException(
                        String.format(
                                "Deleting schemas is not allowed! Attempted to delete: %s",
                                map.get(PropertyFilter.VALUE_KEY)));
            }

            if (!emptyVisibility.equals(map.get(PropertyFilter.VISIBILITY_KEY))) {
                throw new SchemaViolationException(
                        String.format(
                                "Only empty visibilities on schemas are supported. Attempted to add non-empty "
                                        + "visibility to schema: %s", schemaUri));
            }

            if (schemaManager.getSchema(schemaUri) == null) {
                missingSchemas.add(schemaUri);
            }
        }

        if (!missingSchemas.isEmpty()) {
            throw new SchemaViolationException(
                    String.format(
                            "Error while attempting to add a schema to an element. Schema(s) cannot be found: %s",
                            missingSchemas));
        }
    }

    @Override
    public PropertyKeyParser getPropertyKeyParser() throws SchemaViolationException {
        return new PropertyKeyParser();
    }
}
