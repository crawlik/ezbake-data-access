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

package ezbake.data.graph.blueprints.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import ezbake.base.thrift.Visibility;
import ezbake.data.graph.blueprints.visibility.PropertyFilter;
import ezbake.thrift.ThriftUtils;

/**
 * Static methods to make tests easier to read and write.
 */
public class SchemaTestHelpers {

    /**
     * {@code getStartingSchemaValue} adds this uri to the schema value returned.
     */
    public static final String ELEMENT_STARTING_SCHEMA = "http://myuri/zebra";

    /**
     * Get a property value for the {@linkplain ezbake.data.graph.blueprints.schema
     * .SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property} that has a new schema added in on top of the schema
     * property value initialized in {@code setUp()}.
     *
     * @param newValue Schema URI to add to the schemas in this schema value.
     * @return a new schema value with more schemas than {@code setUp()} initializes the system under test with
     */
    public static List<Map<String, Object>> getNewSchemaValue(String newValue) {
        final Map<String, Object> newSchemaValue = new HashMap<>();
        newSchemaValue.put(PropertyFilter.VALUE_KEY, newValue);
        newSchemaValue.put(PropertyFilter.VISIBILITY_KEY, getEncodedVisibility());
        final List<Map<String, Object>> schemaValue = getStartingSchemaValue();
        schemaValue.add(newSchemaValue);

        return schemaValue;
    }

    /**
     * Get a multi-valued property value with its values of type Object.
     *
     * @return a multi-valued property value with its values of type Object
     */
    public static List<Map<String, Object>> getObjectPropertyValue(Object anyValue) {
        final Map<String, Object> valueEntry = new HashMap<>();
        valueEntry.put(PropertyFilter.VALUE_KEY, anyValue);
        valueEntry.put(PropertyFilter.VISIBILITY_KEY, getEncodedVisibility());
        final List<Map<String, Object>> value = new ArrayList<>();
        value.add(valueEntry);

        return value;
    }

    /**
     * /** Get a valid schema value, fitting for testing that requires an element have its {@linkplain
     * ezbake.data.graph.blueprints.schema.SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property} be set.
     *
     * @return a valid value for the schema property
     */
    public static List<Map<String, Object>> getStartingSchemaValue() {
        final List<Map<String, Object>> value = new ArrayList<>();
        final Map<String, Object> availableSchema = new HashMap<>();
        availableSchema.put(PropertyFilter.VALUE_KEY, ELEMENT_STARTING_SCHEMA);
        availableSchema.put(PropertyFilter.VISIBILITY_KEY, getEncodedVisibility());
        value.add(availableSchema);
        return value;
    }

    /**
     * Get a base64 encoded, empty visibility object.  It is required that all values for a given property have a
     * serialized visibility in their map and the  {@linkplain ezbake.data.graph.blueprints.schema
     * .SchemaFilterElement#SCHEMA_PROPERTY_KEY schema property} is not allowed to have non blank visibilities therefor
     * the values for the schema property are supplied with these serialized, blank visibilities.
     *
     * @return a base64 encoded blank visibility object
     */
    public static String getEncodedVisibility() {
        try {
            return ThriftUtils.serializeToBase64(new Visibility());
        } catch (final TException e) {
            throw new RuntimeException("Unexpected thrift exception.", e);
        }
    }
}
