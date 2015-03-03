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

import org.apache.commons.lang.StringUtils;

/**
 * Provides methods that parse out and validate a schema enabled property key's components.
 */
public class PropertyKeyParser {

    /**
     * Schemas in this format have two parts: identifier and name, this String separates them.
     */
    private static final String SCHEMA_SEPARATOR = "#";

    /**
     * Valid property key format for properties on schema enabled graphs.
     */
    private static final String VALID_SCHEMA_KEY_FORMAT =
            String.format("<schemaIdentifier>%s<propertyName>", SCHEMA_SEPARATOR);

    /**
     * Splits and validates a property key into schema identifier and property name.
     *
     * @param propertyKey the property key to split and validate
     * @return a {@code String[]} that contains 0: schema identifier and 1: property name
     */
    private static String[] getParsedPropertyKey(String propertyKey) {
        if (StringUtils.isBlank(propertyKey)) {
            throw new SchemaViolationException("Property key must not be blank!");
        }
        final String[] keyComponents = propertyKey.split(SCHEMA_SEPARATOR);
        if (keyComponents.length != 2 || propertyKey.startsWith(SCHEMA_SEPARATOR)) {
            throw new SchemaViolationException(
                    String.format(
                            "Schemas are required for properties! The following syntax is required for a property"
                                    + " key to be valid: '%s'", VALID_SCHEMA_KEY_FORMAT));
        }
        return keyComponents;
    }

    public String getSchemaIdentifier(String key) {
        return getParsedPropertyKey(key)[0];
    }

    public String getPropertyName(String key) {
        return getParsedPropertyKey(key)[1];
    }
}
