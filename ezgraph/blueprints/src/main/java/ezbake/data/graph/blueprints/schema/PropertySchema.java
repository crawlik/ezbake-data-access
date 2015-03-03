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

import java.util.Map;

//TODO: review use of isValid(...) methods once we get more feedback; isValid(...) may not be needed.

/**
 * Defines a list of properties that can be used on a given element if this PropertySchema is added to it. Via {@link
 * SchemaContext} and {@link PropertySchemaManager} a {@link SchemaFilterGraph} may know about many PropertySchemas.
 * Elements on that graph can have any schema 'added' to them that the graph knows about (in a typical implementation).
 */
public interface PropertySchema {

    /**
     * Gets a map of all properties defined in this schema where the key is the property name and the value is the data
     * type of the property value. Only the String representation of the value data type is returned.
     *
     * @return the properties defined in this schema
     */
    Map<String, String> getPropertyDefinitions();

    /**
     * Checks if a key/value pair can be used in this schema.
     *
     * @param key the key of the property in question
     * @param value the value associated with the key
     * @return true if the key, value pair is valid within this schema.  False if the key does not exist or the data
     * type is not correct for this property.
     */
    boolean isValidKeyValuePair(String key, Object value);

    /**
     * Validates that a key/value pair is valid within this schema.
     *
     * @param key the key of the property in question
     * @param value the value associated with the key
     * @throws SchemaViolationException thrown if the key does not exist or the data type is not correct for this
     * property
     */
    void validateKeyValuePair(String key, Object value) throws SchemaViolationException;

    /**
     * Checks if a key can be used in this schema.
     *
     * @param key the key of the property in question
     * @return true if the key is valid within this schema
     */
    boolean isValidKey(String key);

    /**
     * Validates that a key is valid within this schema.
     *
     * @param key the key of the property in question
     * @throws SchemaViolationException thrown if the key does not exist on the schema
     */
    void validateKey(String key) throws SchemaViolationException;

    /**
     * Gets the identifier for this schema.
     *
     * @return the identifier of this schema
     */
    String getIdentifier();
}