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

import java.util.Set;

/**
 * Keeps track of a list of {@link PropertySchema}s and provides accessor methods for those schemas. A {@link
 * SchemaContext} will take advantage of an implementation of this class in order to determine what schemas it knows
 * about. It is intended that one instance of PropertySchemaManager will be available for each {@link SchemaContext} and
 * one SchemaContext will be available for each {@link ezbake.data.graph.blueprints.schema.SchemaFilterGraph}.
 */
public interface PropertySchemaManager {

    /**
     * Adds a schema to this SchemaManager.
     *
     * @param propertySchema the schema to add
     * @throws SchemaViolationException thrown if the added schema is invalid or has the same name as an already
     * existing schema
     */
    void addSchema(PropertySchema propertySchema) throws SchemaViolationException;

    /**
     * Gets a set of the identifiers of all schemas known to this manager.
     *
     * @return a set of all the identifiers of all schemas
     */
    Set<String> getSchemas();

    /**
     * Checks if schema can be found in this manager.
     *
     * @param identifier the identifier to check
     */
    PropertySchema getSchema(String identifier);

    /**
     * Checks if a schema is found in this manager.
     *
     * @param identifier the uri to check
     * @throws SchemaViolationException thrown if the schema cannot be found
     */
    void validateSchemaExists(String identifier) throws SchemaViolationException;
}