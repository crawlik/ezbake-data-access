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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Allows addition of schemas and has accessor methods for those schemas as well as convenience methods for listing all
 * of the URIs at which it knows schemas.  'URI' is used interchangeably with 'schema identifier' as it is intended that
 * all schema identifiers known by this class be URIs in line with RDF style.
 */
public class DefaultPropertySchemaManager implements PropertySchemaManager {

    /**
     * Map of URI/Identifier to Schema of which this manager keeps track.
     */
    private final Map<String, PropertySchema> schemas = new HashMap<>();

    @Override
    public void addSchema(PropertySchema propertySchema) throws SchemaViolationException {
        final String uri = propertySchema.getIdentifier();
        if (schemas.get(uri) != null) {
            throw new SchemaViolationException(
                    String.format("Schema already exists: %s! Cannot update or overwrite.", uri));
        }
        schemas.put(uri, propertySchema);
    }

    @Override
    public Set<String> getSchemas() {
        return Collections.unmodifiableSet(schemas.keySet());
    }

    @Override
    public PropertySchema getSchema(String uri) {
        return schemas.get(uri);
    }

    @Override
    public void validateSchemaExists(String uri) throws SchemaViolationException {
        if (!schemas.containsKey(uri)) {
            throw new SchemaViolationException(String.format("Cannot find schema at %s!", uri));
        }
    }
}
