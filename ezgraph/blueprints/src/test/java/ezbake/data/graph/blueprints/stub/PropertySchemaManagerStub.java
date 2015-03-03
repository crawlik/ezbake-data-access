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

package ezbake.data.graph.blueprints.stub;

import java.util.Set;

import ezbake.data.graph.blueprints.schema.PropertySchema;
import ezbake.data.graph.blueprints.schema.PropertySchemaManager;
import ezbake.data.graph.blueprints.schema.SchemaViolationException;

/**
 * Supports a variety of tests which require an {@link ezbake.data.graph.blueprints.schema.PropertySchemaManager} as part of their collaborators.
 */
public class PropertySchemaManagerStub implements PropertySchemaManager {

    final PropertySchemaStub schema = new PropertySchemaStub();

    @Override
    public void addSchema(PropertySchema propertySchema) throws SchemaViolationException {

    }

    @Override
    public Set<String> getSchemas() {
        return null;
    }

    @Override
    public PropertySchema getSchema(String uri) {
        if ("missingSchema".equals(uri)) {
            return null;
        }
        return schema;
    }

    @Override
    public void validateSchemaExists(String uri) throws SchemaViolationException {

    }
}
