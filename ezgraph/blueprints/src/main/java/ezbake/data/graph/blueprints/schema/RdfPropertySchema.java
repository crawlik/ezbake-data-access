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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.hp.hpl.jena.datatypes.RDFDatatype;

/**
 * Schema implementation which relies on {@link com.hp.hpl.jena.datatypes.RDFDatatype#isValidValue(Object)} to perform
 * validation on property values. Once created, RdfPropertySchema is immutable. It is intended that schema identifiers
 * will be of an RDF styled format, for example: {@code <uri>#<propertyName>} and so 'URI' is used synonymously with
 * schema's 'identifier'.
 */
public class RdfPropertySchema implements PropertySchema {

    /**
     * This schema's supported properties.
     */
    private final Map<String, RDFDatatype> propertyDefinitions;

    /**
     * The URI for this schema.
     */
    private final String uri;

    /**
     * Construct a new schema with the specified URI and properties. {@code RDFDatatype}s are assumed to be immutable.
     *
     * @param uri URI for this schema
     * @param propertyDefinitions properties defined by this schema
     */
    public RdfPropertySchema(String uri, Map<String, RDFDatatype> propertyDefinitions) {
        this.uri = uri;
        if (propertyDefinitions == null) {
            propertyDefinitions = new HashMap<>();
        }

        this.propertyDefinitions = ImmutableMap.copyOf(propertyDefinitions);
    }

    @Override
    public Map<String, String> getPropertyDefinitions() {
        final Map<String, String> stringProperties = new HashMap<>();

        for (final Map.Entry<String, RDFDatatype> entry : propertyDefinitions.entrySet()) {
            stringProperties.put(entry.getKey(), String.valueOf(entry.getValue()));
        }

        return stringProperties;
    }

    @Override
    public boolean isValidKeyValuePair(String key, Object value) {
        final RDFDatatype propertyDefinition = propertyDefinitions.get(key);
        if (propertyDefinition == null) {
            return false;
        } else {
            
            return propertyDefinitions.get(key).isValidValue(value);
        }
    }

    @Override
    public void validateKeyValuePair(String key, Object value) throws SchemaViolationException {
        if (!isValidKeyValuePair(key, value)) {
            throw new SchemaViolationException(String.format("Invalid property! Key: %s, Value: %s.", key, value));
        }
    }

    @Override
    public boolean isValidKey(String key) {
        return propertyDefinitions.get(key) != null;
    }

    @Override
    public void validateKey(String key) throws SchemaViolationException {
        if (!isValidKey(key)) {
            throw new SchemaViolationException(String.format("Key cannot be found in schema at %s!", uri));
        }
    }

    @Override
    public String getIdentifier() {
        return uri;
    }
}
