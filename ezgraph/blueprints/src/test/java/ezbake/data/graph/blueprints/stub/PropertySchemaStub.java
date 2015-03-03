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

import java.util.Map;

import ezbake.data.graph.blueprints.schema.PropertySchema;
import ezbake.data.graph.blueprints.schema.SchemaViolationException;

/**
 * Stub for schema used for validating input.
 */
public class PropertySchemaStub implements PropertySchema {

    //members variables for validating input
    public boolean isValidKeyValueCalled;
    public String isValidKeyValueKey;
    public Object isValidKeyValueValue;

    public boolean validateKeyValueCalled;
    public String validateKeyValueKey;
    public Object validateKeyValueValue;

    public boolean isValidKeyCalled;
    public String isValidKeyKey;

    public boolean validateKeyCalled;
    public String validateKeyKey;

    public boolean getPropertiesCalled;

    @Override
    public Map<String, String> getPropertyDefinitions() {
        getPropertiesCalled = true;
        return null;
    }

    @Override
    public boolean isValidKeyValuePair(String key, Object value) {
        isValidKeyValueCalled = true;
        isValidKeyValueKey = key;
        isValidKeyValueValue = value;
        return false;
    }

    @Override
    public void validateKeyValuePair(String key, Object value) throws SchemaViolationException {
        validateKeyValueCalled = true;
        validateKeyValueKey = key;
        validateKeyValueValue = value;
    }

    @Override
    public boolean isValidKey(String key) {
        isValidKeyCalled = true;
        isValidKeyKey = key;
        return false;
    }

    @Override
    public void validateKey(String key) throws SchemaViolationException {
        validateKeyCalled = true;
        validateKeyKey = key;
    }

    @Override
    public String getIdentifier() {
        return null;
    }
}
