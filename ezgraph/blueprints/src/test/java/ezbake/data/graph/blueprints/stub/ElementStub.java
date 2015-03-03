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

import com.tinkerpop.blueprints.Element;

import ezbake.data.graph.blueprints.schema.SchemaFilterElement;
import ezbake.data.graph.blueprints.util.SchemaTestHelpers;

/**
 * Stub used for tests that require an element.  Used for validation of input with some helper methods and canned
 * responses for tests.
 */
public class ElementStub implements Element {

    // variables used for validation of input
    public boolean getPropertyCalled;
    public String getPropertyKey;

    public boolean getPropertyKeysCalled;

    public boolean setPropertyCalled;
    public String setPropertyKey;
    public Object setPropertyValue;

    public boolean removePropertyCalled;
    public String removePropertyKey;

    public boolean removeCalled;

    public boolean getIdCalled;

    @Override
    public Object getProperty(String key) {
        getPropertyCalled = true;
        getPropertyKey = key;

        if (SchemaFilterElement.SCHEMA_PROPERTY_KEY.equals(key)) {
            return SchemaTestHelpers.getStartingSchemaValue();
        }
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        getPropertyKeysCalled = true;
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {
        setPropertyCalled = true;
        setPropertyKey = key;
        setPropertyValue = value;
    }

    @Override
    public <T> T removeProperty(String key) {
        removePropertyCalled = true;
        removePropertyKey = key;
        return null;
    }

    @Override
    public void remove() {
        removeCalled = true;
    }

    @Override
    public Object getId() {
        getIdCalled = true;
        return "id";
    }
}
