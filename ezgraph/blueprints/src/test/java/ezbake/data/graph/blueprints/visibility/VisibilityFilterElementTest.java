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

package ezbake.data.graph.blueprints.visibility;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.ValidityCaveats;
import org.apache.thrift.TException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ezbake.base.thrift.Visibility;
import ezbake.thrift.ThriftTestUtils;
import ezbake.thrift.ThriftUtils;

import static ezbake.data.graph.blueprints.util.Assert.assertValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VisibilityFilterElementTest {

    /**
     * View of a wrapped element from a caller that has formal authorizations
     * "U".
     */
    protected Element elementU;

    /**
     * View of a wrapped element from a caller that has formal authorizations
     * "U" and "S".
     */
    protected Element elementS;

    /**
     * Visibility with formal visibility "U"
     */
    protected Visibility visibilityU;

    /**
     * Visibility with formal visibility "S"
     */
    protected Visibility visibilityS;

    @Before
    public void setUp() throws TException {
        Graph graph = new TinkerGraph();
        Vertex vertex = graph.addVertex(0);

        elementU = new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U")).asVisibilityFilterVertex(vertex);
        elementS = new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U", "S")).asVisibilityFilterVertex(vertex);
        visibilityU = new Visibility();
        visibilityU.setFormalVisibility("U");
        visibilityS = new Visibility();
        visibilityS.setFormalVisibility("S");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetVisibilityRejectsNonString() {
        elementU.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetVisibilityRejectsInvalidEncoding() {
        elementU.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, "invalid");
    }

    @Test(expected = IllegalArgumentException.class)
    @Ignore // ThriftUtils.deserializeFromBase64 apparently doesn't validate classes so we can't check this
    public void testSetVisibilityRejectsNonVisibilityObject() throws TException {
        elementU.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY,
                ThriftUtils.serializeToBase64(new ValidityCaveats("", "", 0, "")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testVisibilityCannotBeRemoved() throws TException {
        elementU.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY,
                ThriftUtils.serializeToBase64(visibilityU));
        elementU.removeProperty(ElementFilter.VISIBILITY_PROPERTY_KEY);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPropertyRequiresListOfMaps() {
        elementU.setProperty("color", "green");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPropertyRejectsExtraFields() throws TException {
        Map<String, Object> value = new HashMap<>(3);

        value.put(PropertyFilter.VALUE_KEY, "green");
        value.put(PropertyFilter.VISIBILITY_KEY, ThriftUtils.serializeToBase64(visibilityU));
        value.put("foo", "bar");

        elementU.setProperty("color", Collections.singletonList(value));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPropertyRejectsMissingValueField() throws TException {
        Map<String, Object> value = new HashMap<>(3);

        value.put(PropertyFilter.VISIBILITY_KEY, ThriftUtils.serializeToBase64(visibilityU));

        elementU.setProperty("color", Collections.singletonList(value));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPropertyRejectsMissingVisibilityField() {
        Map<String, Object> value = new HashMap<>(3);

        value.put(PropertyFilter.VALUE_KEY, "green");

        elementU.setProperty("color", Collections.singletonList(value));
    }

    /**
     * Property values are appended to a list of property values.
     */
    @Test
    public void testSetPropertyAppendsValues() {
        elementU.setProperty("color", Collections.singletonList(new PropertyValueMap("green", visibilityU)));
        elementU.setProperty("color", Collections.singletonList(new PropertyValueMap("verde", visibilityU)));

        List<Map<String, Object>> values = elementU.getProperty("color");
        assertValues(Arrays.asList("green", "verde"), values);
    }

    /**
     * If the field delete appears in a property value map, the corresponding
     * property value should be removed.
     */
    @Test
    public void testSetPropertyCanDeleteValues() {
        elementU.setProperty("color", Collections.singletonList(new PropertyValueMap("green", visibilityU)));
        elementU.setProperty("color", Collections.singletonList(new PropertyValueMap("verde", visibilityU)));

        List<Map<String, Object>> values = elementU.getProperty("color");
        assertValues(Arrays.asList("green", "verde"), values);

        elementU.setProperty("color", Collections.singletonList(new PropertyValueMap("verde", visibilityU, true)));

        values = elementU.getProperty("color");
        assertValues(Arrays.asList("green"), values);
    }

    /**
     * If the field delete appears in a property value map that is not visible
     * to the context, it should not be removed.
     */
    @Test
    public void testSetPropertyChecksVisibilityBeforeDeletingValues() {
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("green", visibilityU)));
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("red", visibilityS)));

        // Nothing happens because U can't see the value
        elementU.setProperty("color", Collections.singletonList(new PropertyValueMap("red", visibilityS, true)));

        List<Map<String, Object>> values;
        values = elementS.getProperty("color");
        assertValues(Arrays.asList("green", "red"), values);

        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("red", visibilityS, true)));
        values = elementS.getProperty("color");
        assertValues(Arrays.asList("green"), values);
    }

    /**
     * Property values must be readable to be returned by getProperty().
     */
    @Test
    public void testGetPropertyFiltersProperties() {
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("green", visibilityU)));
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("red", visibilityS)));

        List<Map<String, Object>> values;

        values = elementU.getProperty("color");
        assertValues(Arrays.asList("green"), values);

        values = elementS.getProperty("color");
        assertValues(Arrays.asList("green", "red"), values);
    }

    /**
     * If not values for a property are visible, getProperty() returns null
     * rather than an empty list. This is the same behavior as if the property
     * had never been set.
     */
    @Test
    public void testGetPropertyRemovesEmptyValues() {
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("red", visibilityS)));

        List<Map<String, Object>> values = elementU.getProperty("color");
        assertNull(values);

        values = elementS.getProperty("color");
        assertValues(Arrays.asList("red"), values);
    }

    /**
     * If no values for a property are visible, getPropertyKeys() does not
     * return the property key. This means that if a key appears in the result
     * of getPropertyKeys(), then getProperty() on that key will be non-null.
     */
    @Test
    public void testGetPropertyKeysRemovesEmptyValues() throws TException {
        elementS.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visibilityU));
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("red", visibilityS)));

        Set<String> keys;

        keys = elementU.getPropertyKeys();
        assertEquals(new HashSet<>(Arrays.asList(ElementFilter.VISIBILITY_PROPERTY_KEY)), keys);

        keys = elementS.getPropertyKeys();
        assertEquals(new HashSet<>(Arrays.asList(ElementFilter.VISIBILITY_PROPERTY_KEY, "color")), keys);
    }

    @Test
    public void testRemovePropertyFiltersProperties() {
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("green", visibilityU)));
        elementS.setProperty("color", Collections.singletonList(new PropertyValueMap("red", visibilityS)));

        List<Map<String, Object>> values;

        values = elementU.getProperty("color");
        assertValues(Arrays.asList("green"), values);

        values = elementU.removeProperty("color");
        assertValues(Arrays.asList("green"), values);
        assertNull(elementU.getProperty("color"));

        values = elementS.getProperty("color");
        assertValues(Arrays.asList("red"), values);
    }

    @Test
    public void testElementVisibility() throws TException {
        elementS.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visibilityS));
        assertTrue(((VisibilityFilterElement) elementS).hasAnyPermission(Permission.READ));
        assertFalse(((VisibilityFilterElement) elementU).hasAnyPermission(Permission.READ));
    }
}
