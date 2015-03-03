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

package ezbake.data.graph.blueprints.graphmgmt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Direction;

import ezbake.data.graph.blueprints.stub.GraphManagerStub;
import ezbake.data.graph.blueprints.stub.VertexStub;
import ezbake.data.graph.blueprints.visibility.ElementFilter;

/**
 * Tests methods on {@link GraphManagerGraphFilterVertex}.
 */
public class GraphManagerGraphFilterVertexTest {

    private VertexStub vertexStub;
    private GraphManagerStub managerStub;

    /**
     * System under test.
     */
    private GraphManagerGraphFilterVertex vertex;

    @Before
    public void setUp() {
        vertexStub = new VertexStub();
        managerStub = new GraphManagerStub();
        vertex = new GraphManagerGraphFilterVertex(vertexStub, managerStub);
    }

    @Test
    public void testGetEdges() {
       assertEquals(Collections.emptyList(), vertex.getEdges(null, null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetVertices() {
        vertex.getVertices(Direction.IN, "aLabel");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQuery() {
        vertex.query();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddEdge() {
        vertex.addEdge("aLabel", new VertexStub());
    }

    @Test
    public void testGetProperty() {
        final String propertyKey = "aPropertyKey";
        vertex.getProperty(propertyKey);
        assertTrue(vertexStub.getPropertyCalled);
        assertSame(propertyKey, vertexStub.getPropertyKey);
    }

    @Test
    public void testGetPropertyKeys() {
        final Set<String> keys = new HashSet<>();
        vertexStub.propertyKeysToReturn = keys;
        assertSame(keys, vertex.getPropertyKeys());
        assertTrue(vertexStub.getPropertyKeysCalled);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetPropertyNotEzVisibility() {
        vertex.setProperty("notVisProp", "ABCDEFG");
    }

    @Test
    public void testSetPropertyEzVisibility() {
        final String visibility = "ABCDEFG";
        vertex.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, visibility);
        assertTrue(vertexStub.setPropertyCalled);
        assertSame(ElementFilter.VISIBILITY_PROPERTY_KEY, vertexStub.setPropertyKey);
        assertSame(visibility, vertexStub.setPropertyValue);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveProperty() {
        vertex.removeProperty("aPropertyKey");
    }

    @Test
    public void remove() {
        vertex.remove();
        assertTrue(vertexStub.removeCalled);
        assertTrue(managerStub.removeGraphCalled);
        assertSame(VertexStub.VERTEX_STUB_ID, managerStub.removeGraphGraphName);
    }

    @Test
    public void testGetId() {
        assertSame(VertexStub.VERTEX_STUB_ID, vertex.getId());
        assertTrue(vertexStub.getIdCalled);
    }
}
