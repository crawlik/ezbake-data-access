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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Direction;

import ezbake.data.graph.blueprints.stub.EdgeStub;
import ezbake.data.graph.blueprints.stub.PropertySchemaManagerStub;

/**
 * Test for {@link SchemaFilterEdge}.
 */
public class SchemaFilterEdgeTest {

    /**
     * System under test.
     */
    private SchemaFilterEdge schemaFilterEdge;

    /**
     * SchemaFilterEdge wraps an {@link com.tinkerpop.blueprints.Edge}. This is the Edge we provide that has helper
     * methods for validating input.
     */
    private EdgeStub edgeStub;

    /**
     * Initialize the {@link com.tinkerpop.blueprints.Edge} to be wrapped and give it to the {@link SchemaFilterEdge}.
     */
    @Before
    public void setUp() {
        edgeStub = new EdgeStub();
        schemaFilterEdge = new SchemaFilterEdge(
                edgeStub, new DefaultSchemaContext(new PropertySchemaManagerStub()));
    }

    /**
     * {@code getVertex(id)} needs only delegate to the wrapped vertex.
     */
    @Test
    public void testGetVertex() {
        assertSame(SchemaFilterVertex.class, schemaFilterEdge.getVertex(Direction.OUT).getClass());
        assertTrue(edgeStub.getVertexCalled);
        assertEquals(Direction.OUT, edgeStub.getVertexDirection);
    }

    /**
     * {@code getLabel()} needs only delegate to the wrapped vertex.
     */
    @Test
    public void testGetLabel() {
        schemaFilterEdge.getLabel();
        assertTrue(edgeStub.getLabelCalled);
    }

    /**
     * {@code getBaseEdge()} should return the same object that is wrapped.
     */
    @Test
    public void testGetBaseEdge() {
        assertSame(edgeStub, schemaFilterEdge.getBaseEdge());
    }
}
