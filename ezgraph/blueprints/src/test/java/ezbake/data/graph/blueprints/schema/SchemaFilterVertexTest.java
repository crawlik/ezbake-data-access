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

import com.google.common.collect.Lists;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import ezbake.data.graph.blueprints.stub.PropertySchemaManagerStub;
import ezbake.data.graph.blueprints.stub.VertexStub;

/**
 * Tests for {@link SchemaFilterVertex} which wraps a {@link com.tinkerpop.blueprints.Vertex} and provides compatibility
 * with other schema filter classes.
 */
public class SchemaFilterVertexTest {

    /**
     * Wrapped {@link com.tinkerpop.blueprints.Vertex} with an implementation that assists in verifying input.
     */
    private VertexStub vertexStub;

    /**
     * System under test which wraps {@code vertexStub}.
     */
    private SchemaFilterVertex schemaFilterVertex;

    /**
     * Initialize {@code vertexStub} and {@code schemaFilterVertex}.
     */
    @Before
    public void setUp() {
        vertexStub = new VertexStub();
        schemaFilterVertex = new SchemaFilterVertex(
                vertexStub, new DefaultSchemaContext(new PropertySchemaManagerStub()));
    }

    /**
     * Delegates to wrapped {@link com.tinkerpop.blueprints.Vertex} and converts each {@link
     * com.tinkerpop.blueprints.Edge} in the result to {@link SchemaFilterEdge}.
     */
    @Test
    public void testGetEdges() {
        final String[] labels = {"a", "b", "c"};
        final Direction getEdgesDirection = Direction.OUT;
        assertSame(
                SchemaFilterEdge.class,
                schemaFilterVertex.getEdges(getEdgesDirection, labels).iterator().next().getClass());
        assertTrue(vertexStub.getEdgesCalled);
        assertEquals(getEdgesDirection, vertexStub.getEdgesDirection);
        assertEquals(Lists.newArrayList(labels), Lists.newArrayList(vertexStub.getEdgesLabels));
    }

    /**
     * Delegates to wrapped {@link com.tinkerpop.blueprints.Vertex} and converts each Vertex in the result to {@link
     * SchemaFilterVertex}.
     */
    @Test
    public void testGetVertices() {
        final String[] labels = {"a", "b", "c"};
        final Direction getVerticesDirection = Direction.OUT;
        assertSame(
                SchemaFilterVertex.class,
                schemaFilterVertex.getVertices(getVerticesDirection, labels).iterator().next().getClass());

        assertTrue(vertexStub.getVerticesCalled);
        assertEquals(getVerticesDirection, vertexStub.getVerticesDirection);
        assertEquals(Lists.newArrayList(labels), Lists.newArrayList(vertexStub.getVerticesLabels));
    }

    /**
     * Delegates to wrapped {@link com.tinkerpop.blueprints.Vertex} and returns the resulting {@link
     * com.tinkerpop.blueprints.Edge} as a {@link SchemaFilterEdge}.
     */
    @Test
    public void testAddEdge() {
        final String label = "aLabel";
        final Vertex vertex = new VertexStub();
        assertSame(SchemaFilterEdge.class, schemaFilterVertex.addEdge(label, vertex).getClass());
        assertTrue(vertexStub.addEdgeCalled);
        assertEquals(label, vertexStub.addEdgeLabel);
        assertEquals(vertex, vertexStub.addEdgeVertex);
    }

    /**
     * Returns the {@link com.tinkerpop.blueprints.Vertex} wrapped by a {@link SchemaFilterVertex}.
     */
    @Test
    public void testGetBaseVertex() {
        assertSame(vertexStub, schemaFilterVertex.getBaseVertex());
    }
}
