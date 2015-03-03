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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Vertex;

import ezbake.data.graph.blueprints.stub.EdgeStub;
import ezbake.data.graph.blueprints.stub.GraphStub;
import ezbake.data.graph.blueprints.stub.PropertySchemaManagerStub;
import ezbake.data.graph.blueprints.stub.VertexStub;

/**
 * Tests {@link SchemaFilterGraph}.
 */
public class SchemaFilterGraphTest {

    /**
     * {@link SchemaFilterGraph} wraps a {@link com.tinkerpop.blueprints.Graph}. This GraphStub can be wrapped
     * and used for verification of input.
     */
    private GraphStub graphStub;

    /**
     * System under test.
     */
    private SchemaFilterGraph schemaFilterGraph;

    /**
     * Schema context that can be passed into the graph and elements, doesn't perform any behavior, just made a member
     * for brevity in tests.
     */
    private SchemaContext context;

    /**
     * Initialize {@code graphStub}, {@code context} and {@code schemaFilterGraph}.
     */
    @Before
    public void setUp() {
        graphStub = new GraphStub();
        context = new DefaultSchemaContext(new PropertySchemaManagerStub());
        schemaFilterGraph = new SchemaFilterGraph(
                graphStub, context);
    }

    /**
     * {@code getFeatures()} delegates to the wrapped {@link com.tinkerpop.blueprints.Graph}, but also sets {@code
     * isWrapper} to true.
     */
    @Test
    public void testGetFeatures() {
        final Features features = schemaFilterGraph.getFeatures();
        assertTrue(features.isWrapper);

        assertTrue(graphStub.getFeaturesCalled);
        assertNotSame(graphStub.features, features);
    }

    /**
     * {@code addVertex(id)} delegates to the {@link com.tinkerpop.blueprints.Graph}  and converts the returned result
     * into a {@link SchemaFilterVertex}.
     */
    @Test
    public void testAddVertex() {
        final Object id = new Object();
        assertSame(schemaFilterGraph.addVertex(id).getClass(), SchemaFilterVertex.class);
        assertTrue(graphStub.addVertexCalled);
        assertEquals(id, graphStub.addVertexId);
    }

    /**
     * {@code getVertex(id)} delegates to the wrapped {@link com.tinkerpop.blueprints.Graph} and converts the returned
     * result into a {@link SchemaFilterVertex}.
     */
    @Test
    public void testGetVertex() {
        final Object id = new Object();
        final Vertex vertex = schemaFilterGraph.getVertex(id);
        assertSame(vertex.getClass(), SchemaFilterVertex.class);
        assertTrue(graphStub.getVertexCalled);
        assertEquals(id, graphStub.getVertexId);
    }

    /**
     * {@code removeVertex(id)} delegates to the wrapped {@link com.tinkerpop.blueprints.Graph}.
     */
    @Test
    public void testRemoveVertex() {
        final Vertex vertex = new VertexStub();
        schemaFilterGraph.removeVertex(vertex);
        assertTrue(graphStub.removeVertexCalled);
        assertEquals(vertex, graphStub.removeVertexVertex);
    }

    /**
     * {@code getVertices()} delegates to the wrapped {@link com.tinkerpop.blueprints.Graph} and converts the returned
     * result into an Iterable of {@link SchemaFilterVertex}.
     */
    @Test
    public void testGetVerticesNoArgs() {
        assertSame(schemaFilterGraph.getVertices().iterator().next().getClass(), SchemaFilterVertex.class);
        assertTrue(graphStub.getVerticesCalled);
    }

    /**
     * {@code getVertices(...)} delegates to the wrapped {@link com.tinkerpop.blueprints.Graph} and converts the
     * returned result into an Iterable of {@link SchemaFilterVertex}.
     */
    @Test
    public void testGetVerticesArgs() {
        final String key = "key";
        final Object value = "value";
        assertSame(schemaFilterGraph.getVertices(key, value).iterator().next().getClass(), SchemaFilterVertex.class);
        assertTrue(graphStub.getVerticesArgsCalled);
        assertEquals(key, graphStub.getVerticesArgsKey);
        assertEquals(value, graphStub.getVerticesArgsValue);
    }

    /**
     * {@code addEdge(...)} takes in two different {@link com.tinkerpop.blueprints.Vertex} objects. This tests with {@link
     * SchemaFilterVertex} type Vertex given for those params. This should pass the unwrapped vertices to the wrapped {@link
     * com.tinkerpop.blueprints.Graph}'s {@code addEdge(...)} method and return the result wrapped in a {@link
     * SchemaFilterEdge}.
     */
    @Test
    public void testAddEdgeFilterVertices() {
        final Object id = new Object();
        final Vertex wrapped = new VertexStub();
        final Vertex outVertex = new SchemaFilterVertex(wrapped, context);
        final Vertex inVertex = new SchemaFilterVertex(wrapped, context);
        final String label = "label";
        assertSame(schemaFilterGraph.addEdge(id, outVertex, inVertex, label).getClass(), SchemaFilterEdge.class);
        assertTrue(graphStub.addEdgeCalled);
        assertEquals(id, graphStub.addEdgeId);
        assertSame(wrapped, graphStub.addEdgeOutVertex);
        assertSame(wrapped, graphStub.addEdgeInVertex);
        assertEquals(label, graphStub.addEdgeLabel);
    }

    /**
     * Tests {@code addEdge(...)} with already unwrapped {@link com.tinkerpop.blueprints.Vertex}.  Delegates to wrapped
     * {@link com.tinkerpop.blueprints.Graph}'s method and returns the result wrapped in a {@link SchemaFilterEdge}.
     */
    @Test
    public void testAddEdgeRawVertices() {
        final Object id = new Object();
        final Vertex outVertex = new VertexStub();
        final Vertex inVertex = new VertexStub();
        final String label = "label";
        assertSame(schemaFilterGraph.addEdge(id, outVertex, inVertex, label).getClass(), SchemaFilterEdge.class);
        assertTrue(graphStub.addEdgeCalled);
        assertEquals(id, graphStub.addEdgeId);
        assertSame(outVertex, graphStub.addEdgeOutVertex);
        assertSame(inVertex, graphStub.addEdgeInVertex);
        assertEquals(label, graphStub.addEdgeLabel);
    }

    /**
     * Delegates to wrapped {@link com.tinkerpop.blueprints.Graph}'s method and returns the result wrapped in a {@link
     * SchemaFilterEdge}.
     */
    @Test
    public void testGetEdge() {
        final Object id = new Object();
        assertSame(schemaFilterGraph.getEdge(id).getClass(), SchemaFilterEdge.class);
        assertTrue(graphStub.getEdgeCalled);
        assertEquals(id, graphStub.getEdgeId);
    }

    /**
     * {@code removeEdge(...)} with a {@link SchemaFilterEdge} passed in. Delegates to wrapped {@link
     * com.tinkerpop.blueprints.Graph} by passing the unwrapped {@link com.tinkerpop.blueprints.Edge}.
     */
    @Test
    public void testRemoveEdgeWrappedEdge() {
        final Edge wrapped = new EdgeStub();
        final SchemaFilterEdge edge = new SchemaFilterEdge(wrapped, context);

        schemaFilterGraph.removeEdge(edge);
        assertTrue(graphStub.removeEdgeCalled);
        assertSame(wrapped, graphStub.removeEdgeEdge);
    }

    /**
     * {@code removeEdge(...)} with an already unwrapped {@link com.tinkerpop.blueprints.Edge} passed in. Delegates to
     * wrapped {@link com.tinkerpop.blueprints.Graph}.
     */
    @Test
    public void testRemoveEdgeRawEdge() {
        final Edge edge = new EdgeStub();

        schemaFilterGraph.removeEdge(edge);
        assertTrue(graphStub.removeEdgeCalled);
        assertEquals(edge, graphStub.removeEdgeEdge);
    }

    /**
     * Delegates to wrapped {@link com.tinkerpop.blueprints.Graph} and returns the resulting {@link
     * com.tinkerpop.blueprints.Edge}s as {@link SchemaFilterEdge}.
     */
    @Test
    public void testGetEdges() {
        assertSame(schemaFilterGraph.getEdges().iterator().next().getClass(), SchemaFilterEdge.class);
        assertTrue(graphStub.getEdgesCalled);
    }

    /**
     * Delegates to wrapped {@link com.tinkerpop.blueprints.Graph} and returns the resulting {@link
     * com.tinkerpop.blueprints.Edge}s as {@link SchemaFilterEdge}.
     */
    @Test
    public void testGetEdgesWithArgs() {
        final String key = "key";
        final Object value = "value";
        assertSame(schemaFilterGraph.getEdges(key, value).iterator().next().getClass(), SchemaFilterEdge.class);
        assertTrue(graphStub.getEdgesArgsCalled);
        assertEquals(key, graphStub.getEdgesArgsKey);
        assertEquals(value, graphStub.getEdgesArgsValue);
    }

    /**
     * Delegates to wrapped {@link com.tinkerpop.blueprints.Graph}.
     */
    @Test
    public void testShutdown() {
        schemaFilterGraph.shutdown();
        assertTrue(graphStub.shutdownCalled);
    }

    /**
     * Returns the {@link com.tinkerpop.blueprints.Graph} wrapped by the {@link SchemaFilterGraph}.
     */
    @Test
    public void testGetBaseGraph() {
        assertEquals(graphStub, schemaFilterGraph.getBaseGraph());
    }
}
