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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static junit.framework.TestCase.assertFalse;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Features;

import ezbake.data.graph.blueprints.stub.EdgeStub;
import ezbake.data.graph.blueprints.stub.GraphManagerStub;
import ezbake.data.graph.blueprints.stub.GraphStub;
import ezbake.data.graph.blueprints.stub.VertexStub;

/**
 * Tests methods of {@link GraphManagerGraphFilterGraph}.
 */
public class GraphManagerGraphFilterGraphTest {

    private GraphStub graphStub;
    private GraphManagerStub managerStub;
    private GraphManagerGraphFilterGraph graph;

    @Before
    public void setUp() {
        graphStub = new GraphStub();
        managerStub = new GraphManagerStub();

        graph = new GraphManagerGraphFilterGraph(graphStub, managerStub);
    }

    @Test
    public void testGetFeatures() {
        Features features = graph.getFeatures();
        assertNotSame(graphStub.features, features);

        assertTrue(features.isWrapper);
        assertTrue(graphStub.getFeaturesCalled);
    }

    @Test
    public void testGetVertex() {
        final String id = "id";
        graph.getVertex(id);
        assertTrue(graphStub.getVertexCalled);
        assertSame(id, graphStub.getVertexId);
    }

    @Test
    public void testAddVertex() {
        final String id = "id";
        graph.addVertex(id);
        assertTrue(managerStub.addGraphCalled);
        assertEquals(id, managerStub.addGraphGraphName);

        assertTrue(graphStub.addVertexCalled);
        assertSame(id, graphStub.addVertexId);
    }

    @Test
    public void testAddVertexExceptionFromManager() {

        try {
            graph.addVertex(GraphManagerStub.THROW_EXCEPTION);
            fail();
        } catch (GraphManagementException e) {
            //expected exception
        }

        // If a vertex cannot be added to the graph manager, add vertex should NOT get called.
        assertFalse(graphStub.addVertexCalled);
        assertNull(graphStub.addVertexId);
    }

    @Test
    public void testRemoveVertex() {
        graph.removeVertex(graphStub.getVertex(GraphStub.STUB_FOR_VERIFICATION));

        assertTrue(managerStub.removeGraphCalled);
        assertSame(VertexStub.VERTEX_STUB_ID, managerStub.removeGraphGraphName);

        assertTrue(((VertexStub) graphStub.getVertex(GraphStub.STUB_FOR_VERIFICATION)).removeCalled);
    }

    @Test
    public void testRemoveVertexExceptionFromManager() {
        try {
            graph.removeVertex(
                    new VertexStub() {
                        @Override
                        public Object getId() {
                            return GraphManagerStub.THROW_EXCEPTION;
                        }
                    });
            fail();
        } catch (GraphManagementException e) {
            //expected exception
        }

        assertFalse(graphStub.removeVertexCalled);
        assertNull(graphStub.removeVertexVertex);
    }

    @Test
    public void testGetVertices() {
        assertSame(graph.getVertices().iterator().next().getClass(), GraphManagerGraphFilterVertex.class);
        assertTrue(graphStub.getVerticesCalled);
    }

    @Test
    public void testGetVerticesWithArgs() {
        final String key = "key";
        final String value = "value";

        graph.getVertices(key, value);
        assertTrue(graphStub.getVerticesArgsCalled);
        assertSame(key, graphStub.getVerticesArgsKey);
        assertSame(value, graphStub.getVerticesArgsValue);
    }

    @Test
    public void testGetEdge() {
        // the GraphManagerGraph currently does not support edges
        // a null value is returned to support some Vertex operations
        assertEquals(null, graph.getEdge("id"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddEdge() {
        graph.addEdge("id", new VertexStub(), new VertexStub(), "label");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveEdge() {
        graph.removeEdge(new EdgeStub());
    }

    @Test
    public void testGetEdges() {
        // the GraphManagerGraph currently does not support edges
        // an empty array list is returned to support some Vertex operations.
        assertEquals(Collections.emptyList(), graph.getEdges());
    }

    @Test
    public void testGetEdgesWithArgs() {
        // the GraphManagerGraph currently does not support edges
        // an empty array list is returned to support some Vertex operations.
        assertEquals(Collections.emptyList(), graph.getEdges("Key", "Value"));
    }

    @Test
    public void testQuery() {
        assertEquals(GraphManagerGraphFilterGraphQuery.class, graph.query().getClass());
    }

    @Test
    public void testShutdown() {
        graph.shutdown();
        assertTrue(graphStub.shutdownCalled);
    }
}
