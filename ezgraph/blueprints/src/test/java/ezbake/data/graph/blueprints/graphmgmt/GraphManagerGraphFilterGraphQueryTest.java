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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import ezbake.data.graph.blueprints.stub.GraphManagerStub;
import ezbake.data.graph.blueprints.stub.GraphQueryStub;
import ezbake.data.graph.blueprints.stub.GraphStub;

/**
 * Tests {@link GraphManagerGraphFilterGraphQuery}.
 */
public class GraphManagerGraphFilterGraphQueryTest {

    /**
     * Wrapped query.
     */
    private GraphQueryStub stub;

    /**
     * System under test.
     */
    private GraphManagerGraphFilterGraphQuery query;

    @Before
    public void setUp() {
        stub = new GraphQueryStub();
        query = new GraphManagerGraphFilterGraphQuery(
                stub, new GraphManagerGraphFilterGraph(new GraphStub(), new GraphManagerStub()));
    }

    @Test
    public void testVertices() {
        //verify that vertices that come out are wrapped.
        assertEquals(GraphManagerGraphFilterVertex.class, query.vertices().iterator().next().getClass());
        assertTrue(stub.verticesCalled);
    }

    @Test
    public void testEdges() {
        //We return an empty list from edges() to support certain Vertex operations.
        assertEquals(new ArrayList(), query.edges());
        assertFalse(stub.edgesCalled);
    }
}
