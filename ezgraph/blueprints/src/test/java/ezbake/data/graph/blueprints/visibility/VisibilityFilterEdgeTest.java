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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.Visibility;
import ezbake.thrift.ThriftTestUtils;
import ezbake.thrift.ThriftUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class VisibilityFilterEdgeTest {

    protected Graph graph;

    protected Visibility visibilityU;
    protected Visibility visibilityS;
    protected PermissionContext ctxU;
    protected PermissionContext ctxS;

    @Before
    public void setUp() {
        graph = new TinkerGraph();
        visibilityU = new Visibility();
        visibilityU.setFormalVisibility("U");
        visibilityS = new Visibility();
        visibilityS.setFormalVisibility("S");

        ctxU = new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U"));
        ctxS = new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U", "S"));
    }

    @Test
    public void testGetLabelRequiresRead() throws TException {
        Vertex v1 = graph.addVertex(1);
        Vertex v2 = graph.addVertex(2);

        Edge edge = v1.addEdge("label", v2);
        edge.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visibilityS));

        VisibilityFilterEdge eU = ctxU.asVisibilityFilterEdge(edge);
        Exception exception = null;
        try {
            eU.getLabel();
        } catch (IllegalArgumentException e) {
            exception = e;
        }
        assertNotNull(exception);

        VisibilityFilterEdge eS = ctxS.asVisibilityFilterEdge(edge);
        assertEquals("label", eS.getLabel());
    }

    @Test
    public void testGetVertexRequiresRead() throws TException {
        Vertex v1 = graph.addVertex(1);
        Vertex v2 = graph.addVertex(2);

        Edge edge = v1.addEdge("label", v2);
        edge.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visibilityS));

        VisibilityFilterEdge eU = ctxU.asVisibilityFilterEdge(edge);
        assertNull(eU.getVertex(Direction.OUT));

        VisibilityFilterEdge eS = ctxS.asVisibilityFilterEdge(edge);
        assertEquals("1", eS.getVertex(Direction.OUT).getId());
    }

    @Test
    public void testGetBaseEdge() {
        Vertex v = graph.addVertex(1);
        Edge e = graph.addEdge(1, v, v, "label");
        VisibilityFilterEdge f = ctxU.asVisibilityFilterEdge(e);

        assertSame(e, f.getBaseEdge());
    }
}
