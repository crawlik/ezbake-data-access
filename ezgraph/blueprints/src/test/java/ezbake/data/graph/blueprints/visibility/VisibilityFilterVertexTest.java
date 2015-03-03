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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
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

import static ezbake.data.graph.blueprints.util.Assert.assertEdgeLabels;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VisibilityFilterVertexTest {

    protected Graph g;

    protected Visibility visU;
    protected Visibility visS;
    protected PermissionContext ctxU;
    protected PermissionContext ctxS;

    @Before
    public void setUp() {
        g = new TinkerGraph();

        visU = new Visibility();
        visU.setFormalVisibility("U");
        visS = new Visibility();
        visS.setFormalVisibility("S");

        ctxU = new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U"));
        ctxS = new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U", "S"));
    }

    @Test
    public void testGetEdgesRequiresRead() throws TException {
        Vertex v = g.addVertex(1);
        v.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        VisibilityFilterVertex vU = ctxU.asVisibilityFilterVertex(v);
        assertEquals(0, Iterables.size(vU.getEdges(Direction.OUT, "")));

        VisibilityFilterVertex vS = ctxS.asVisibilityFilterVertex(v);
        assertNotNull(vS.getEdges(Direction.OUT, ""));
    }

    @Test
    public void testGetEdgesFiltersEdges() throws TException {
        Vertex v1 = g.addVertex(1);
        v1.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));
        Vertex v2 = g.addVertex(2);
        v2.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));
        Vertex v3 = g.addVertex(3);
        v3.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        Edge e1 = v1.addEdge("L1", v2);
        e1.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));

        // Won't be seen by U since the edge isn't readable
        Edge e2 = v1.addEdge("L2", v3);
        e2.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        VisibilityFilterVertex vU = ctxU.asVisibilityFilterVertex(v1);
        assertEdgeLabels(Sets.newHashSet("L1"), vU.getEdges(Direction.OUT, "L1", "L2"));

        VisibilityFilterVertex vS = ctxS.asVisibilityFilterVertex(v1);
        assertEdgeLabels(Sets.newHashSet("L1", "L2"), vS.getEdges(Direction.OUT, "L1", "L2"));
    }

    @Test
    public void testGetVerticesRequiresRead() throws TException {
        Vertex v = g.addVertex(1);
        v.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        VisibilityFilterVertex vU = ctxU.asVisibilityFilterVertex(v);
        assertEquals(0, Iterables.size(vU.getVertices(Direction.OUT, "")));

        VisibilityFilterVertex vS = ctxS.asVisibilityFilterVertex(v);
        assertNotNull(vS.getVertices(Direction.OUT, ""));
    }

    @Test
    public void testGetVerticesFiltersVertices() throws TException {
        Vertex v1 = g.addVertex(1);
        v1.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));
        Vertex v2 = g.addVertex(2);
        v2.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));
        Vertex v3 = g.addVertex(3);
        v3.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        Edge e1 = v1.addEdge("label", v1);
        e1.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));

        // Won't be seen by U since the edge isn't readable
        Edge e2 = v1.addEdge("label", v2);
        e2.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        // Won't be seen by U since other vertex isn't readable
        Edge e3 = v1.addEdge("label", v3);
        e3.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));

        VisibilityFilterVertex vU = ctxU.asVisibilityFilterVertex(v1);
        Set<String> idU = new HashSet<>();
        for (Vertex v : vU.getVertices(Direction.OUT, "label")) {
            idU.add(v.getId().toString());
        }
        assertEquals(Collections.singleton("1"), idU);

        VisibilityFilterVertex vS = ctxS.asVisibilityFilterVertex(v1);
        Set<String> idS = new HashSet<>();
        for (Vertex v : vS.getVertices(Direction.OUT, "label")) {
            idS.add(v.getId().toString());
        }
        assertEquals(Sets.newHashSet("1", "2", "3"), idS);
    }
}
