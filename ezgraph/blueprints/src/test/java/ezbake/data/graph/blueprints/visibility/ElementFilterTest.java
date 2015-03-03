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
import java.util.List;

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.apache.thrift.TException;
import org.junit.Test;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.thrift.ThriftTestUtils;
import ezbake.thrift.ThriftUtils;

import static ezbake.data.graph.blueprints.util.Assert.assertEdgeLabels;
import static ezbake.data.graph.blueprints.util.Assert.assertElementIds;

public class ElementFilterTest {

    @Test
    public void testFilterVertices() throws TException {
        Graph g = new TinkerGraph();

        Vertex vU = g.addVertex("u");
        Visibility visU = new Visibility();
        visU.setFormalVisibility("U");
        vU.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));

        Vertex vS = g.addVertex("s");
        Visibility visS = new Visibility();
        visS.setFormalVisibility("S");
        vS.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        List<Vertex> elements = Arrays.asList(vU, vS);

        ElementFilter filter = new ElementFilter(new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U")));
        assertElementIds(Sets.newHashSet("u"), filter.filterVertices(elements, Permission.READ, Permission.DISCOVER));
    }

    @Test
    public void testFilterEdges() throws TException {
        Graph g = new TinkerGraph();

        Vertex v1 = g.addVertex(1);
        Vertex v2 = g.addVertex(2);

        Edge eU = v1.addEdge("u", v2);
        Visibility visU = new Visibility();
        visU.setFormalVisibility("U");
        eU.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visU));

        Edge eS = v1.addEdge("s", v2);
        Visibility visS = new Visibility();
        visS.setFormalVisibility("S");
        eS.setProperty(ElementFilter.VISIBILITY_PROPERTY_KEY, ThriftUtils.serializeToBase64(visS));

        List<Edge> elements = Arrays.asList(eU, eS);

        ElementFilter filter = new ElementFilter(new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("U")));
        assertEdgeLabels(Sets.newHashSet("u"), filter.filterEdges(elements, Permission.READ, Permission.DISCOVER));
    }
}
