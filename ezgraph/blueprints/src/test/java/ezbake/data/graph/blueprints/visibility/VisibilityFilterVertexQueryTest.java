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

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.Permission;
import ezbake.security.permissions.PermissionUtils;

import static ezbake.data.graph.blueprints.util.Assert.assertElementIds;

public class VisibilityFilterVertexQueryTest {

    private MockPermissionContext ctx;
    private Graph graph;

    @Before
    public void setUp() {
        ctx = new MockPermissionContext();
        graph = new VisibilityFilterGraph(new TinkerGraph(), ctx);
    }

    @Test
    public void testVerticesChecksEdgePermissions() {
        Vertex t = graph.addVertex("t");
        Vertex u = graph.addVertex("u");
        Vertex v = graph.addVertex("v");
        Edge vt = graph.addEdge("vt", v, t, "L");
        Edge vu = graph.addEdge("vu", v, u, "L");

        ctx.setDefaultElementPermissions(PermissionUtils.ALL_PERMS);
        ctx.setElementPermissions(vt, Permission.READ);
        ctx.setElementPermissions(vu, Permission.DISCOVER);

        VertexQuery q = v.query();
        assertElementIds(Collections.singleton("t"), q.vertices());
    }

    @Test
    public void testVerticesChecksVertexPermissions() {
        Vertex s = graph.addVertex("s");
        Vertex t = graph.addVertex("t");
        Vertex u = graph.addVertex("u");
        Vertex v = graph.addVertex("v");
        graph.addEdge("vs", v, s, "L");
        graph.addEdge("vt", v, t, "L");
        graph.addEdge("vu", v, u, "L");

        ctx.setDefaultElementPermissions(PermissionUtils.ALL_PERMS);
        ctx.setElementPermissions(s, Permission.DISCOVER);
        ctx.setElementPermissions(t, Permission.READ);
        ctx.setElementPermissions(u, PermissionUtils.NO_PERMS);
        VertexQuery q = v.query();
        assertElementIds(Sets.newHashSet("s", "t"), q.vertices());
    }
}
