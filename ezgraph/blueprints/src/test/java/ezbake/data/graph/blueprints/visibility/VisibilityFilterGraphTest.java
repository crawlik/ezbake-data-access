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
import java.util.List;

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.security.permissions.PermissionUtils;

import static ezbake.data.graph.blueprints.util.Assert.assertElementIds;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class VisibilityFilterGraphTest {

    private MockPermissionContext ctx;
    private Visibility vis;
    private List<PropertyValueMap> prop;
    private Graph g;
    private Graph f;

    @Before
    public void setUp() {
        ctx = new MockPermissionContext();
        vis = new Visibility();
        prop = Collections.singletonList(new PropertyValueMap("", vis));
        g = new TinkerGraph();
        f = new VisibilityFilterGraph(g, ctx);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddVertex() {
        Vertex v = g.addVertex("v");
        assertSame(v, g.getVertex("v"));

        // Even though we can't see the vertex, we get an exception if we try to add one with the same id.
        ctx.setElementPermissions(v, PermissionUtils.NO_PERMS);
        assertNull(f.getVertex("v"));

        f.addVertex("v");
    }

    @Test
    public void testGetVertex() {
        Vertex v = g.addVertex(0);
        assertSame(v, g.getVertex(0));

        ctx.setElementPermissions(v, PermissionUtils.NO_PERMS);
        assertNull(f.getVertex(0));

        ctx.setElementPermissions(v, Permission.DISCOVER);
        assertNotNull(f.getVertex(0));

        ctx.setElementPermissions(v, Permission.READ);
        assertNotNull(f.getVertex(0));
    }

    @Test
    public void testRemoveVertex() {
        Vertex v = g.addVertex("v");
        v.setProperty("foo", prop);
        Edge e = g.addEdge("e", v, v, "label");
        e.setProperty("foo", prop);

        // Note that we don't need to be able to read the elements to be able to write them.
        ctx.setElementPermissions(v, Permission.WRITE);
        ctx.setElementPermissions(e, Permission.WRITE);
        ctx.setPermissions(vis, Permission.WRITE);

        f.removeVertex(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveVertexRequiresElementPermission() {
        // Fails because no write permission on element
        Vertex v = g.addVertex("v");

        ctx.setElementPermissions(v, PermissionUtils.NO_PERMS);

        f.removeVertex(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveVertexRequiresPropertyPermission() {
        // Fails because no write permission on a property
        Vertex v = g.addVertex("v");
        v.setProperty("foo", prop);

        ctx.setElementPermissions(v, Permission.WRITE);
        ctx.setPermissions(vis, PermissionUtils.NO_PERMS);

        f.removeVertex(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveVertexRequiresEdgeElementPermission() {
        // Fails because no write permission on edge
        Vertex v = g.addVertex("v");
        Edge e = g.addEdge("e", v, v, "label");

        ctx.setElementPermissions(v, Permission.WRITE);
        ctx.setElementPermissions(e, Permission.READ);

        f.removeVertex(v);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveVertexRequiresEdgePropertyPermission() {
        // Fails because no write permission on edge property
        Vertex v = g.addVertex("v");
        Edge e = g.addEdge("e", v, v, "label");
        e.setProperty("foo", prop);

        ctx.setElementPermissions(v, Permission.WRITE);
        ctx.setElementPermissions(e, Permission.WRITE);
        ctx.setPermissions(vis, PermissionUtils.NO_PERMS);

        f.removeVertex(v);
    }

    @Test
    public void testGetVertices() {
        Vertex v0 = g.addVertex("0");
        Vertex v1 = g.addVertex("1");
        Vertex v2 = g.addVertex("2");
        assertElementIds(Sets.newHashSet("0", "1", "2"), g.getVertices());

        ctx.setElementPermissions(v0, PermissionUtils.NO_PERMS);
        ctx.setElementPermissions(v1, Permission.DISCOVER);
        ctx.setElementPermissions(v2, Permission.READ);
        assertElementIds(Sets.newHashSet("1", "2"), f.getVertices());
    }

    @Test
    public void testGetVerticesWithPropertyRequiresElementPermission() {
        Vertex v0 = g.addVertex("0");
        Vertex v1 = g.addVertex("1");
        Vertex v2 = g.addVertex("2");

        Visibility vis0 = new Visibility();
        Visibility vis1 = new Visibility();
        Visibility vis2 = new Visibility();

        v0.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis0)));
        v1.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis1)));
        v2.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis2)));

        ctx.setElementPermissions(v0, PermissionUtils.NO_PERMS);
        ctx.setElementPermissions(v1, Permission.DISCOVER);
        ctx.setElementPermissions(v2, Permission.READ);
        ctx.setDefaultPermissions(Permission.READ);

        assertElementIds(Sets.newHashSet("1", "2"), f.getVertices("foo", "bar"));
    }

    @Test
    public void testGetVerticesWithPropertyRequiresPropertyPermission() {
        Vertex v0 = g.addVertex("0");
        Vertex v1 = g.addVertex("1");
        Vertex v2 = g.addVertex("2");
        Vertex v3 = g.addVertex("3");
        Vertex v4 = g.addVertex("4");

        Visibility vis0 = new Visibility();
        vis0.setFormalVisibility("0");
        Visibility vis1 = new Visibility();
        vis1.setFormalVisibility("1");
        Visibility vis2 = new Visibility();
        vis2.setFormalVisibility("2");
        Visibility vis3 = new Visibility();
        vis3.setFormalVisibility("3");

        v0.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis0)));
        v1.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis1)));
        v2.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis2)));
        v3.setProperty("foo", Collections.singletonList(new PropertyValueMap("quz", vis3)));
        v4.setProperty("quz", Collections.singletonList(new PropertyValueMap("bar", vis3)));

        ctx.setDefaultElementPermissions(Permission.READ);
        ctx.setPermissions(vis0, PermissionUtils.NO_PERMS);
        ctx.setPermissions(vis1, Permission.DISCOVER);
        ctx.setPermissions(vis2, Permission.READ);
        ctx.setPermissions(vis3, PermissionUtils.ALL_PERMS);

        // v0 is reject because we have no permissions to see property "foo" with value "bar". v3 is rejected because
        // it has the property key, but not the correct value. v4 is rejected because it doesn't have the key.
        assertElementIds(Sets.newHashSet("1", "2"), f.getVertices("foo", "bar"));
    }

    @Test
    public void testAddEdge() {
        Vertex v0 = f.addVertex(0);
        Vertex v1 = f.addVertex(1);
        Edge e = f.addEdge("e", v0, v1, "label");

        ctx.setElementPermissions(e, PermissionUtils.NO_PERMS);
        assertNull(f.getEdge("e"));

        Exception exception = null;
        try {
            f.addEdge("e", v0, v1, "label");
        } catch (IllegalArgumentException ex) {
            exception = ex;
        }

        assertNotNull(exception);
    }

    @Test
    public void testGetEdge() {
        Vertex v0 = g.addVertex(0);
        Vertex v1 = g.addVertex(1);
        Edge e = g.addEdge("e", v0, v1, "label");
        assertSame(e, g.getEdge("e"));

        ctx.setElementPermissions(e, PermissionUtils.NO_PERMS);
        assertNull(f.getEdge("e"));

        ctx.setElementPermissions(e, Permission.DISCOVER);
        assertNotNull(f.getEdge("e"));

        ctx.setElementPermissions(e, Permission.READ);
        assertNotNull(f.getEdge("e"));
    }

    @Test
    public void testRemoveEdge() {
        Vertex v = g.addVertex("v");
        Edge e = g.addEdge("e", v, v, "label");
        e.setProperty("foo", prop);

        ctx.setElementPermissions(e, Permission.WRITE);
        ctx.setPermissions(vis, Permission.WRITE);

        f.removeEdge(e);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveEdgeRequiresElementPermission() {
        Vertex v = g.addVertex("v");
        Edge e = g.addEdge("e", v, v, "label");

        ctx.setElementPermissions(e, PermissionUtils.NO_PERMS);

        f.removeEdge(e);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveEdgeRequiresPropertyPermission() {
        Vertex v = g.addVertex("v");
        Edge e = g.addEdge("e", v, v, "label");
        e.setProperty("foo", prop);

        ctx.setElementPermissions(e, Permission.WRITE);
        ctx.setPermissions(vis, PermissionUtils.NO_PERMS);

        f.removeEdge(e);
    }

    @Test
    public void testGetEdges() {
        Vertex v = f.addVertex(0);

        Edge e1 = f.addEdge("1", v, v, "label");
        Edge e2 = f.addEdge("2", v, v, "label");
        Edge e3 = f.addEdge("3", v, v, "label");

        ctx.setElementPermissions(e1, PermissionUtils.NO_PERMS);
        ctx.setElementPermissions(e2, Permission.DISCOVER);
        ctx.setElementPermissions(e3, Permission.READ);

        assertElementIds(Sets.newHashSet("2", "3"), f.getEdges());
    }

    @Test
    public void testGetEdgesWithPropertyRequiresElementPermission() {
        Vertex v = g.addVertex("v");

        Edge e0 = g.addEdge("0", v, v, "label");
        Edge e1 = g.addEdge("1", v, v, "label");
        Edge e2 = g.addEdge("2", v, v, "label");

        e0.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis)));
        e1.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis)));
        e2.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis)));

        ctx.setElementPermissions(v, PermissionUtils.NO_PERMS);
        ctx.setElementPermissions(e0, PermissionUtils.NO_PERMS);
        ctx.setElementPermissions(e1, Permission.DISCOVER);
        ctx.setElementPermissions(e2, Permission.READ);
        ctx.setDefaultPermissions(PermissionUtils.ALL_PERMS);

        assertElementIds(Sets.newHashSet("1", "2"), f.getEdges("foo", "bar"));
    }

    @Test
    public void testGetEdgesWithPropertyRequiresPropertyPermission() {
        Vertex v = g.addVertex("v");

        Edge e0 = g.addEdge("0", v, v, "label");
        Edge e1 = g.addEdge("1", v, v, "label");
        Edge e2 = g.addEdge("2", v, v, "label");

        Visibility vis0 = new Visibility();
        vis0.setFormalVisibility("0");
        Visibility vis1 = new Visibility();
        vis1.setFormalVisibility("1");
        Visibility vis2 = new Visibility();
        vis2.setFormalVisibility("2");

        e0.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis0)));
        e1.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis1)));
        e2.setProperty("foo", Collections.singletonList(new PropertyValueMap("bar", vis2)));

        ctx.setDefaultElementPermissions(Permission.READ);
        ctx.setElementPermissions(v, PermissionUtils.NO_PERMS);
        ctx.setPermissions(vis0, PermissionUtils.NO_PERMS);
        ctx.setPermissions(vis1, Permission.DISCOVER);
        ctx.setPermissions(vis2, Permission.READ);

        assertElementIds(Sets.newHashSet("1", "2"), f.getEdges("foo", "bar"));
    }
}
