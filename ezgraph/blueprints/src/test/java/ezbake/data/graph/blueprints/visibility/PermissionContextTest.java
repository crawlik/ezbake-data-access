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

import java.util.Set;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class PermissionContextTest {

    private Vertex vertex;
    private Edge edge;
    private PermissionContext ctx1;
    private PermissionContext ctx2;

    private static class PermissionContext1 extends PermissionContext {
        @Override
        public Set<Permission> getPermissions(Visibility visibility) {
            return null;
        }

        @Override
        public VisibilityDeserializer getElementVisibilityDeserializer() {
            return null;
        }

        @Override
        public VisibilityDeserializer getPropertyVisibilityDeserializer() {
            return null;
        }
    }

    private static class PermissionContext2 extends PermissionContext {
        @Override
        public Set<Permission> getPermissions(Visibility visibility) {
            return null;
        }

        @Override
        public VisibilityDeserializer getElementVisibilityDeserializer() {
            return null;
        }

        @Override
        public VisibilityDeserializer getPropertyVisibilityDeserializer() {
            return null;
        }
    }

    @Before
    public void setUp() {
        Graph graph = new TinkerGraph();
        vertex = graph.addVertex(0);
        edge = vertex.addEdge("loop", vertex);
        ctx1 = new PermissionContext1();
        ctx2 = new PermissionContext2();
    }

    @Test
    public void testAsVisibilityFilterEdgeChecksInstance() {
        Edge f = ctx1.asVisibilityFilterEdge(edge);
        Edge g = ctx1.asVisibilityFilterEdge(f);

        assertSame(f, g);
    }

    @Test
    public void testAsVisibilityFilterEdgeChecksContext() {
        Edge e1 = ctx1.asVisibilityFilterEdge(edge);
        Edge e2 = ctx2.asVisibilityFilterEdge(edge);

        assertNotSame(e1, e2);
    }

    @Test
    public void testAsVisibilityFilterVertexChecksInstance() {
        Vertex f = ctx1.asVisibilityFilterVertex(vertex);
        Vertex g = ctx1.asVisibilityFilterVertex(f);

        assertSame(f, g);
    }

    @Test
    public void testAsVisibilityFilterVertexChecksContext() {
        Vertex v1 = ctx1.asVisibilityFilterVertex(vertex);
        Vertex v2 = ctx2.asVisibilityFilterVertex(vertex);

        assertNotSame(v1, v2);
    }
}
