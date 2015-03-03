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

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.junit.Test;

import ezbake.base.thrift.Permission;

import static ezbake.data.graph.blueprints.util.Assert.assertElementIds;

public class MockPermissionContextTest {

    @Test
    public void testFilterVertices() {
        Graph g = new TinkerGraph();

        Vertex v1 = g.addVertex("1");
        Vertex v2 = g.addVertex("2");

        MockPermissionContext ctx = new MockPermissionContext();
        ctx.setElementPermissions(v1, Permission.READ);
        ctx.setElementPermissions(v2, Permission.DISCOVER);

        ElementFilter filter = ctx.getElementFilter();
        assertElementIds(Sets.newHashSet("1"), filter.filterVertices(Arrays.asList(v1, v2), Permission.READ));
    }
}
