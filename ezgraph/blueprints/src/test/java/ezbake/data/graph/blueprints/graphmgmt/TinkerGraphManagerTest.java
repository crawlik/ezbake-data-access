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
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.Graph;

import ezbake.base.thrift.Visibility;

/**
 * Tests methods on {@link TinkerGraphManager}.
 */
public class TinkerGraphManagerTest {

    /**
     * System under test.
     */
    private TinkerGraphManager manager;
    private HashMap<String, Graph> managedGraphs;

    @Before
    public void setUp() throws TException {
        final Visibility abVisibility = new Visibility();
        abVisibility.setFormalVisibility(("a&b"));

        managedGraphs = new HashMap<>();
        manager = new TinkerGraphManager(managedGraphs);
    }

    @Test
    public void testAddGraph() {
        final String graphName = "bo";
        assertEquals(0, managedGraphs.size());
        manager.addGraph(graphName);
        assertEquals(1, managedGraphs.size());
        assertNotNull(managedGraphs.get(graphName));
    }

    @Test
    public void testRemoveGraph() {
        final String graphName = "bo";
        manager.addGraph(graphName);
        manager.removeGraph(graphName);
        assertEquals(0, managedGraphs.size());
    }

    @Test(expected = GraphManagementException.class)
    public void testAddGraphManagerGraph() {
        manager.addGraph(GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME);
    }

    @Test
    public void testOpenGraphGraphManagerGraph() {
        assertNotNull(manager.openGraph(GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME));
    }

    @Test
    public void testOpenGraph() {
        final String graphName = "graphName";
        manager.addGraph(graphName);
        assertNotNull(managedGraphs.get(graphName));
        assertNotNull(manager.openGraph(graphName));
    }
}
