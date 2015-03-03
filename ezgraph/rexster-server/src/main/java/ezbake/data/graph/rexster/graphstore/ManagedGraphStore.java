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

package ezbake.data.graph.rexster.graphstore;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.rexster.RexsterApplicationGraph;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.graph.blueprints.graphmgmt.GraphManagementException;
import ezbake.data.graph.blueprints.graphmgmt.GraphManager;
import ezbake.data.graph.blueprints.graphmgmt.GraphManagerGraphFilterGraph;
import ezbake.data.graph.blueprints.graphmgmt.TinkerGraphManager;
import ezbake.data.graph.blueprints.visibility.DefaultPermissionContext;
import ezbake.data.graph.blueprints.visibility.PermissionContext;
import ezbake.data.graph.blueprints.visibility.VisibilityFilterGraph;

/**
 * Uses a {@link ezbake.data.graph.blueprints.graphmgmt.GraphManager} to provide graphs. A GraphManager like {@link
 * ezbake.data.graph.blueprints.graphmgmt.TinkerGraphManager} that has a dedicated graph management graph (a graph
 * wrapped by {@link ezbake.data.graph.blueprints.graphmgmt.GraphManagerGraphFilterGraph}) named {@link
 * ezbake.data.graph.blueprints.graphmgmt.GraphManagerGraphFilterGraph#GRAPH_MANAGER_GRAPH_NAME} should be used.  To add
 * or remove graphs, this graph management graph can be accessed and manipulated via Rexster's interfaces.
 * <p/>
 * Before a graph is returned, both the existence of the graph and the requesting parties' access to the graph must be
 * determined. To determine the graph's existence, the GraphManager (which has no security controls) simply has {@code
 * openGraph(...)} called with the given graph name (a lack of an exception indicates the graph exists). Access to the
 * graph is determined by wrapping the graph management graph in a {@link ezbake.data.graph.blueprints.visibility
 * .VisibilityFilterGraph} and querying for the existence of the Vertex with ID equal to the graph name. This GraphStore
 * will return a graph if both the GraphManager returns a graph AND a Vertex with ID equal to the graph name can be
 * found on the graph management graph. If the graph exists but cannot be found in the graph management graph (indicated
 * the requester does not have read permissions), an exception indicating access denied is thrown.
 */
public class ManagedGraphStore implements GraphStore {

    /**
     * Key to property whose value should be the fully qualified name of a class implementing GraphManager.  Will be
     * used to populate the {@code manager} field for this.
     */
    public static final String GRAPH_MANAGER_KEY = "graph.manager.class";

    /**
     * Logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(ManagedGraphStore.class);

    /**
     * This uses this GraphManager to retrieve and return graphs when graph requests are made on this GraphStore.
     */
    private GraphManager manager;

    /**
     * Gets a {@code Set<String>} from an Vertex Iterator by extracting the ID value from each vertex.
     *
     * @param vertices vertices from which to extract ids
     * @return IDs for the given vertices
     */
    private static Set<String> extractIds(Iterable<Vertex> vertices) {
        final Set<String> ids = new HashSet<>();
        final Iterator<Vertex> it = vertices.iterator();
        while (it.hasNext()) {
            ids.add((String) it.next().getId());
        }
        return ids;
    }

    @Override
    public RexsterApplicationGraph getApplicationGraph(String graphName, EzSecurityToken token) {
        final Graph graphManagerGraph = new GraphManagerGraphFilterGraph(
                manager.openGraph(GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME), manager);

        if (GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME.equals(graphName)) {
            return new RexsterApplicationGraph(graphName, graphManagerGraph);
        }

        final Graph graph = manager.openGraph(graphName);

        final PermissionContext permissionContext = new DefaultPermissionContext(token);
        final Graph wrapped = new VisibilityFilterGraph(graphManagerGraph, permissionContext);

        if (wrapped.getVertex(graphName) == null) {
            throw new GraphManagementException(String.format("Access to graph '%s' denied!", graphName));
        }

        // TODO: Determine best way to keep security context up to date while still caching graphs
        return new RexsterApplicationGraph(graphName, graph);
    }

    @Override
    public void initialize(Properties properties) {
        final String managerName = properties.getProperty(GRAPH_MANAGER_KEY, TinkerGraphManager.class.getName());
        final Class clazz;
        try {
            logger.info("Attempting to initalize GraphManager from class {}", managerName);
            clazz = Class.forName(managerName);
        } catch (final ClassNotFoundException e) {
            final String errMsg = String.format("Unable to find class: %s.  Aborting startup.", managerName);
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }

        if (GraphManager.class.isAssignableFrom(clazz)) {
            try {
                manager = (GraphManager) clazz.newInstance();
            } catch (final Exception e) {
                final String errMsg = String.format("Error instantiating instance of GraphManager: %s!", managerName);
                logger.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        } else {
            final String errMsg = String.format(
                    "Property value for %s must refer to a class that implements GraphManager.", GRAPH_MANAGER_KEY);

            logger.error(errMsg);
            throw new RuntimeException(errMsg);
        }
    }

    @Override
    public Set<String> getGraphNames(EzSecurityToken token) {
        final Graph graphManagerGraph = new GraphManagerGraphFilterGraph(
                manager.openGraph(GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME), manager);

        //TODO: RexPro requests always call this method and are not currently guaranteed to have access to a token
        //  final PermissionContext permissionContext = new DefaultPermissionContext(token);
        //  final Graph wrapped = new VisibilityFilterGraph(graphManagerGraph, permissionContext);

        final Set<String> availableGraphs = extractIds(graphManagerGraph.getVertices());
        availableGraphs.add(GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME);
        return availableGraphs;
    }
}
