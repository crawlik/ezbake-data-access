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

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

/**
 * Graph management system that uses a graph management graph AKA 'GraphManagerGraph' to add, remove, and update a map
 * of {@link TinkerGraph}. This in-memory GraphManager is intended to be used for testing and development.
 * <p/>
 * A graph management graph is expected to be wrapped by {@link GraphManagerGraphFilterGraph}, and that filter is given
 * an instance of this class or another GraphManager. When vertex operations are performed through the
 * GraphManagerGraphFilterGraph, both the graph management graph and the GraphManager are manipulated. Vertices in the
 * graph management graph are expected to correspond to, and be synchronized with, graphs in the GraphManager. In other
 * words, adding or removing a vertex in the graph management graph will result in graphs being added or removed from
 * the GraphManager. Note that this does not occur in the reverse direction - direct modifications to the GraphManager
 * will not be reflected in a 'associated' graph management graph, so the Blueprints interface {@code
 * GraphManagerGraphFilterGraph} should be used to interact with objects of this class rather than directly calling
 * methods on it.
 * <p/>
 * When using a graph management graph in EzBake's blueprints system, it is expected that that graph will be named
 * {@link GraphManagerGraphFilterGraph#GRAPH_MANAGER_GRAPH_NAME}. Components developed for EzBake's Rexster project will
 * look for a graph of this name when attempting to retrieve a graph management graph.
 */
public class TinkerGraphManager implements GraphManager {

    /**
     * GraphManagerGraph used to manage available graphs.
     */
    private final Graph graphManagerGraph;

    /**
     * This cache stands in for an 'actual' backend where graphs can be requested by name.
     */
    private final Map<String, Graph> constructedTinkerGraphs;

    /**
     * Constructs a new TinkerGraphManager and initializes an unwrapped GraphManagerGraph.
     */
    public TinkerGraphManager() {
        this(new HashMap<String, Graph>());
    }

    /**
     * Constructs a new TinkerGraphManager using the given map of TinkerGraph. This is helpful for verifying the
     * behavior of the TinkerGraphFilter by having direct access to the passed in Map.
     *
     * @param constructedTinkerGraphs map of TinkerGraph to use as the 'backend'
     */
    @VisibleForTesting
    TinkerGraphManager(Map<String, Graph> constructedTinkerGraphs) {
        graphManagerGraph = new TinkerGraph();
        this.constructedTinkerGraphs = constructedTinkerGraphs;
    }

    @Override
    public Graph openGraph(String graphName) throws GraphManagementException {
        if (GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME.equals(graphName)) {
            return graphManagerGraph;
        }

        final Graph graph = constructedTinkerGraphs.get(graphName);

        if (graph == null) {
            throw new GraphManagementException(String.format("Graph '%s' cannot be found!", graphName));
        }

        return graph;
    }

    @Override
    public void addGraph(String graphName) throws GraphManagementException {

        if (GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME.equals(graphName)) {
            throw new GraphManagementException("'GraphManagerGraph' is a reserved name! Cannot add graph.");
        }

        if (graphManagerGraph.getVertex(graphName) != null) {
            throw new GraphManagementException(String.format("Graph '%s' already exists.", graphName));
        }

        constructedTinkerGraphs.put(graphName, new TinkerGraph());
    }

    @Override
    public void removeGraph(String graphName) throws GraphManagementException {
        //only called after passing through visibility filter, so no need to manage visibility.
        constructedTinkerGraphs.remove(graphName);
    }
}
