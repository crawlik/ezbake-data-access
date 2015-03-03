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

import com.tinkerpop.blueprints.Graph;

/**
 * A {@code GraphManager} keeps track of a group of graphs identifiable by name and provides operations to add, remove,
 * and open them.
 * <p/>
 * For example, see {@link TinkerGraphManager} which keeps track of a Map of graph name to graph.
 * <p/>
 * With EzGraph, an implementation of this interface is expected to be given to a {@link GraphManagerGraphFilterGraph}
 * which allows an user to interact with this API via a Blueprints interface, particularly allowing Rexster to be used.
 * See the {@code GraphManagerGraphFilter} for more info on this.
 */
public interface GraphManager {

    /**
     * Opens the graph with the given name, if it can be found.
     *
     * @param graphName name of the graph to open
     * @return a graph by the given name
     * @throws GraphManagementException if the graph cannot be found
     */
    Graph openGraph(String graphName) throws GraphManagementException;

    /**
     * Creates and retrieves a new graph under the given name, with the given visibility.
     *
     * @param graphName name of the graph to create and return
     * @throws GraphManagementException if the graph already exists
     */
    void addGraph(String graphName) throws GraphManagementException;

    /**
     * Deletes a graph with the given name.
     *
     * @param graphName name of the graph to delete
     * @throws GraphManagementException if the graph cannot be found
     */
    void removeGraph(String graphName) throws GraphManagementException;
}
