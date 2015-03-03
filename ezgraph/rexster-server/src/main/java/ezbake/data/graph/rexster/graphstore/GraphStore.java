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

import java.util.Properties;
import java.util.Set;

import com.tinkerpop.rexster.RexsterApplicationGraph;

import ezbake.base.thrift.EzSecurityToken;

/**
 * Interface that provides a pluggable way to define how graphs are supplied to the {@link
 * ezbake.data.graph.rexster.SecurityTokenRexsterApplication}
 */
public interface GraphStore {

    /**
     * Gets a graph by identified by the given name.
     *
     * @param graphName name of the graph to get
     * @return a RexsterApplicationGraph that can be used to access a graph through the Blueprints API
     */
    RexsterApplicationGraph getApplicationGraph(String graphName, EzSecurityToken token);

    /**
     * Initializes this GraphStore with the given properties.
     *
     * @param props properties with which to initialize this GraphStore
     */
    void initialize(Properties props);

    /**
     * Gets graphs available via this GraphStore.
     *
     * @return the set of available graphs in this GraphStore
     */
    Set<String> getGraphNames(EzSecurityToken token);
}
