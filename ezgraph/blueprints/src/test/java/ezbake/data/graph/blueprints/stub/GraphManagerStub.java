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

package ezbake.data.graph.blueprints.stub;

import com.tinkerpop.blueprints.Graph;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.graph.blueprints.graphmgmt.GraphManagementException;
import ezbake.data.graph.blueprints.graphmgmt.GraphManager;

/**
 * Stub for GraphManager, contains some canned code and ways to test error cases.
 */
public class GraphManagerStub implements GraphManager {

    /**
     * When passed into add or remove Graph methods, causes a {@link GraphManagementException} to be thrown.
     */
    public static final String THROW_EXCEPTION = "throwException";

    public boolean openGraphCalled;
    public String openGraphGraphName;
    public EzSecurityToken openGraphToken;

    public boolean addGraphCalled;
    public String addGraphGraphName;

    public boolean removeGraphCalled;
    public String removeGraphGraphName;

    @Override
    public Graph openGraph(String graphName) throws GraphManagementException {
        openGraphCalled = true;
        openGraphGraphName = graphName;

        return null;
    }

    @Override
    public void addGraph(String graphName) throws GraphManagementException {
        if (graphName == THROW_EXCEPTION) {
            throw new GraphManagementException("nomsg");
        }

        addGraphCalled = true;
        addGraphGraphName = graphName;
    }

    @Override
    public void removeGraph(String graphName) throws GraphManagementException {
        if (graphName == THROW_EXCEPTION) {
            throw new GraphManagementException("nomsg");
        }

        removeGraphCalled = true;
        removeGraphGraphName = graphName;
    }
}
