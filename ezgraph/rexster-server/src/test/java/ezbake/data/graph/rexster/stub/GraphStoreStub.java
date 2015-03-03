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

package ezbake.data.graph.rexster.stub;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.tinkerpop.rexster.RexsterApplicationGraph;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.graph.rexster.graphstore.GraphStore;

/**
 * Stub used for testing {@link ezbake.data.graph.rexster.SecurityTokenRexsterApplication}.
 */
public class GraphStoreStub implements GraphStore {

    private static final String UNSUPPORTED_ERR_MSG =
            "'%s' not supported on GraphStoreStub, implement as needed for tests.";

    // members for validating input/return values.
    public final Set<String> testGraphNames = new HashSet<>();
    public boolean getGraphNamesCalled;
    public EzSecurityToken getGraphNamesToken;

    @Override
    public RexsterApplicationGraph getApplicationGraph(
            String graphName, EzSecurityToken token) {
        throw new UnsupportedOperationException(
                String.format(
                        UNSUPPORTED_ERR_MSG, "getApplicationGraph"));
    }

    @Override
    public void initialize(Properties props) {
        throw new UnsupportedOperationException(String.format(UNSUPPORTED_ERR_MSG, "initialize"));
    }

    @Override
    public Set<String> getGraphNames(EzSecurityToken token) {
        getGraphNamesCalled = true;
        getGraphNamesToken = token;
        return testGraphNames;
    }
}
