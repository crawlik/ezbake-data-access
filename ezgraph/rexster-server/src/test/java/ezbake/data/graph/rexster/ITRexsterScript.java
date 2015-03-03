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

package ezbake.data.graph.rexster;

import org.junit.Test;

import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;

import ezbake.data.graph.rexster.graphstore.GraphManagerGraphAndManagedGraphStoreIntegrationTest;

/**
 * To be run once Rexster has been started in standalone mode, runs tests from {@link
 * ezbake.data.graph.rexster.graphstore.GraphManagerGraphAndManagedGraphStoreIntegrationTest} and {@link
 * ezbake.data.graph.rexster .VisibilityFilterGraphWrapperIntegrationTest}. Note that the tests in ManagedGraphStoreTest
 * must be run first in order to create the graph that will be used.
 */
public class ITRexsterScript {

    @Test
    public void testScriptSuccessful() throws Exception {
        int count = 0;
        boolean disconnected = true;
        RexsterClient client = null;

        // verify that we can connect to Rexster
        while (disconnected) {
            try {
                client = RexsterClientFactory.open("localhost", 8184);
                client.execute("");
            } catch (final RexProException e) {
                //we expect a security exception (in the form of RexProException) if the connection is established.
                disconnected = false;
            } catch (final Exception e) {
                count++;
                if (count > 10) {
                    throw new RuntimeException("Failed to connect to Rexster!");
                }
                Thread.sleep(300);
            }
        }

        // close the client as it was only used for verifying the RexPro server is available
        client.close();

        // create a graph with ID defined by the constant MANAGED_GRAPH_NAME in ManagedGraphStoreTest
        GraphManagerGraphAndManagedGraphStoreIntegrationTest.main();

        // use the created graph in some select tests
        VisibilityFilterGraphWrapperIntegrationTest
                .main(GraphManagerGraphAndManagedGraphStoreIntegrationTest.MANAGED_GRAPH_NAME);
    }
}
