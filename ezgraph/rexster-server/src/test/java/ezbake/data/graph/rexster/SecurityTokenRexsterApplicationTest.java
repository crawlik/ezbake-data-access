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

import java.util.Properties;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.NotFoundException;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.graph.rexster.graphstore.ManagedGraphStore;
import ezbake.data.graph.rexster.stub.GraphStoreStub;
import ezbake.data.test.TestUtils;
import ezbake.thrift.ThriftUtils;

public class SecurityTokenRexsterApplicationTest {

    /**
     * Object being tested.
     */
    private SecurityTokenRexsterApplication securityTokenRexsterApplication;

    /**
     * EzBake properties
     */
    private Properties properties;

    @Before
    public void setUp() throws EzConfigurationLoaderException {
        properties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        securityTokenRexsterApplication = new SecurityTokenRexsterApplication(properties);
    }

    /**
     * Tests that a SecurityException is thrown if an EzBakeSecurityToken is not available in it's context via the
     * {@link SecurityTokenSecurityFilter}
     */
    @Test(expected = SecurityException.class)
    public void testGetApplicationGraphNoToken() {
        securityTokenRexsterApplication.getApplicationGraph("NonApplicableParam");
    }

    /**
     * Tests that an exception is thrown if the passed in graph name cannot be used to locate a graph.
     *
     * @throws org.apache.thrift.TException If an exception occurs serializing the token.
     */
    @Test(expected = NotFoundException.class)
    public void getApplicationGraphNoGraphFound() throws TException {
        SecurityTokenSecurityFilter filter = new SecurityTokenSecurityFilter();
        String tokenHeader = ThriftUtils.serializeToBase64(TestUtils.createTS_S_B_User());
        filter.configure(properties);
        filter.authenticate(tokenHeader, null);
        securityTokenRexsterApplication.getApplicationGraph("NotFoundGraph");
    }

    //TODO: RexPro script requests always query available graphs, but aren't guaranteed to have a token in their context
    //    public void testGetGraphsNoToken(){
    //         securityTokenRexsterApplication.getGraphNames();
    //     }

    @Test
    public void testGetGraphs() throws TException {
        properties.setProperty(ManagedGraphStore.GRAPH_MANAGER_KEY, GraphStoreStub.class.getName());
        EzSecurityToken token = TestUtils.createTS_S_B_User();
        SecurityTokenSecurityFilter filter = new SecurityTokenSecurityFilter();
        String tokenHeader = ThriftUtils.serializeToBase64(token);
        filter.configure(properties);
        filter.authenticate(tokenHeader, null);

        securityTokenRexsterApplication.getGraphNames();
    }
}
