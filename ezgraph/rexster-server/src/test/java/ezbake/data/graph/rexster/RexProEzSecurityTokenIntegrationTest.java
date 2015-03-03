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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.protocol.msg.ErrorResponseMessage;
import com.tinkerpop.rexster.protocol.msg.RexProMessage;
import com.tinkerpop.rexster.protocol.msg.ScriptRequestMessage;
import com.tinkerpop.rexster.protocol.msg.ScriptResponseMessage;
import com.tinkerpop.rexster.protocol.msg.SessionRequestMessage;
import com.tinkerpop.rexster.server.RexProRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterProperties;
import com.tinkerpop.rexster.server.RexsterServer;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.data.test.TestUtils;
import ezbake.thrift.ThriftUtils;

/**
 * Tests that the EzSecurityToken is being set in the right context for Rexster to use.
 */
public class RexProEzSecurityTokenIntegrationTest {

    /**
     * Rexpro server
     */
    private RexsterServer rexproServer;

    /**
     * Rexster client from which to make RexPro requests.
     */
    private RexsterClient rexsterClient;

    /**
     * Starts Rexster and RexPro with the appropriate configuration.
     *
     * @throws Exception If an error occurs starting the RexPro server or opening a client connection to it.
     */
    @Before
    public void setUp() throws Exception {
        RexsterProperties rexsterProperties = new RexsterProperties("config/rexster.xml");
        Properties ezBakeProperties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        RexsterProperties properties = RexsterPropertiesUtils.combineRexsterEzBakeProperties(
                rexsterProperties, ezBakeProperties);

        RexsterTestUtils.initScriptEngines(rexsterProperties);

        rexproServer = new RexProRexsterServer(properties, true);

        final RexsterApplication rexsterApplication = new SecurityTokenRexsterApplication(ezBakeProperties);
        rexproServer.start(rexsterApplication);

        rexsterClient = RexsterClientFactory.open("localhost", 8184);
    }

    /**
     * Opens a session and attempts to make requests using either blank, or invalid sessionIds.
     *
     * @throws org.apache.thrift.TException if there was an error serializing the EzSecurityToken
     * @throws java.io.IOException If an error occurred while trying to communicate with RexPro.
     * @throws com.tinkerpop.rexster.client.RexProException If there as an error with RexPro message handling.
     */
    @Test
    public void testNonSession() throws IOException, RexProException, TException {

        //assignment just for illustration, we are creating a session but not referring to it.
        final byte[] session = initiateSession("emptygraph", TestUtils.createTS_S_B_User());
        final byte[] invalidSession = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        final byte[] noSession = new byte[16];

        try {
            makeScriptRequest(invalidSession, RexsterTestUtils.GET_ALL_VERTICES);
            fail();
        } catch (RuntimeException e) {
            assertEquals("The session on the request does not exist or has otherwise expired.", e.getMessage());
        }

        try {
            makeScriptRequest(noSession, RexsterTestUtils.GET_ALL_VERTICES);
            fail();
        } catch (RuntimeException e) {
            assertEquals("Cannot make sessionless requests with <security> turned on.", e.getMessage());
        }
    }

    /**
     * Closes down started servers.
     *
     * @throws Exception If there was an error shutting down the RexsterClient and RexPro server.
     */
    @After
    public void tearDown() throws Exception {
        rexsterClient.close();
        rexproServer.stop();
    }

    /**
     * Makes a request to RexPro.
     *
     * @param session The session to make the request under.
     * @param script The script which forms the content of the response.
     * @return A ScriptResponseMessage containing the results of the request.
     * @throws java.io.IOException If an error occurred while trying to communicate with RexPro.
     * @throws com.tinkerpop.rexster.client.RexProException If there as an error with RexPro message handling.
     */

    private ScriptResponseMessage makeScriptRequest(byte[] session, String script) throws IOException, RexProException {
        ScriptRequestMessage scriptRequest = new ScriptRequestMessage();
        scriptRequest.Session = session;
        scriptRequest.setRequestAsUUID(UUID.randomUUID());
        scriptRequest.Script = script;
        scriptRequest.LanguageName = "groovy";
        scriptRequest.metaSetInSession(true);

        RexProMessage scriptResponse = rexsterClient.execute(scriptRequest);

        if (scriptResponse instanceof ErrorResponseMessage) {
            throw new RuntimeException(((ErrorResponseMessage) scriptResponse).ErrorMessage);
        }

        return (ScriptResponseMessage) scriptResponse;
    }

    /**
     * Opens a session and returns the UUID for that session as a byte[].
     *
     * @param graphName Name of the graph to base the session on.
     * @param ezSecurityToken EzSecurityToken to associate with the session.
     * @return The UUID of the session in a byte[].
     * @throws org.apache.thrift.TException If an exception occurred while serializing the security token.
     * @throws java.io.IOException If an error occurred while trying to communicate with RexPro.
     * @throws com.tinkerpop.rexster.client.RexProException If there as an error with RexPro message handling.
     */
    private byte[] initiateSession(String graphName, EzSecurityToken ezSecurityToken)
            throws TException, IOException, RexProException {
        String token = ThriftUtils.serializeToBase64(ezSecurityToken);

        SessionRequestMessage sessionRequestMessage = new SessionRequestMessage();
        sessionRequestMessage.Username = token;
        sessionRequestMessage.Password = "RexProTest";
        sessionRequestMessage.metaSetGraphName(graphName);
        sessionRequestMessage.setRequestAsUUID(UUID.randomUUID());
        RexProMessage sessionResponse = rexsterClient.execute(sessionRequestMessage);
        if (sessionResponse instanceof ErrorResponseMessage) {
            throw new RuntimeException(((ErrorResponseMessage) sessionResponse).ErrorMessage);
        }
        return sessionResponse.Session;
    }
}

