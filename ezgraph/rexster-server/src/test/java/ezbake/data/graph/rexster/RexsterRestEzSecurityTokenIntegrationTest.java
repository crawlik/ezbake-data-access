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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.rexster.server.HttpRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterProperties;
import com.tinkerpop.rexster.server.RexsterServer;
import com.tinkerpop.rexster.server.RexsterSettings;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.data.test.TestUtils;
import ezbake.thrift.ThriftUtils;

public class RexsterRestEzSecurityTokenIntegrationTest {

    /**
     * Rexster HttpServer
     */
    private RexsterServer httpServer;

    /**
     * Set as custom RexsterApplication that integrates with the EzSecurityToken
     */
    private RexsterApplication rexsterApplication;

    /**
     * RexsterProperties for getting host:port for rest calls.
     */
    private RexsterProperties properties;

    /**
     * Builds the authorization header value.
     *
     * @param token The token to be inserted into the header.
     * @return A string that can be assigned to the {@link javax.ws.rs.core.HttpHeaders#AUTHORIZATION} header for making
     * rest requests on Rexster.
     * @throws org.apache.thrift.TException If an exception occurred while serializing the token.
     */
    private static String getAuthorizationHeader(EzSecurityToken token) throws TException {
        final String tokenb64 = ThriftUtils.serializeToBase64(token);
        final byte[] bytes = Base64.encodeBase64((tokenb64 + ":nopass").getBytes());
        final String headerPart2 = new String(bytes);
        return "Basic " + headerPart2;
    }

    /**
     * Starts Rexster and RexPro with the appropriate configuration.
     *
     * @throws Exception If an exception occured while starting the HttpServer.
     */
    @Before
    public void setUp() throws Exception {
        RexsterProperties rexsterProperties = new RexsterProperties("config/rexster.xml");
        Properties ezBakeProperties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        properties = RexsterPropertiesUtils.combineRexsterEzBakeProperties(
                rexsterProperties, ezBakeProperties);

        httpServer = new HttpRexsterServer(properties);
        rexsterApplication = new SecurityTokenRexsterApplication(ezBakeProperties);
        httpServer.start(this.rexsterApplication);
    }

    /**
     * Tests that the security token has been set by the time the RexsterApplicationGraph is instantiated.
     *
     * @throws java.io.IOException if an error occurred while making the HttpRequest using the URL.
     * @throws org.apache.thrift.TException If an error occurred serializing the token.
     */
    @Test
    public void testContextRexster() throws IOException, TException {
        final int rexsterServerPort = properties.getConfiguration().getInteger(
                "http.server-port", RexsterSettings.DEFAULT_HTTP_PORT);
        final String rexsterServerHost = properties.getConfiguration().getString("http.server-host", "0.0.0.0");

        final String url =
                String.format("http://%s:%s/graphs/non-existing-graph", rexsterServerHost, rexsterServerPort);

        final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestProperty(HttpHeaders.AUTHORIZATION, getAuthorizationHeader(TestUtils.createTS_S_B_User()));

        assertEquals("Not expected result from http request.", "Not Found", connection.getResponseMessage());
    }

    /**
     * Closes down started HttpRexsterServer
     *
     * @throws Exception If an exception occurred attempting to shut down the httpServer.
     */
    @After
    public void tearDown() throws Exception {
        httpServer.stop();
    }
}
