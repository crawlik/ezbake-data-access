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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.thrift.TException;

import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.protocol.EngineConfiguration;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.protocol.msg.ErrorResponseMessage;
import com.tinkerpop.rexster.protocol.msg.RexProMessage;
import com.tinkerpop.rexster.protocol.msg.ScriptRequestMessage;
import com.tinkerpop.rexster.protocol.msg.ScriptResponseMessage;
import com.tinkerpop.rexster.protocol.msg.SessionRequestMessage;
import com.tinkerpop.rexster.server.RexsterProperties;
import com.tinkerpop.rexster.server.RexsterSettings;

public class RexsterTestUtils {

    /**
     * Some request expect a password but never use it. We pass this value in to demonstrate.
     */
    public static final String ANY_NON_BLANK_PASSWORD = "x";

    /**
     * Domain of Rexster to test against.
     */
    public static final String REXSTER_DOMAIN = "localhost";

    /**
     * Port of Rexpro to test against.
     */
    public static final int REXPRO_PORT = RexsterSettings.DEFAULT_REXPRO_PORT;

    /**
     * Port of Rexster Rest to test against.
     */
    public static final int REXSTER_PORT = RexsterSettings.DEFAULT_HTTP_PORT;

    /**
     * Gremlin-groovy query to return all vertices on a graph.
     */
    public static final String GET_ALL_VERTICES = "g.V";

    /**
     * A visibility marking. All security tokens in these tests have this authorization.
     */
    public static final String A_MARKING = "A";

    /**
     * A visibility marking. Only the 'AB' security token has this marking. Without this marking, values with a formal
     * visibility of 'A&B' cannot be accessed.
     */
    public static final String B_MARKING = "B";

    /**
     * Gets a URL that can retrieve a graph via the Rexster REST API.
     *
     * @param graphName name of the graph to retrieve
     * @return a URL that can get the graph identified the given graphName
     */
    public static String getGraphUrl(String graphName) {
        return String.format("http://%s:%s/graphs/%s", REXSTER_DOMAIN, REXSTER_PORT, graphName);
    }

    /**
     * Opens a session and returns the UUID for that session as a byte[].
     *
     * @param token Serialized EzSecurityToken to associate with the session.
     * @return The UUID of the session in a byte[].
     */
    public static byte[] initiateSession(String token, String graph, RexsterClient client) throws Exception {
        final SessionRequestMessage sessionRequestMessage = new SessionRequestMessage();
        sessionRequestMessage.Username = token;
        sessionRequestMessage.Password = ANY_NON_BLANK_PASSWORD;
        sessionRequestMessage.metaSetGraphName(graph);
        sessionRequestMessage.setRequestAsUUID(UUID.randomUUID());
        final RexProMessage sessionResponse = client.execute(sessionRequestMessage);
        if (sessionResponse instanceof ErrorResponseMessage) {
            throw new RuntimeException(((ErrorResponseMessage) sessionResponse).ErrorMessage);
        }
        return sessionResponse.Session;
    }

    /**
     * Makes a request to RexPro.
     *
     * @param session the session to make the request under
     * @param script the script which forms the content of the response
     * @return a ScriptResponseMessage containing the results of the request
     */
    public static ScriptResponseMessage makeScriptRequest(byte[] session, String script, RexsterClient client)
            throws IOException, RexProException {
        final ScriptRequestMessage scriptRequest = new ScriptRequestMessage();
        scriptRequest.Session = session;
        scriptRequest.setRequestAsUUID(UUID.randomUUID());
        scriptRequest.Script = script;
        scriptRequest.LanguageName = "groovy";
        scriptRequest.metaSetInSession(true);

        final RexProMessage scriptResponse = client.execute(scriptRequest);

        if (scriptResponse instanceof ErrorResponseMessage) {
            throw new RuntimeException(((ErrorResponseMessage) scriptResponse).ErrorMessage);
        }

        return (ScriptResponseMessage) scriptResponse;
    }

    /**
     * Creates a header for HTTP requests on ez Rexster.
     *
     * @param token base64-serialized token to use with Rexster
     * @return a String that can be used as the header for a HTTP request.
     */
    public static String makeAuthorizationHeader(String token) throws TException, UnsupportedEncodingException {
        final byte[] bytes = Base64.encodeBase64((token + ":nopass").getBytes("UTF-8"));
        final String headerPart2 = new String(bytes, "UTF-8");
        return "Basic " + headerPart2;
    }

    /**
     * Get a gremlin formatted script that should add a vertex with the given id, visibility, and properties.
     *
     * @param id id for the vertex
     * @param visibility visibility of the vertex
     * @param properties properties for the vertex
     * @return a string that can be passed to gremlin that adds a vertex with the specified parameters
     */
    public static String makeAddVertexGremlinRequest(String id, String visibility, String... properties) {
        final StringBuilder request = new StringBuilder();
        request.append("g.addVertex(").append('"').append(id).append("\",").append("[ezbake_visibility:\"")
                .append(visibility).append('"');

        int count = 0;
        for (final String property : properties) {
            if (count == 0) {
                request.append(',');
            }
            count++;
            request.append(property);
            if (count != properties.length) {
                request.append(',');
            }
        }
        request.append("])");
        return request.toString();
    }

    /**
     * Takes a property name and a map of base64 encoded visibility: value and converts it to a groovy formatted string,
     * suitable for gremlin queries.
     *
     * @param propName name of property
     * @param values a map of serialized visibility : string value
     * @return a groovy formatted string that represents a property key and value that can be used in request scripts.
     */
    public static String makeEzPropertyString(String propName, Map<String, String> values) {
        final StringBuilder prop = new StringBuilder();
        prop.append(propName).append(":[");

        int count = 1;
        for (final Map.Entry entry : values.entrySet()) {
            prop.append("[visibility:\"").append(entry.getKey()).append("\",").append("value:\"")
                    .append(entry.getValue()).append("\"]");
            if (count < values.size()) {
                prop.append(',');
            }
            count++;
        }
        prop.append(']');
        return prop.toString();
    }

    /**
     * When we don't start rexster as the standalone app, groovy (or other script engines) don't get initialized.  Pass
     * {@link com.tinkerpop.rexster.server.RexsterProperties} here to configure script engines in those properties.
     *
     * @param properties Rexster properties containing script engines to configure
     */
    public static void initScriptEngines(RexsterProperties properties) {
        final List<EngineConfiguration> configuredScriptEngines = new ArrayList<>();

        final List<HierarchicalConfiguration> configs = properties.getScriptEngines();
        for (HierarchicalConfiguration config : configs) {
            configuredScriptEngines.add(new EngineConfiguration(config));
        }
        EngineController.configure(configuredScriptEngines);
    }
}
