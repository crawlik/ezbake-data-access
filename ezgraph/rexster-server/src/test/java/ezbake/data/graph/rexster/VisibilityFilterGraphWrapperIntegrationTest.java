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
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.ws.rs.core.HttpHeaders;

import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.rexster.RexsterGraph;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.protocol.msg.ScriptResponseMessage;
import com.tinkerpop.rexster.server.HttpRexsterServer;
import com.tinkerpop.rexster.server.RexProRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterProperties;
import com.tinkerpop.rexster.server.RexsterServer;

import ezbake.base.thrift.Visibility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.data.test.TestUtils;
import ezbake.thrift.ThriftUtils;

/**
 * Some of these tests are designed to work in conjunction with tests from {@link ezbake.data.graph.rexster
 * .graphstore.ManagedGraphStoreTest}. Specifically, graph creation is done in ManagedGraphStoreTest and writing to that
 * graph is tested more comprehensively here. These 'cooperative tests' should be done against a standalone instance of
 * 'ez' Rexster, see {@link ezbake.data.graph.rexster.ITRexsterScript}.
 */
public class VisibilityFilterGraphWrapperIntegrationTest {

    /**
     * Property key for a tinkergraph elements whose value are an {@link com.tinkerpop.blueprints.Element}'s Blueprints
     * properties.
     */
    private static final String BLUEPRINTS_PROPERTIES_KEY = "_properties";

    /**
     * A key used in the maps that compose an EzBake property value (a {@code List<Map<String,Object>>}). This key is
     * also used in the GraphSON data returned by Rexster.
     */
    private static final String VALUE_KEY = "value";

    /**
     * A property key that can be used for a property on an {@link com.tinkerpop.blueprints.Element}.
     */
    private static final String OCCUPATION_PROPERTY_KEY = "occupation";

    /**
     * A property value for key {@code OCCUPATION_PROPERTY_KEY}.
     */
    private static final String OCCUPATION_PROPVALUE_1 = "normal_car";

    /**
     * A property value for key {@code OCCUPATION_PROPERTY_KEY}.
     */
    private static final String OCCUPATION_PROPVALUE_2 = "superhero_car";

    /**
     * Name property value for a car vertex.
     */
    private static final String CAR_NAME = "andy";

    /**
     * Emptygraph is used for these tests. 'emptygraph' is an empty Tinkergraph.
     */
    private static final String DEFAULT_GRAPH = "emptygraph";

    /**
     * Rexpro server
     */
    private RexsterServer rexproServer;

    /**
     * Rexster HttpServer
     */
    private RexsterServer httpServer;

    /**
     * Rexster client from which to make RexPro requests.
     */
    private RexsterClient rexsterClient;

    /**
     * Base 64 encoded visibility string with formal visibility of {@code A&B}.
     */
    private String abVisibility;

    /**
     * Base 64 encoded visibility string with formal visibility of {@code A}.
     */
    private String aVisibility;

    /**
     * Base 64 encoded authorization string with auths: {@code A} and {@code B}.
     */
    private String abToken;

    /**
     * Base 64 encoded authorization string with auths: {@code A}.
     */
    private String aToken;

    /**
     * Graph name used when tests are run.
     */
    private String graphName;

    /**
     * Test Rexster instance that has already been stood up where RexPro is on {@code localhost:8184} and Rexster rest
     * is on {@code localhost:8182}.
     */
    public static void main(String... args) throws Exception {
        final VisibilityFilterGraphWrapperIntegrationTest tester = new VisibilityFilterGraphWrapperIntegrationTest();

        if (args.length > 0) {
            tester.graphName = args[0];
        } else {
            tester.graphName = DEFAULT_GRAPH;
        }

        tester.rexsterClient = RexsterClientFactory.open(RexsterTestUtils.REXSTER_DOMAIN, RexsterTestUtils.REXPRO_PORT);
        tester.generateEncodedTokensAndVisibilities();
        tester.queryVertsRestRexsterGremlinTest();

        tester.rexsterClient.close();
    }

    @Before
    public void setUp() throws Exception {
        graphName = DEFAULT_GRAPH;

        initRexClientAndServers();

        generateEncodedTokensAndVisibilities();
    }

    @After
    public void tearDown() throws Exception {
        rexsterClient.close();
        rexproServer.stop();
        httpServer.stop();
    }

    @Test
    public void queryVertsRexproTest() throws Exception {
        addVertexToGraphRexPro(CAR_NAME);

        verifyAddedVertexRexPro();

        final RexsterGraph abGraph = new RexsterGraph(
                RexsterTestUtils.getGraphUrl(graphName), 1, abToken, RexsterTestUtils.ANY_NON_BLANK_PASSWORD);

        verifyAddedVertexRest(abGraph);
    }

    @Test
    public void testRestUrlDirectly() throws Exception {
        final RexsterGraph abGraph = new RexsterGraph(
                RexsterTestUtils.getGraphUrl(graphName), 1, abToken, RexsterTestUtils.ANY_NON_BLANK_PASSWORD);

        addVertexToGraphRestNonGremlin();

        verifyAddedVertexRest(abGraph);
        verifyAddedVertexRexPro();
    }

    @Test
    public void queryVertsRestRexsterGremlinTest() throws Exception {
        final RexsterGraph abGraph = new RexsterGraph(
                RexsterTestUtils.getGraphUrl(graphName), 1, abToken, RexsterTestUtils.ANY_NON_BLANK_PASSWORD);

        addVertexToGraphRexsterRestGremlin(abGraph, CAR_NAME);

        verifyAddedVertexRest(abGraph);
        verifyAddedVertexRexPro();
    }

    /**
     * Use Rexster's rest interface to read a canned vertex from the the backend (for reads using Gremlin, see {@code
     * verifyAddedVertexRexPro()}.) Does basically the same thing as {@code verifyAddedVertexRexPro()}.
     *
     * @param abGraph Reuses a graph with A+B auths, if possible.
     */
    private void verifyAddedVertexRest(RexsterGraph abGraph) throws JSONException {
        final Iterable<Vertex> abVertices = abGraph.getVertices();
        final Iterator abIt = abVertices.iterator();
        final Vertex abAndy = (Vertex) abIt.next();
        assertFalse(abIt.hasNext());

        final JSONArray abOccupationValues = abAndy.getProperty(OCCUPATION_PROPERTY_KEY);

        assertEquals(2, abOccupationValues.length());

        final Set<String> occupations = new HashSet();
        occupations.add(
                abOccupationValues.getJSONObject(0).getJSONObject(VALUE_KEY).getJSONObject(VALUE_KEY).getString(
                        VALUE_KEY));

        occupations.add(
                abOccupationValues.getJSONObject(1).getJSONObject(VALUE_KEY).getJSONObject(VALUE_KEY).getString(
                        VALUE_KEY));

        final Set<String> expected = Sets.newHashSet(OCCUPATION_PROPVALUE_1, OCCUPATION_PROPVALUE_2);

        assertEquals(expected, occupations);

        final RexsterGraph aGraph = new RexsterGraph(
                RexsterTestUtils.getGraphUrl(graphName), 1, aToken, RexsterTestUtils.ANY_NON_BLANK_PASSWORD);

        final Iterable<Vertex> aVertices = aGraph.getVertices();
        final Iterator aIt = aVertices.iterator();
        final Vertex aAndy = (Vertex) aIt.next();
        assertFalse(aIt.hasNext());

        final JSONArray uOccupationValues = aAndy.getProperty(OCCUPATION_PROPERTY_KEY);

        assertEquals(1, uOccupationValues.length());
        assertEquals(
                OCCUPATION_PROPVALUE_1,
                uOccupationValues.getJSONObject(0).getJSONObject(VALUE_KEY).getJSONObject(VALUE_KEY).getString(
                        VALUE_KEY));
    }

    /**
     * Use the binary protocol, RexPro, to read a canned vertex added via methods: {@code
     * addVertexToGraphRexPro(String)}, {@code addVertexToGraphRestGremlin(Graph, String)}, and {@code
     * addVertexToGraphRestNonGremlin()}.  Basically does the same thing as {@code verifyAddedVertexRest()} just via
     * RexPro.
     */
    private void verifyAddedVertexRexPro() throws Exception {
        final byte[] aSession = RexsterTestUtils.initiateSession(aToken, graphName, rexsterClient);

        final ScriptResponseMessage aResponse =
                RexsterTestUtils.makeScriptRequest(aSession, RexsterTestUtils.GET_ALL_VERTICES, rexsterClient);

        final ArrayList<HashMap> list1 = (ArrayList<HashMap>) aResponse.Results.get();
        assertEquals(1, list1.size());
        assertEquals(
                1, ((List) ((Map) list1.get(0).get(BLUEPRINTS_PROPERTIES_KEY)).get(OCCUPATION_PROPERTY_KEY)).size());
        assertEquals(
                OCCUPATION_PROPVALUE_1,
                ((Map) ((List) ((Map) list1.get(0).get(BLUEPRINTS_PROPERTIES_KEY)).get(OCCUPATION_PROPERTY_KEY)).get(0))
                        .get(VALUE_KEY));

        final byte[] abSession = RexsterTestUtils.initiateSession(abToken, graphName, rexsterClient);

        final ScriptResponseMessage abResponse =
                RexsterTestUtils.makeScriptRequest(abSession, RexsterTestUtils.GET_ALL_VERTICES, rexsterClient);

        final ArrayList<HashMap> list2 = (ArrayList<HashMap>) abResponse.Results.get();
        assertEquals(1, list2.size());
        assertEquals(
                2, ((List) ((Map) list2.get(0).get(BLUEPRINTS_PROPERTIES_KEY)).get(OCCUPATION_PROPERTY_KEY)).size());

        final List<Map> occupationValues = (List) ((Map) list2.get(0).get(BLUEPRINTS_PROPERTIES_KEY)).get(
                OCCUPATION_PROPERTY_KEY);

        final Set<String> abOccupationValues = new HashSet<>();
        abOccupationValues.add((String) occupationValues.get(0).get(VALUE_KEY));
        abOccupationValues.add((String) occupationValues.get(1).get(VALUE_KEY));

        final Set<String> expected = Sets.newHashSet(OCCUPATION_PROPVALUE_1, OCCUPATION_PROPVALUE_2);

        assertEquals(
                expected, abOccupationValues);
    }

    /**
     * Makes a session with a 'ab' token (auths set to 'A&B') and adds a vertex to the graph with properties of varying
     * visibility. RexPro is used here to add the vertex to the graph.
     */
    private void addVertexToGraphRexPro(String name) throws Exception {
        final byte[] session = RexsterTestUtils.initiateSession(abToken, graphName, rexsterClient);

        final String script = makeStartingVertexGremlinScript(name);

        RexsterTestUtils.makeScriptRequest(session, script, rexsterClient);
    }

    /**
     * Makes a session with a 'ab' token (auths set to 'A&B') and adds a vertex to the graph with properties of varying
     * visibility. Rexster Rest with Gremlin is used here to add the vertex to the graph.
     */
    private void addVertexToGraphRexsterRestGremlin(RexsterGraph graph, String name) throws Exception {
        final String script = makeStartingVertexGremlinScript(name);

        graph.execute(script);
    }

    /**
     * Adds a vertex via Rexster by using HttpURLConnection.
     */
    private void addVertexToGraphRestNonGremlin() throws IOException, TException {
        String vertexPath = "/vertices/id1?" + URLEncoder.encode(
                String.format(
                        "ezbake_visibility=%s" +
                                "&name=(list," +
                                "((map,(visibility=%s,value=%s)))" +
                                ")" +
                                "&occupation=(list," +
                                "((map,(visibility=%s,value=%s))," +
                                "(map,(visibility=%s,value=%s)))" +
                                ")", aVisibility, aVisibility, CAR_NAME, aVisibility, OCCUPATION_PROPVALUE_1,
                        abVisibility, OCCUPATION_PROPVALUE_2), "UTF-8").replaceAll("%3D", "=").replaceAll("%26", "&");

        final String url = String.format(
                RexsterTestUtils.getGraphUrl(graphName) + "%s", vertexPath);

        final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestProperty(HttpHeaders.AUTHORIZATION, RexsterTestUtils.makeAuthorizationHeader(abToken));
        connection.setRequestMethod("POST");
        assertEquals(200, connection.getResponseCode());
        connection.disconnect();
    }

    /**
     * Gets a Gremlin script that will add a vertex to a graph, with canned properties.
     *
     * @param name A way to vary the canned properties on the vertex added on this script (sets the 'name' property with
     * this value)
     * @return A script that can be executed via Gremlin.
     */
    private String makeStartingVertexGremlinScript(String name) {
        final Map<String, String> propValues1 = new HashMap<>();
        propValues1.put(aVisibility, name);

        final Map<String, String> propValues2 = new HashMap<>();
        propValues2.put(aVisibility, OCCUPATION_PROPVALUE_1);
        propValues2.put(abVisibility, OCCUPATION_PROPVALUE_2);

        final String prop1 = RexsterTestUtils.makeEzPropertyString("name", propValues1);
        final String prop2 = RexsterTestUtils.makeEzPropertyString(OCCUPATION_PROPERTY_KEY, propValues2);

        return RexsterTestUtils.makeAddVertexGremlinRequest("id2", aVisibility, prop1, prop2);
    }

    /**
     * Initializes the RexPro client and RexPro server.
     */
    private void initRexClientAndServers() throws Exception {
        final RexsterProperties rexsterProperties = new RexsterProperties("config/rexster.xml");
        final Properties ezBakeProperties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        final RexsterProperties properties = RexsterPropertiesUtils.combineRexsterEzBakeProperties(
                rexsterProperties, ezBakeProperties);

        RexsterTestUtils.initScriptEngines(rexsterProperties);

        rexproServer = new RexProRexsterServer(properties, true);

        final RexsterApplication rexsterApplication = new SecurityTokenRexsterApplication(ezBakeProperties);
        rexproServer.start(rexsterApplication);

        rexsterClient = RexsterClientFactory.open(RexsterTestUtils.REXSTER_DOMAIN, RexsterTestUtils.REXPRO_PORT);

        httpServer = new HttpRexsterServer(properties);
        httpServer.start(rexsterApplication);
    }

    /**
     * Initializes some tokens and visibilities used in tests. Only formal visibility is manipulated for brevity,
     * although advanced markings is fully supported.
     */
    private void generateEncodedTokensAndVisibilities() throws TException {
        final Visibility ab = new Visibility();
        ab.setFormalVisibility(String.format("%s&%s", RexsterTestUtils.A_MARKING, RexsterTestUtils.B_MARKING));
        this.abVisibility = ThriftUtils.serializeToBase64(ab);
        final Visibility a = new Visibility();
        a.setFormalVisibility(RexsterTestUtils.A_MARKING);
        aVisibility = ThriftUtils.serializeToBase64(a);
        abToken = ThriftUtils.serializeToBase64(
                TestUtils.createTestToken(
                        RexsterTestUtils.A_MARKING, RexsterTestUtils.B_MARKING));
        aToken = ThriftUtils.serializeToBase64(TestUtils.createTestToken(RexsterTestUtils.A_MARKING));
    }
}
