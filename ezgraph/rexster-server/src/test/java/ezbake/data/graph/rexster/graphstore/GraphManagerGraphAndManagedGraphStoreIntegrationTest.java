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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.io.IOUtils;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.impls.rexster.RexsterGraph;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.server.HttpRexsterServer;
import com.tinkerpop.rexster.server.RexProRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterProperties;
import com.tinkerpop.rexster.server.RexsterServer;

import ezbake.base.thrift.Visibility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.data.graph.blueprints.graphmgmt.GraphManagerGraphFilterGraph;
import ezbake.data.graph.rexster.RexsterPropertiesUtils;
import ezbake.data.graph.rexster.RexsterTestUtils;
import ezbake.data.graph.rexster.SecurityTokenRexsterApplication;
import ezbake.data.test.TestUtils;
import ezbake.thrift.ThriftUtils;

/**
 * Tests graph creation with managed graph store. Note that the default configuration is used, meaning {@link
 * ezbake.data.graph.rexster.graphstore.ManagedGraphStore} is used for the graph store and {@link
 * ezbake.data.graph.blueprints.graphmgmt.TinkerGraphManager} is used as the graph manager. These tests run against a
 * specific implementation of GraphStore and GraphManager that rely on a graph management graph for keeping track of
 * available graphs. This is not a requirement but an expected use case.
 * <p/>
 * Some of these tests are designed to work in conjunction with tests from {@link ezbake.data.graph.rexster
 * .VisibilityFilterGraphWrapperIntegrationTest}. These paired tests are run by {@link
 * ezbake.data.graph.rexster.ITRexsterScript}. Specifically, graph creation is done here and writing to that newly
 * created graph is tested more comprehensively in the VisibilityFilterGraphWrapperIntegrationTest. These 'cooperative
 * tests' should be done against a standalone instance of 'ez' Rexster.
 */
public class GraphManagerGraphAndManagedGraphStoreIntegrationTest {

    /**
     * Graph name to use for a newly added graph.
     */
    public static final String MANAGED_GRAPH_NAME = "myNewGraph";

    /**
     * Rexpro server
     */
    private RexsterServer rexproServer;

    /**
     * Rexster HttpServer
     */
    private RexsterServer httpServer;

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
     * Creates a vertex (and a new graph) on the GraphManagerGraph, assuming a RexPro server has started and can be
     * reached.
     *
     * @param args unused
     * @throws Exception if an exception occurs adding a vertex to the GraphManagerGraph or performing a deletion of a
     * graph from the GraphManagerGraph
     */
    public static void main(String... args) throws Exception {
        final GraphManagerGraphAndManagedGraphStoreIntegrationTest tester =
                new GraphManagerGraphAndManagedGraphStoreIntegrationTest();
        tester.generateEncodedTokensAndVisibilities();
        tester.testGraphCreationRexPro();
    }

    /**
     * Once the 'managed graph' has been created per {@code addAndVerifyVertexOnGraphManagerGraph(...)} this method can
     * update that vertex/graph's visibility (remember a 'managed graph' is represented by a vertex on the graph
     * management graph.)
     *
     * @param rexsterClient client to use to make update visibility script request
     * @param abSessionGraphManagerGraph session to make the update request under
     * @param visibility visibility to set the graph/ graph vertex to
     * @throws Exception if an exception occurs executing or sending the request
     */
    private static void updateManagedGraphVisibility(RexsterClient rexsterClient, byte[] abSessionGraphManagerGraph,
            String visibility) throws Exception {
        // setting visibility is allowed (NOTE: element level visibility does not need List<Map> structure,
        //nor delete flags to replace the old value.)
        //TODO: should a visibility update on a graph invalidate other sessions on that graph?
        // graph visibility is only checked when a session is started.
        final String scriptUpdateVisibility =
                String.format("g.v('%s').ezbake_visibility='%s'", MANAGED_GRAPH_NAME, visibility);
        RexsterTestUtils.makeScriptRequest(abSessionGraphManagerGraph, scriptUpdateVisibility, rexsterClient);
    }

    /**
     * Gets the current set of vertices from a session's graph, limited by the permissions of the token given to the
     * session request.
     *
     * @param session session in which to get vertices
     * @param rexsterClient client to use to get the list
     * @return a list of vertices in the graph associated with the given session
     * @throws Exception if an exception occurs make the request of RexPro
     */
    private static ArrayList<HashMap> graphVertexList(byte[] session, RexsterClient rexsterClient) throws Exception {
        return (ArrayList<HashMap>) RexsterTestUtils.makeScriptRequest(
                session, RexsterTestUtils.GET_ALL_VERTICES, rexsterClient).Results.get();
    }

    @Before
    public void setUp() throws Exception {
        initRexServers();

        generateEncodedTokensAndVisibilities();
    }

    @Test
    public void testGraphCreationRexPro() throws Exception {
        final RexsterClient rexsterClient =
                RexsterClientFactory.open(RexsterTestUtils.REXSTER_DOMAIN, RexsterTestUtils.REXPRO_PORT);

        // open the GraphManagerGraph
        final byte[] abSessionGraphManagerGraph = RexsterTestUtils.initiateSession(
                abToken, GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME, rexsterClient);

        // add a new graph vertex and verify that it has been added. This should trigger the creation of a new graph,
        // with the same name as the newly created vertex id.
        addAndVerifyVertexOnGraphManagerGraph(rexsterClient, abSessionGraphManagerGraph, abVisibility);

        // check that sessions with the appropriate auths can be opened on graph created in 'addAndVerify...' above.
        tryOpeningSessionOnNewGraphWithABVisibility(rexsterClient);

        // update the visibility on the newly created graph.
        // TODO: do we need to invalidate sessions if this is updated?
        updateManagedGraphVisibility(rexsterClient, abSessionGraphManagerGraph, aVisibility);

        // add, delete a graph via the GraphManagerGraph, and add a vertex to that graph before it is deleted.
        addGraphAddVertexDeleteGraph(rexsterClient, abSessionGraphManagerGraph);

        rexsterClient.close();
    }

    @Test
    public void testGraphCreationAndGetGraphNamesRest() throws Exception {
        // post the new graph vertex to the GraphManagerGraph
        final String url = RexsterTestUtils.getGraphUrl(GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME) + String
                .format("/vertices/%s?ezbake_visibility=%s", MANAGED_GRAPH_NAME, abVisibility);

        final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setRequestProperty(HttpHeaders.AUTHORIZATION, RexsterTestUtils.makeAuthorizationHeader(abToken));
        connection.setRequestMethod("POST");
        assertEquals(200, connection.getResponseCode());
        connection.disconnect();

        // assert that the graph can be accessed
        final RexsterGraph graph = new RexsterGraph(
                RexsterTestUtils.getGraphUrl(MANAGED_GRAPH_NAME), 1, abToken, RexsterTestUtils.ANY_NON_BLANK_PASSWORD);

        // make a call on it to further check that it worked
        graph.getVertices();

        final String getGraphsUrl =
                String.format("http://%s:%s/graphs", RexsterTestUtils.REXSTER_DOMAIN, RexsterTestUtils.REXSTER_PORT);

        // Test get available graph names after we've added a graph above.
        final HttpURLConnection getGraphsConnection = (HttpURLConnection) new URL(getGraphsUrl).openConnection();
        getGraphsConnection
                .setRequestProperty(HttpHeaders.AUTHORIZATION, RexsterTestUtils.makeAuthorizationHeader(abToken));
        getGraphsConnection.setRequestMethod("GET");
        assertEquals(200, getGraphsConnection.getResponseCode());

        final InputStream in = getGraphsConnection.getInputStream();
        final String result = IOUtils.toString(in);

        getGraphsConnection.disconnect();

        final JSONObject jsonResult = new JSONObject(result);
        final JSONArray graphs = jsonResult.getJSONArray("graphs");

        assertEquals(2, graphs.length());
        final Set<Object> graphNames = Sets.newHashSet(graphs.get(0), graphs.get(1));

        assertEquals(
                Sets.newHashSet(GraphManagerGraphFilterGraph.GRAPH_MANAGER_GRAPH_NAME, MANAGED_GRAPH_NAME), graphNames);
    }

    @After
    public void tearDown() throws Exception {
        rexproServer.stop();
        httpServer.stop();
    }

    /**
     * Adds a graph, then adds a vertex to the new graph, then deletes that graph.
     *
     * @param rexsterClient client to use to make script requests
     * @param abSessionGraphManagerGraph session to make the requests under
     * @throws Exception if an exception occurs making requests
     */
    private void addGraphAddVertexDeleteGraph(RexsterClient rexsterClient, byte[] abSessionGraphManagerGraph)
            throws Exception {
        // add the graph that will be deleted to the GraphManagerGraph
        final String graphToBeDeleted = "deleteMe";
        final String addGraphToDeleteScript =
                RexsterTestUtils.makeAddVertexGremlinRequest(graphToBeDeleted, abVisibility);

        // reuse GraphManagerGraph session from before to make the add graph script request
        RexsterTestUtils.makeScriptRequest(abSessionGraphManagerGraph, addGraphToDeleteScript, rexsterClient);

        // confirm the graph was added
        final ArrayList<HashMap> list2 = graphVertexList(abSessionGraphManagerGraph, rexsterClient);
        assertEquals(2, list2.size());

        // add a vertex to the newly created graph before we delete it, to verify it works.
        final String addVertexScript = RexsterTestUtils.makeAddVertexGremlinRequest("aVertexId", abVisibility);
        final byte[] graphToBeDeletedSession =
                RexsterTestUtils.initiateSession(abToken, graphToBeDeleted, rexsterClient);
        RexsterTestUtils.makeScriptRequest(graphToBeDeletedSession, addVertexScript, rexsterClient);

        // delete the graph
        final String deleteScript = String.format("g.v(\"%s\").remove()", graphToBeDeleted);
        RexsterTestUtils.makeScriptRequest(abSessionGraphManagerGraph, deleteScript, rexsterClient);

        // confirm the graph was deleted
        final ArrayList<HashMap> list3 = graphVertexList(abSessionGraphManagerGraph, rexsterClient);
        assertEquals(1, list3.size());
        assertEquals(MANAGED_GRAPH_NAME, list3.get(0).get("_id"));
    }

    /**
     * Try opening sessions on a managed graph.  Assumes the graph vertex has been assigned a visibility with formal
     * visibility "A&B".
     *
     * @param rexsterClient client to use to make script requests
     * @throws Exception if the request failed or there was a problem sending the request
     */
    private void tryOpeningSessionOnNewGraphWithABVisibility(RexsterClient rexsterClient) throws Exception {
        // Fail to get session without auths for graph.
        try {
            RexsterTestUtils.initiateSession(aToken, MANAGED_GRAPH_NAME, rexsterClient);
            fail("Able to initiate an invalid session.");
        } catch (final RuntimeException e) {
            //eat exception, expected.
        }

        //Succeed in getting session with appropriate auths.
        RexsterTestUtils.initiateSession(abToken, MANAGED_GRAPH_NAME, rexsterClient);
    }

    /**
     * Create a new vertex on the graph management graph and verify that it has been created.  Importantly: it is
     * expected when using {@link ManagedGraphStore} with this particular GraphManager that a graph named "manage" will
     * be already available (tested here) and that a graph will be created when a vertex is added to "manage".
     *
     * @param rexsterClient client to use to make script requests
     * @param abSessionGraphManagerGraph session to make the requests under
     * @param visibility visibility to set the graph/ graph vertex to
     * @throws Exception if there is an exception while making the script request
     */
    private void addAndVerifyVertexOnGraphManagerGraph(RexsterClient rexsterClient, byte[] abSessionGraphManagerGraph,
            String visibility) throws Exception {
        final String addNewGraphVertexScript =
                RexsterTestUtils.makeAddVertexGremlinRequest(MANAGED_GRAPH_NAME, visibility);

        RexsterTestUtils.makeScriptRequest(abSessionGraphManagerGraph, addNewGraphVertexScript, rexsterClient);

        final ArrayList<HashMap> list1 = graphVertexList(abSessionGraphManagerGraph, rexsterClient);
        assertEquals(1, list1.size());
        assertEquals(MANAGED_GRAPH_NAME, list1.get(0).get("_id"));
    }

    /**
     * Initializes the RexPro client and RexPro server.
     */
    private void initRexServers() throws Exception {
        final RexsterProperties rexsterProperties = new RexsterProperties("config/rexster.xml");
        final Properties ezBakeProperties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        final RexsterProperties properties = RexsterPropertiesUtils.combineRexsterEzBakeProperties(
                rexsterProperties, ezBakeProperties);

        RexsterTestUtils.initScriptEngines(rexsterProperties);
        rexproServer = new RexProRexsterServer(properties, true);

        final RexsterApplication rexsterApplication = new SecurityTokenRexsterApplication();
        rexproServer.start(rexsterApplication);
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

        ab.setFormalVisibility(RexsterTestUtils.A_MARKING);
        this.aVisibility = ThriftUtils.serializeToBase64(ab);

        abToken = ThriftUtils
                .serializeToBase64(TestUtils.createTestToken(RexsterTestUtils.A_MARKING, RexsterTestUtils.B_MARKING));
        aToken = ThriftUtils.serializeToBase64(TestUtils.createTestToken(RexsterTestUtils.A_MARKING));
    }
}
