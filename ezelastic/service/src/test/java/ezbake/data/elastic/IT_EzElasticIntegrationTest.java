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

package ezbake.data.elastic;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static ezbake.data.elastic.common.ElasticUtils.PERCOLATOR_TYPE;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceFilterBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.TokenType;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.elastic.test.TestUtils;
import ezbake.data.elastic.test.models.Location;
import ezbake.data.elastic.test.models.PlaceOfInterest;
import ezbake.data.elastic.thrift.Document;
import ezbake.data.elastic.thrift.EzElastic;
import ezbake.data.elastic.thrift.Facet;
import ezbake.data.elastic.thrift.FacetRequest;
import ezbake.data.elastic.thrift.FacetResult;
import ezbake.data.elastic.thrift.IndexResponse;
import ezbake.data.elastic.thrift.Page;
import ezbake.data.elastic.thrift.PercolateQuery;
import ezbake.data.elastic.thrift.PercolateRequest;
import ezbake.data.elastic.thrift.Query;
import ezbake.data.elastic.thrift.RangeFacetEntry;
import ezbake.data.elastic.thrift.ScriptParam;
import ezbake.data.elastic.thrift.SearchResult;
import ezbake.data.elastic.thrift.TermsFacetEntry;
import ezbake.data.elastic.thrift.TermsScriptFacet;
import ezbake.data.elastic.thrift.TermsStatsFacet;
import ezbake.data.elastic.thrift.TermsStatsFacetResultEntry;
import ezbake.data.elastic.thrift.TermsStatsValue;
import ezbake.data.elastic.thrift.ValueScript;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftServerPool;
import ezbake.thrift.ThriftTestUtils;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.elasticsearch.ElasticsearchConfigurationHelper;

@SuppressWarnings("StaticNonFinalField")
public final class IT_EzElasticIntegrationTest {
    private static final String TEST_TYPE = "sample";
    private static final String SERVICE_NAME = "EzElastic";
    private static final String MOCK_SECURITY_ID = "mockSecurityId";
    private static final int PORT_NUMBER = 23471;

    private static String applicationName;
    private static Properties configuration;
    private static Gson gson;
    private static EsSetup esSetup;
    private static EzSecurityToken fakeSecurity;
    private static String securityId;

    private ThriftClientPool pool;
    private ThriftServerPool servers;
    private Document jeffersonMemorialDoc;
    private Document whiteHouseDoc;
    private Document columbiaDoc;
    private Document lincolnMemorialDoc;
    private PlaceOfInterest whiteHouse;
    private PlaceOfInterest columbia;
    private PlaceOfInterest lincolnMemorial;
    private PlaceOfInterest jeffersonMemorial;

    private EzElastic.Client client;

    @BeforeClass
    public static void setUpClass() throws Exception {
        gson = new GsonBuilder().setDateFormat("ddHHmm'Z' MM yy").create();
        configuration = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();

        final EzBakeApplicationConfigurationHelper appConfig = new EzBakeApplicationConfigurationHelper(configuration);

        securityId = appConfig.getSecurityID();
        fakeSecurity =
                ThriftTestUtils.generateTestSecurityToken(securityId, securityId, Collections.singletonList("U"));
        applicationName = appConfig.getApplicationName();
        configuration.setProperty(EzBakePropertyConstants.ELASTICSEARCH_CLUSTER_NAME, "elasticsearch");
        final ElasticsearchConfigurationHelper helper = new ElasticsearchConfigurationHelper(configuration);

        final Settings settings = ImmutableSettings.settingsBuilder()
                .put("script.native.visibility.type", "ezbake.data.elastic.security.EzSecurityScriptFactory")
                .put("cluster.name", helper.getElasticsearchClusterName()).put("script.disable_dynamic", false)
                .put("network.host", helper.getElasticsearchHost())
                .put("transport.tcp.port", helper.getElasticsearchPort()).put("node.local", false).build();

        esSetup = new EsSetup(settings);
        esSetup.execute(EsSetup.deleteAll());

        if (esSetup.client() == null) {
            throw new Exception("Could not start EsSetup node!");
        }
    }

    private static String getMappingForTest() throws IOException {
        final XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject(TEST_TYPE);
        mapping.startObject("properties");

        mapping.startObject("title");
        mapping.field("type", "string");
        mapping.field("stored", "yes");
        mapping.endObject();

        mapping.startObject("comments");
        mapping.field("type", "string");
        mapping.endObject();

        mapping.startObject("location");
        mapping.field("type", "geo_point");
        mapping.endObject();

        mapping.startObject("rating");
        mapping.field("type", "integer");
        mapping.field("stored", "yes");
        mapping.endObject();

        mapping.startObject("visit");
        mapping.field("type", "date");
        mapping.field("format", "ddHHmm'Z' MM yy");
        mapping.field("stored", "yes");
        mapping.endObject();

        mapping.endObject();
        mapping.endObject();
        mapping.endObject();

        return mapping.string();
    }

    private static void deleteIndex(String name) {
        if (esSetup.exists(name)) {
            esSetup.execute(EsSetup.deleteIndex(name));
        }
    }

    private static void createIndexProbably(String name) {
        if (!esSetup.exists(name)) {
            esSetup.execute(EsSetup.createIndex(name));
        }
    }

    @Before
    public void setUp() throws Exception {
        // Start up thrift server
        servers = new ThriftServerPool(configuration, PORT_NUMBER);
        // Now start the service supplying the service implementation and the service name
        servers.startApplicationService(new EzElasticHandler(), SERVICE_NAME, applicationName, MOCK_SECURITY_ID);

        // Still create our client pool like we would for real
        pool = new ThriftClientPool(configuration);

        deleteIndex(applicationName);
        loadSamples();
    }

    @After
    public void tearDown() throws Exception {
        servers.shutdown();
        pool.clearPool();
        pool.close();
    }

    @Test
    public void testPut() throws Exception {
        // Prepare: Setup Thrift Client
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);

        // Action: Index single document
        final IndexResponse response = client.put(whiteHouseDoc, fakeSecurity);

        // Assert: IndexResponse values should match initial supplied values
        assertEquals(whiteHouseDoc.get_id(), response.get_id());
        assertEquals(1, response.get_version());
        assertEquals(TEST_TYPE, response.get_type());

        pool.returnToPool(client);
    }

    @Test
    public void testGet() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Retrieve single document by id
        final Document result = client.get(whiteHouseDoc.get_id(), fakeSecurity);

        // Assert: Document contents should match the expected contents
        final PlaceOfInterest fromDataset = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
        assertEquals(whiteHouse.getTitle(), fromDataset.getTitle());
        assertEquals(whiteHouse.getComments(), fromDataset.getComments());
        assertEquals(whiteHouse.getRating(), fromDataset.getRating());
        assertEquals(whiteHouseDoc.get_id(), result.get_id());

        pool.returnToPool(client);
    }

    @Test
    public void testBulkGet() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        final List<Document> twentyDocs = new ArrayList<>();
        final Set<String> ids = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            final Document doc = TestUtils.generateDocument("BulkDoc", "{}");
            doc.set_id(Integer.toString(i));
            ids.add(Integer.toString(i));
            twentyDocs.add(doc);
        }
        client.bulkPut(twentyDocs, fakeSecurity);

        final List<Document> results = client.bulkGetWithType(ids, "BulkDoc", fakeSecurity);

        // Assert: We should see all the ids come back exactly once
        for (final Document doc : results) {
            assertTrue(ids.contains(doc.get_id()));
            ids.remove(doc.get_id());
        }
        pool.returnToPool(client);
    }

    @Test
    public void testGetNonExistantId() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Retrieve single document by id with a non-existent id
        final Document result = client.get(UUID.randomUUID().toString(), fakeSecurity);

        // Assert: Document should be empty as it doesn't exist
        assertEquals(StringUtils.EMPTY, result.get_type());
        assertEquals(StringUtils.EMPTY, result.get_jsonObject());
        assertNull(result.getVisibility().getFormalVisibility());

        pool.returnToPool(client);
    }

    @Test
    public void testGetWithType() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Retrieve single document by id
        final Document result = client.getWithType(columbiaDoc.get_id(), TEST_TYPE, fakeSecurity);

        // Assert: Document contents should match the expected contents
        final PlaceOfInterest fromDataset = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
        assertEquals(columbia.getTitle(), fromDataset.getTitle());
        assertEquals(columbia.getComments(), fromDataset.getComments());
        assertEquals(columbia.getRating(), fromDataset.getRating());
        assertEquals(columbiaDoc.get_id(), result.get_id());

        pool.returnToPool(client);
    }

    @Test
    public void testGetWithInvalidType() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Retrieve single document by id
        final Document result = client.getWithType(lincolnMemorialDoc.get_id(), "fake_type", fakeSecurity);

        // Assert: Document should be empty since it doesn't exist
        assertEquals(StringUtils.EMPTY, result.get_type());
        assertEquals(StringUtils.EMPTY, result.get_jsonObject());
        assertNull(result.getVisibility().getFormalVisibility());

        pool.returnToPool(client);
    }

    @Test
    public void testGetWithFields() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Retrieve rating, visit, and title from single document by id
        final Document result = client.getWithFields(
                columbiaDoc.get_id(), TEST_TYPE, ImmutableSet.of("title", "rating"), fakeSecurity);

        final Map<String, Object> resultMap = TestUtils.jsonToMap(result.get_jsonObject());

        // Assert: Document contents should match the expected contents
        assertEquals(2, resultMap.size());
        assertTrue(resultMap.containsKey("title"));
        assertTrue(resultMap.containsKey("rating"));
        assertEquals(columbia.getTitle(), resultMap.get("title"));
        assertEquals(columbia.getRating(), Integer.parseInt(resultMap.get("rating").toString()));

        pool.returnToPool(client);
    }

    @Test
    public void testQuery() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Create and execute range query on rating
        final String elasticQuery = QueryBuilders.rangeQuery("rating").gt(20).lte(100).toString();
        final SearchResult results = client.query(new Query(elasticQuery), fakeSecurity);

        // Assert: There should be 3 matching documents
        final List<String> expectedIds =
                Arrays.asList(columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }

        pool.returnToPool(client);
    }

    @Test
    public void testQueryWithIndex() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Create and execute an AND match query on title
        final String elasticQuery =
                QueryBuilders.matchQuery("title", "Columbia").operator(MatchQueryBuilder.Operator.AND).toString();
        final SearchResult results = client.query(new Query(elasticQuery), fakeSecurity);

        // Assert: There should be a single matching document due to the AND operator
        assertEquals(1, results.getTotalHits());
        assertEquals(columbiaDoc.get_id(), results.getMatchingDocuments().get(0).get_id());

        pool.returnToPool(client);
    }

    @Test
    public void testQueryWithType() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Create and execute an OR match query on title
        final Query query = new Query(QueryBuilders.matchQuery("title", "memorial").toString());
        query.setType(TEST_TYPE);
        final SearchResult results = client.query(query, fakeSecurity);

        // Assert: There should be 2 matching documents
        final List<String> expectedIds = Arrays.asList(jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }

        pool.returnToPool(client);
    }

    @Test
    public void testDeleteById() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.put(whiteHouseDoc, fakeSecurity);

        // Action: Delete by id and then try to retrieve document
        client.deleteById(whiteHouseDoc.get_id(), fakeSecurity);
        final Document result = client.get(whiteHouseDoc.get_id(), fakeSecurity);

        // Assert: Document should be empty since it doesn't exist
        assertEquals(StringUtils.EMPTY, result.get_type());
        assertEquals(StringUtils.EMPTY, result.get_jsonObject());
        assertNull(result.getVisibility().getFormalVisibility());

        pool.returnToPool(client);
    }

    @Test
    public void testCountByType() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Create and execute an OR match query on title
        final long result = client.countByType(ImmutableSet.of(TEST_TYPE), fakeSecurity);

        // Assert: There should be 2 matching documents
        assertEquals(4, result);

        pool.returnToPool(client);
    }

    @Test
    public void testRangeDateFacets() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        final SimpleDateFormat dtg = new SimpleDateFormat("ddHHmm'Z' MM yy");
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Create and execute a match all query with date bucket facets
        final Query query = new Query(QueryBuilders.matchAllQuery().toString());
        query.setType(TEST_TYPE).setFacets(Collections.singletonList(TestUtils.generateDateBucketFacet()))
                .setPage(new Page(0, (short) 10));
        final SearchResult results = client.query(query, fakeSecurity);

        // Assert: All records should be a match and the facet buckets should be organized as expected
        assertEquals(4, results.getTotalHits());
        assertFalse(results.getFacets().isEmpty());
        assertTrue(results.getFacets().containsKey("Report Date"));
        final FacetResult facetResult = results.getFacets().get("Report Date");
        for (final RangeFacetEntry entry : facetResult.getRangeFacetResult().getEntries()) {
            if (dtg.parse(entry.getFrom()).getTime() >= TestUtils.last24time
                    || dtg.parse(entry.getFrom()).getTime() >= TestUtils.last48time
                    || dtg.parse(entry.getFrom()).getTime() >= TestUtils.last72time) {
                assertEquals(3, entry.getCount());
            } else {
                assertEquals(4, entry.getCount());
            }
        }

        pool.returnToPool(client);
    }

    @Test(timeout = 60000)
    public void testNonExistantIndexQuery() throws Exception {
        deleteIndex(applicationName);
        if (esSetup.exists(applicationName)) {
            fail();
        }

        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        final Query query = new Query(QueryBuilders.matchAllQuery().toString()).setType(TEST_TYPE);
        final SearchResult results = client.query(query, fakeSecurity);

        assertEquals(0, results.getMatchingDocuments().size());
        assertEquals(QueryBuilders.matchAllQuery().toString(), results.actualQuery);
        pool.returnToPool(client);
    }

    @Test
    public void testExistingIndex() throws Exception {
        createIndexProbably(applicationName);

        // This will attempt to create an index testapp_v1 and alias it to testapp; this should not
        // throw an exception if there is already an index called testapp (and will not create the alias).
        final EzElasticHandler handler = new EzElasticHandler();
        handler.setConfigurationProperties(configuration);
        handler.init();

        assertFalse(esSetup.exists(String.format("%s_v1", applicationName)));
        assertTrue(esSetup.exists(applicationName));
    }

    @Test
    public void testQueryWithFilter() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "memorial"))
                .toString();

        // Action: Create and execute a match all query with date bucket facets
        final Query query =
                new Query(QueryBuilders.matchAllQuery().toString()).setType(TEST_TYPE).setFilterJson(filter);
        final SearchResult results = client.query(query, fakeSecurity);

        // Assert: All records should be a match and the facet buckets should be organized as expected
        final List<String> expectedIds = Arrays.asList(jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }

        pool.returnToPool(client);
    }

    @Test
    public void testQueryWithElasticJsonAndFilter() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "columbia"))
                .toString();
        final String queryJson = QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("rating").from(35).to(98).includeLower(true).includeLower(true))
                .must(QueryBuilders.matchQuery("comments", "columbia")).mustNot(QueryBuilders.prefixQuery("title", "W"))
                .toString();

        // Action: Create and execute a match all query with date bucket facets
        final Query query =
                new Query(queryJson).setType(TEST_TYPE).setPage(new Page(0, (short) 10)).setFilterJson(filter);
        final SearchResult results = client.query(query, fakeSecurity);

        // Assert: All records should be a match and the facet buckets should be organized as expected
        final List<String> expectedIds = Collections.singletonList(columbiaDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }

        pool.returnToPool(client);
    }

    @Test
    public void testTermScript() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        final TermsScriptFacet tsf = new TermsScriptFacet();
        tsf.setFields(Collections.singletonList("title"));
        tsf.setScript(new ValueScript("term == 'columbia' ? 'Company' : 'Location'", new ArrayList<ScriptParam>()));
        final FacetRequest request = new FacetRequest();
        request.setTermsScriptFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);

        // Action: Create and execute a match all query with date bucket facets
        final Query query = new Query(QueryBuilders.matchAllQuery().toString()).setType(TEST_TYPE).setFacets(
                Collections.singletonList(facet)).setPage(new Page(0, (short) 10));
        final SearchResult results = client.query(query, fakeSecurity);

        // Assertion
        assertEquals(4, results.getTotalHits());
        int locationCount = 0;
        int companyCount = 0;

        for (final Document result : results.getMatchingDocuments()) {
            final PlaceOfInterest fromJson = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
            for (final String val : fromJson.getTitle().split(" ")) {
                if ("columbia".equalsIgnoreCase(val)) {
                    companyCount++;
                } else {
                    locationCount++;
                }
            }
        }

        for (final TermsFacetEntry facetResult : results.getFacets().get("magic").getTermsFacetResult().getEntries()) {
            if ("company".equalsIgnoreCase(facetResult.getTerm())) {
                assertEquals(companyCount, facetResult.getCount());
            } else {
                assertEquals(locationCount, facetResult.getCount());
            }
        }

        pool.returnToPool(client);
    }

    @Test
    public void testTermsStatsStringFacet() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        final TermsStatsValue val = new TermsStatsValue();
        val.setValueField("rating");
        final TermsStatsFacet tsf = new TermsStatsFacet();
        tsf.setKeyField("title");
        tsf.setValueField(val);
        final FacetRequest request = new FacetRequest();
        request.setTermsStatsFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);

        final Query query = new Query(QueryBuilders.matchAllQuery().toString()).setType(TEST_TYPE).setFacets(
                Collections.singletonList(facet)).setPage(new Page(0, (short) 10));
        final SearchResult results = client.query(query, fakeSecurity);

        // Assertion
        assertEquals(4, results.getTotalHits());
        for (final TermsStatsFacetResultEntry facetResult : results.getFacets().get("magic").getTermsStatsFacetResult()
                .getEntries()) {
            assertNotEquals(facetResult.getTerm(), Integer.toString((int) facetResult.getTermAsNumber()));
        }

        pool.returnToPool(client);
    }

    @Test
    public void testTermsStatsDoubleFacet() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        final TermsStatsValue val = new TermsStatsValue();
        val.setValueField("rating");
        final TermsStatsFacet tsf = new TermsStatsFacet();
        tsf.setKeyField("rating");
        tsf.setValueField(val);
        final FacetRequest request = new FacetRequest();
        request.setTermsStatsFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);

        final Query query = new Query(QueryBuilders.matchAllQuery().toString()).setType(TEST_TYPE).setFacets(
                Collections.singletonList(facet)).setPage(new Page(0, (short) 10));
        final SearchResult results = client.query(query, fakeSecurity);

        // Assertion
        assertEquals(4, results.getTotalHits());
        for (final TermsStatsFacetResultEntry facetResult : results.getFacets().get("magic").getTermsStatsFacetResult()
                .getEntries()) {
            assertEquals(facetResult.getTerm(), Integer.toString((int) facetResult.getTermAsNumber()));
        }

        pool.returnToPool(client);
    }

    @Test
    public void testTermsStatsFacetWithFilter() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc, lincolnMemorialDoc), fakeSecurity);

        // Action: Filter will match two but query should only match one due to rating range of query.
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "memorial"))
                .toString();

        final TermsStatsValue val = new TermsStatsValue();
        val.setValueField("rating");
        final TermsStatsFacet tsf = new TermsStatsFacet();
        tsf.setKeyField("title");
        tsf.setValueField(val);
        final FacetRequest request = new FacetRequest();
        request.setTermsStatsFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);
        facet.setFilterJSON(filter);

        final Query query = new Query(QueryBuilders.matchAllQuery().toString());
        query.setType(TEST_TYPE);
        query.setFacets(Collections.singletonList(facet));
        query.setPage(new Page(0, (short) 10));
        final SearchResult results = client.query(query, fakeSecurity);

        // Assertion
        assertEquals(4, results.getTotalHits());
        final Set<String> uniqueTitleTerms = new HashSet<>();
        uniqueTitleTerms.addAll(Arrays.asList(lincolnMemorial.getTitle().split(" ")));
        uniqueTitleTerms.addAll(Arrays.asList(jeffersonMemorial.getTitle().split(" ")));

        assertEquals(
                uniqueTitleTerms.size(),
                results.getFacets().get("magic").getTermsStatsFacetResult().getEntries().size());

        pool.returnToPool(client);
    }

    @Test
    public void testPercolatorVisibility() throws Exception{
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);
        Document jeffersonMemorialDocWithVisibility = jeffersonMemorialDoc;

        jeffersonMemorialDocWithVisibility.setVisibility(new Visibility().setFormalVisibility("EF&AB&CD&(USA|GBR)"));
        List<IndexResponse> response = client.bulkPut(
                Arrays.asList(jeffersonMemorialDocWithVisibility), fakeSecurity);

        assertEquals(1, response.size());

        IndexResponse percolateResponse = populatePercolate(fakeSecurity,"U",null);
        assertTrue(percolateResponse.isSuccess());

        // Assert: There should be 0 matching hits
        jeffersonMemorialDocWithVisibility.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches = client.percolate(
                Arrays.asList(
                        jeffersonMemorialDocWithVisibility), fakeSecurity);
        assertEquals(0, matches.size());

        final EzSecurityToken exampleSecurity =
                ThriftTestUtils.generateTestSecurityToken(securityId, securityId,
                        Arrays.asList("U","S", "AB", "CD", "EF", "USA"));

        IndexResponse percolateResponse2 = populatePercolate(exampleSecurity,
                "EF&AB&CD&(USA)",exampleSecurity.getAuthorizations());
        assertTrue(percolateResponse2.isSuccess());

        IndexResponse percolateResponse3 = populatePercolate(exampleSecurity,
                "U",fakeSecurity.getAuthorizations());
        assertTrue(percolateResponse3.isSuccess());

        // Assert: There should be 1 matching hits
        jeffersonMemorialDocWithVisibility.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches2 = client.percolate(
                Arrays.asList(
                        jeffersonMemorialDocWithVisibility), exampleSecurity);
        assertEquals(1, matches2.size());

        jeffersonMemorialDocWithVisibility.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches3 = client.percolate(
                Arrays.asList(
                        jeffersonMemorialDocWithVisibility), fakeSecurity);
        assertEquals(0, matches3.size());

        // Expect an exception when trying to set authorizations higher than the token passed
        try{
            populatePercolate(fakeSecurity,"U",exampleSecurity.getAuthorizations());
            fail();
        } catch (TException e){
            //Expected
        }

        // Assert: There should be 1 matching hit because exampleSecurity has the proper auths
        jeffersonMemorialDocWithVisibility.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches4 = client.percolateByIds(
                Arrays.asList(
                        jeffersonMemorialDocWithVisibility.get_id()),jeffersonMemorialDocWithVisibility.get_type(),
                        10, exampleSecurity);
        assertEquals(1, matches4.size());


        // Assert: There should be 0 matching hits because fakeSecurity does not have the proper auths
        jeffersonMemorialDocWithVisibility.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches5 = client.percolateByIds(
                Arrays.asList(
                        jeffersonMemorialDocWithVisibility.get_id()),jeffersonMemorialDocWithVisibility.get_type(),
                        10, fakeSecurity);
        assertEquals(0, matches5.size());


        /* Assert: There should be 1 matching hit because appfakeSecurity is an app token
         * Note: In practice an app will never have access to documents above their auths because they each have their
         * own ElasticSearch index.
         */
        EzSecurityToken appfakeSecurity = fakeSecurity.deepCopy();
        appfakeSecurity.setType(TokenType.APP);
        jeffersonMemorialDocWithVisibility.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches6 = client.percolateByIds(
                Arrays.asList(
                        jeffersonMemorialDocWithVisibility.get_id()),jeffersonMemorialDocWithVisibility.get_type(),
                10, appfakeSecurity);
        assertEquals(1, matches6.size());
    }

    @Test
    public void testPercolator() throws Exception {
        // Prepare: Setup Thrift Client and add some docs
        client = pool.getClient(SERVICE_NAME, EzElastic.Client.class);
        client.setTypeMapping(TEST_TYPE, getMappingForTest(), fakeSecurity);

        List<IndexResponse> response = client.bulkPut(
                Arrays.asList(jeffersonMemorialDoc, whiteHouseDoc, columbiaDoc,
                        lincolnMemorialDoc), fakeSecurity);

        final FilterBuilder geoDistanceFilter =
                new GeoDistanceFilterBuilder(TEST_TYPE + ".location").distance(20, DistanceUnit.KILOMETERS)
                        .lat(whiteHouse.getLocation().getLat()).lon(whiteHouse.getLocation().getLon());

        final QueryBuilder filteredQuery = new FilteredQueryBuilder(QueryBuilders.matchAllQuery(), geoDistanceFilter);

        final PercolateQuery percolator = new PercolateQuery();

        final Visibility visibility = new Visibility().setFormalVisibility("U");
        percolator.setVisibility(visibility);

        final String queryDoc = jsonBuilder().startObject().field("query", filteredQuery).endObject().string();

        percolator.setQueryDocument(queryDoc);

        // Action: Create and execute an OR match query on title
        final IndexResponse percolateResponse = client.addPercolateQuery(percolator, fakeSecurity);
        assertTrue(percolateResponse.isSuccess());

        final QueryBuilder filteredQuery2 = new FilteredQueryBuilder(QueryBuilders.matchAllQuery(), geoDistanceFilter);
        final String queryDoc2 = jsonBuilder().startObject().field("query", filteredQuery2).endObject().string();

        final PercolateQuery percolator2 = new PercolateQuery();

        final Visibility visibility2 = new Visibility().setFormalVisibility("U");
        percolator2.setVisibility(visibility2);
        percolator2.setQueryDocument(queryDoc2);
        final IndexResponse percolateResponse2 = client.addPercolateQuery(percolator2, fakeSecurity);

        // Assert: There should be 6 matching hits
        lincolnMemorialDoc.setPercolate(new PercolateRequest());
        columbiaDoc.setPercolate(new PercolateRequest());
        whiteHouseDoc.setPercolate(new PercolateRequest());
        jeffersonMemorialDoc.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches = client.percolate(
                Arrays.asList(
                        lincolnMemorialDoc, columbiaDoc, whiteHouseDoc,
                        jeffersonMemorialDoc), fakeSecurity);
        assertEquals(6, matches.size());

        final IndexResponse percolateResponse3 = client.addPercolateQuery(percolator2.setId("NotThis"), fakeSecurity);
        final IndexResponse percolateResponse4 =
                client.addPercolateQuery(percolator2.setId("HopeFullyNotThis"), fakeSecurity);
        final IndexResponse percolateResponse5 =
                client.addPercolateQuery(percolator2.setId("HopeFullyNotThis2"), fakeSecurity);
        final IndexResponse percolateResponse6 =
                client.addPercolateQuery(percolator2.setId("HopeFullyNotThis3"), fakeSecurity);


        String queryJson = QueryBuilders.matchAllQuery().toString();
        Query query = new Query(queryJson);
        query.setType(PERCOLATOR_TYPE);
        XContentBuilder obj = jsonBuilder();
        obj.startObject();
        obj.startObject("type");
        obj.field("value", PERCOLATOR_TYPE);
        obj.endObject();
        obj.endObject();
        obj.close();
        query.setFilterJson(obj.string());

        final SearchResult results2 = client.query(query, fakeSecurity);
        List<IndexResponse> response4 = client.bulkPut(results2.getMatchingDocuments(), fakeSecurity);

        obj = jsonBuilder();
        obj.startObject();
        obj.startObject("ids");
        obj.field("type", PERCOLATOR_TYPE);
        obj.startArray("values");
        obj.field(percolateResponse.get_id());
        obj.endArray();
        obj.endObject();
        obj.endObject();
        obj.close();
        query.setFilterJson(obj.string());
        final SearchResult results = client.query(query, fakeSecurity);
        assertEquals(1,results.getMatchingDocumentsSize());
        assertEquals(percolateResponse.get_id(),results.getMatchingDocuments().get(0).get_id());

        Query newquery = new Query(queryJson);
        obj = jsonBuilder();
        obj.startObject();
        obj.startObject("regexp");
        obj.field("_id", ".*");
        obj.endObject();
        obj.endObject();
        obj.close();
        newquery.setSearchString(obj.string());
        newquery.setType(PERCOLATOR_TYPE);
        final SearchResult resultsQuery = client.queryPercolate(newquery, fakeSecurity);
        assertEquals(6, resultsQuery.getMatchingDocuments().size());

        boolean deleteResponse = client.removePercolateQuery(percolateResponse.get_id(), fakeSecurity);
        assertTrue(deleteResponse);

        final SearchResult resultsQuery2 = client.queryPercolate(newquery, fakeSecurity);
        assertEquals(5, resultsQuery2.getMatchingDocuments().size());

        final List<PercolateQuery> secondMatches = client.percolateByIds(
                Arrays.asList(
                        lincolnMemorialDoc.get_id(), columbiaDoc.get_id(), whiteHouseDoc.get_id(),
                        jeffersonMemorialDoc.get_id()), "sample", 50, fakeSecurity);
        assertEquals(15, secondMatches.size());
        Document lastResponse = client.getWithType(percolateResponse.get_id(), PERCOLATOR_TYPE, fakeSecurity);
        assertNull(lastResponse.get_id());
        pool.returnToPool(client);
    }

    private void loadSamples() {
        final Calendar visit = new GregorianCalendar();
        // Washington Monument
        jeffersonMemorial = new PlaceOfInterest();
        jeffersonMemorial.setTitle("Jefferson Memorial");
        jeffersonMemorial.setComments(
                "The Thomas Jefferson Memorial is a presidential memorial in Washington, D.C., dedicated to Thomas "
                        + "Jefferson, (1743–1826), one of the most important of the American \"Founding Fathers\" as "
                        + "the main drafter and writer of the \"Declaration of Independence\", member of the "
                        + "Continental Congress, Governor of the newly independent Commonwealth of Virginia, American"
                        + " minister to King Louis XVI and the Kingdom of France, first U.S. Secretary of State under"
                        + " the first President George Washington, the second Vice President of the United States "
                        + "under second President John Adams, and also the third President of the United States, "
                        + "(1801–1809).");
        jeffersonMemorial.setLocation(new Location(38.889468, -77.03524));
        jeffersonMemorial.setRating(92);
        jeffersonMemorial.setVisit(visit.getTime());
        jeffersonMemorialDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(jeffersonMemorial));
        // Columbia
        columbia = new PlaceOfInterest();
        columbia.setTitle("Columbia");
        columbia.setComments(
                "Columbia is a planned community comprising 10 self-contained villages, located in Howard "
                        + "County, Maryland—the second wealthiest county in the United States, according to "
                        + "2013 U.S. Census Bureau figures.");
        columbia.setLocation(new Location(39.182786, -76.808853));
        columbia.setRating(85);
        visit.add(Calendar.HOUR, -5);
        columbia.setVisit(visit.getTime());
        columbiaDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(columbia));
        //Lincoln Memorial
        lincolnMemorial = new PlaceOfInterest();
        lincolnMemorial.setTitle("Lincoln Memorial");
        lincolnMemorial.setComments(
                "The Lincoln Memorial is an American national monument built to honor the 16th President of the "
                        + "United States, Abraham Lincoln.");
        lincolnMemorial.setLocation(new Location(38.888481, -77.051518));
        lincolnMemorial.setRating(2);
        visit.add(Calendar.HOUR, -2);
        lincolnMemorial.setVisit(visit.getTime());
        lincolnMemorialDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(lincolnMemorial));
        // White House
        whiteHouse = new PlaceOfInterest();
        whiteHouse.setTitle("White House");
        whiteHouse.setComments(
                "The home of the president of the US located at 1600 Pennsylvania Ave. Since 1800 it has "
                        + "been the home of every US president.");
        whiteHouse.setLocation(new Location(38.8977, -77.0365));
        whiteHouse.setRating(23);
        visit.add(Calendar.DAY_OF_MONTH, -3);
        whiteHouse.setVisit(visit.getTime());
        whiteHouseDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(whiteHouse));
    }
    private IndexResponse populatePercolate(EzSecurityToken securityToken, String visibilityString,
            Authorizations authorizationsToSet) throws IOException, TException {
        final FilterBuilder geoDistanceFilter =
                new GeoDistanceFilterBuilder(TEST_TYPE + ".location").distance(20, DistanceUnit.KILOMETERS)
                        .lat(whiteHouse.getLocation().getLat()).lon(whiteHouse.getLocation().getLon());

        final QueryBuilder filteredQuery = new FilteredQueryBuilder(QueryBuilders.matchAllQuery(), geoDistanceFilter);
        final PercolateQuery percolator = new PercolateQuery();
        final Visibility visibility = new Visibility().setFormalVisibility(visibilityString);
        percolator.setVisibility(visibility);
        if(authorizationsToSet!=null) {
            percolator.setAuthorizations(authorizationsToSet);
        }
        final String queryDoc = jsonBuilder().startObject().field("query", filteredQuery).endObject().string();
        percolator.setQueryDocument(queryDoc);

        return client.addPercolateQuery(percolator, securityToken);
    }
}
