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
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.time.DateUtils;
import org.elasticsearch.client.Client;
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
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.data.elastic.common.ElasticUtils;
import ezbake.data.elastic.test.TestUtils;
import ezbake.data.elastic.test.models.Location;
import ezbake.data.elastic.test.models.PlaceOfInterest;
import ezbake.data.elastic.thrift.DateField;
import ezbake.data.elastic.thrift.DateHistogramFacet;
import ezbake.data.elastic.thrift.DateInterval;
import ezbake.data.elastic.thrift.DateIntervalType;
import ezbake.data.elastic.thrift.Document;
import ezbake.data.elastic.thrift.DocumentIdentifier;
import ezbake.data.elastic.thrift.Facet;
import ezbake.data.elastic.thrift.FacetRequest;
import ezbake.data.elastic.thrift.FacetResult;
import ezbake.data.elastic.thrift.FieldSort;
import ezbake.data.elastic.thrift.HighlightRequest;
import ezbake.data.elastic.thrift.HighlightedField;
import ezbake.data.elastic.thrift.IndexResponse;
import ezbake.data.elastic.thrift.MalformedQueryException;
import ezbake.data.elastic.thrift.PercolateQuery;
import ezbake.data.elastic.thrift.PercolateRequest;
import ezbake.data.elastic.thrift.Query;
import ezbake.data.elastic.thrift.RangeFacetEntry;
import ezbake.data.elastic.thrift.ScriptParam;
import ezbake.data.elastic.thrift.SearchResult;
import ezbake.data.elastic.thrift.SortCriteria;
import ezbake.data.elastic.thrift.SortOrder;
import ezbake.data.elastic.thrift.TermsFacet;
import ezbake.data.elastic.thrift.TermsFacetEntry;
import ezbake.data.elastic.thrift.TermsScriptFacet;
import ezbake.data.elastic.thrift.TermsStatsFacet;
import ezbake.data.elastic.thrift.TermsStatsFacetResultEntry;
import ezbake.data.elastic.thrift.TermsStatsValue;
import ezbake.data.elastic.thrift.UpdateOptions;
import ezbake.data.elastic.thrift.UpdateScript;
import ezbake.data.elastic.thrift.ValueScript;
import ezbake.thrift.ThriftTestUtils;

//TODO Add proper visibility testing, specifically for percolators
@SuppressWarnings({"StaticNonFinalField", "unchecked"})
public final class ElasticClientTest {
    private static final String TEST_TYPE = "sample";
    private static final String APPLICATION_NAME = "elastic-client-unit-tests";
    private static final EzSecurityToken USER_TOKEN = ezbake.data.test.TestUtils.createTestToken("U");

    private static final Gson gson = new GsonBuilder().setDateFormat("ddHHmm'Z' MM yy").create();

    private static EsSetup esSetup;

    private Document jeffersonMemorialDoc;
    private Document whiteHouseDoc;
    private Document columbiaDoc;
    private Document lincolnMemorialDoc;
    private PlaceOfInterest jeffersonMemorial;
    private PlaceOfInterest whiteHouse;
    private PlaceOfInterest columbia;
    private PlaceOfInterest lincolnMemorial;
    private ElasticClient client;

    @BeforeClass
    public static void setupClass() throws Exception {
        final Settings settings = ImmutableSettings.settingsBuilder().put("script.disable_dynamic", false)
                .put("script.native.visibility.type", "ezbake.data.elastic.security.EzSecurityScriptFactory").build();

        esSetup = new EsSetup(settings);
        esSetup.execute(EsSetup.deleteAll());

        if (esSetup.client() == null) {
            throw new Exception("Could not start EsSetup node!");
        }
    }

    @AfterClass
    public static void teardownClass() {
        esSetup.terminate();
    }

    @Before
    public void setup() throws IOException {
        esSetup.execute(EsSetup.deleteAll());

        client = new ElasticClient(esSetup.client(), APPLICATION_NAME, true, 1);
        setMappingForTest();

        final Calendar visit = new GregorianCalendar();
        // Jefferson Memorial
        jeffersonMemorial = new PlaceOfInterest();
        jeffersonMemorial.setTitle("Jefferson Memorial");
        jeffersonMemorial.setComments(
                "The Thomas Jefferson Memorial is a presidential monument in Washington, D.C., dedicated to Thomas "
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
        jeffersonMemorial.setTags(new String[] {"jefferson", "memorial", "dc"});
        jeffersonMemorialDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(jeffersonMemorial));
        // Columbia
        columbia = new PlaceOfInterest();
        columbia.setTitle("Columbia");
        columbia.setComments(
                "Columbia is a planned community comprising 10 self-contained villages, located in Howard "
                        + "County, Maryland—the second wealthiest county in the United States, according to "
                        + "2013 U.S. Census Bureau figures.");
        columbia.setLocation(new Location(39.182786, -76.808853));
        columbia.setRating(92);
        visit.add(Calendar.HOUR, -5);
        columbia.setVisit(visit.getTime());
        columbia.setTags(new String[] {"columbia", "community", "md"});
        columbiaDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(columbia));
        //Lincoln Memorial
        lincolnMemorial = new PlaceOfInterest();
        lincolnMemorial.setTitle("Lincoln Memorial");
        lincolnMemorial.setComments(
                "The Lincoln Memorial is an American national monument built to honor the 16th President of the "
                        + "United States, Abraham Lincoln. Made of white stone.");
        lincolnMemorial.setLocation(new Location(38.888481, -77.051518));
        lincolnMemorial.setRating(2);
        visit.add(Calendar.HOUR, -2);
        lincolnMemorial.setVisit(visit.getTime());
        lincolnMemorial.setTags(new String[] {"lincoln", "memorial", "dc"});
        lincolnMemorialDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(lincolnMemorial));
        // White House
        whiteHouse = new PlaceOfInterest();
        whiteHouse.setTitle("White House");
        whiteHouse.setComments(
                "The home of the president of the US located at 1600 Pennsylvania Ave. Since 1800 it has "
                        + "been the home of every US president.");
        whiteHouse.setTags(new String[] {"dc", "white", "monument"});
        whiteHouse.setLocation(new Location(38.8977, -77.0365));
        whiteHouse.setRating(23);
        visit.add(Calendar.DAY_OF_MONTH, -3);
        whiteHouse.setVisit(visit.getTime());
        whiteHouseDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(whiteHouse));
    }

    @Test
    public void testPutOne() throws Exception {
        // Action
        final List<IndexResponse> result = client.put(Collections.singletonList(jeffersonMemorialDoc));

        // Result
        assertEquals(1, result.size()); // We're only indexing a single document
        assertEquals(1, result.get(0).get_version()); // This should be the
        // first time the document
        // was indexed
        assertEquals(TEST_TYPE, result.get(0).get_type()); // Should have been
        // written to the
        // correct type
    }

    @Test
    public void testPutInvalid() {
        // Prepare
        final Document testDoc =
                TestUtils.generateDocument(TEST_TYPE, "{ THIS OBJECT ISN'T WELL FORMED: JSON : IS IT? :}");

        // Action
        final List<IndexResponse> result = client.put(Collections.singletonList(testDoc));

        assertEquals(1, result.size());
        for (final IndexResponse response : result) {
            assertFalse(response.isSuccess()); // The index should have
            // failed
        }
    }

    @Test
    public void testPutMany() throws Exception {
        // Action
        final List<IndexResponse> result = client.put(Arrays.asList(lincolnMemorialDoc, columbiaDoc, whiteHouseDoc));

        // Assertion
        assertEquals(3, result.size());
        for (final IndexResponse response : result) {
            assertEquals(1, response.get_version()); // This should be the first
            // time the document was
            // indexed
            assertEquals(TEST_TYPE, response.get_type()); // Should have been
            // written to the
            // correct type
        }
    }

    @Test
    public void testUpdateScript() throws Exception {
        populateWithTestDocs();

        final UpdateScript script = new UpdateScript().setScript("ctx._source.tags += newtag");
        script.putToParameters("newtag", "fun");

        final UpdateOptions options = new UpdateOptions();
        final DocumentIdentifier docId = new DocumentIdentifier(columbiaDoc.get_id()).setType(TEST_TYPE);
        final IndexResponse result = client.update(docId, script, options, USER_TOKEN);

        assertEquals(columbiaDoc.get_id(), result.get_id());
        final Document updated = client.get(columbiaDoc.get_id(), columbiaDoc.get_type(), null, USER_TOKEN);

        final PlaceOfInterest updatedModel = gson.fromJson(updated.get_jsonObject(), PlaceOfInterest.class);

        final List<String> expectedTags =
                ListUtils.union(Arrays.asList(columbia.getTags()), Collections.singletonList("fun"));
        assertArrayEquals(expectedTags.toArray(), updatedModel.getTags());
    }

    @Test
    public void testGetSingleById() throws Exception {
        // Prepare
        client.put(Collections.singletonList(whiteHouseDoc));
        client.forceIndexRefresh();
        // Action
        final List<Document> results = client.get(ImmutableSet.of(whiteHouseDoc.get_id()), TEST_TYPE, USER_TOKEN);

        // Assertion
        assertEquals(1, results.size()); // We should have a single result

        final PlaceOfInterest fromDataSet = gson.fromJson(results.get(0).get_jsonObject(), PlaceOfInterest.class);
        assertEquals(whiteHouse.getTitle(), fromDataSet.getTitle());
        assertEquals(whiteHouse.getComments(), fromDataSet.getComments());
        assertEquals(whiteHouse.getRating(), fromDataSet.getRating());
        // We're saving the data using a DTG which doesn't track lower than
        // minutes
        assertEquals(
                DateUtils.truncate(whiteHouse.getVisit(), Calendar.MINUTE),
                DateUtils.truncate(fromDataSet.getVisit(), Calendar.MINUTE));
        assertEquals(whiteHouse.getLocation().getLat(), fromDataSet.getLocation().getLat(), 0);
        assertEquals(whiteHouse.getLocation().getLon(), fromDataSet.getLocation().getLon(), 0);
    }

    @Test
    public void testGetFields() throws Exception {
        // Prepare
        client.put(Collections.singletonList(whiteHouseDoc));
        client.forceIndexRefresh();
        // Action should retrieve a subset of fields for the specified document
        final Document result = client.get(
                whiteHouseDoc.get_id(), TEST_TYPE, Sets.newHashSet("title", "rating", "location"), USER_TOKEN);

        // Assertion
        final Map<String, Object> resultMap = TestUtils.jsonToMap(result.get_jsonObject());

        assertEquals(3, resultMap.size());
        assertTrue(resultMap.containsKey("title"));
        assertEquals(whiteHouse.getTitle(), resultMap.get("title"));
        assertTrue(resultMap.containsKey("rating"));
        assertEquals(whiteHouse.getRating(), Integer.parseInt(resultMap.get("rating").toString()));
        assertTrue(resultMap.containsKey("location"));
        final Location locationFromResult = gson.fromJson(resultMap.get("location").toString(), Location.class);
        assertEquals(whiteHouse.getLocation().getLat(), locationFromResult.getLat(), 0);
        assertEquals(whiteHouse.getLocation().getLon(), locationFromResult.getLon(), 0);
    }

    @Test(expected = MalformedQueryException.class)
    public void testQueryMalformed() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: No expected matches just an empty result set if query can't
        // be parsed
        final String malformedQuery = "I CAN HAZ LUCENE :-)";
        client.get(malformedQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        fail("Query was malformed and an exception should have been thrown.");
    }

    @Test
    public void testQueryMatchOr() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact title
        final String matchTitleQuery = QueryBuilders.matchQuery("title", "Memorial").toString();
        final SearchResult results =
                client.get(matchTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds = Arrays.asList(jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testQueryMatchAnd() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact title
        final String matchTitleQuery =
                QueryBuilders.matchQuery("title", "Columbia").operator(MatchQueryBuilder.Operator.AND).toString();
        final SearchResult results =
                client.get(matchTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(1, results.getTotalHits());
        assertEquals(columbiaDoc.get_id(), results.getMatchingDocuments().get(0).get_id());
    }

    @Test
    public void testQueryMatchAndLucene() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact title
        final String luceneQuery = "title:Jefferson AND title:Memorial";
        final SearchResult results =
                client.get(luceneQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(1, results.getTotalHits());
        assertEquals(jeffersonMemorialDoc.get_id(), results.getMatchingDocuments().get(0).get_id());
    }

    @Test
    public void testQueryMatchInt() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact rating
        final String matchRatingQuery = QueryBuilders.matchQuery("rating", 23).toString();
        final SearchResult results =
                client.get(matchRatingQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(1, results.getTotalHits());
        assertEquals(whiteHouseDoc.get_id(), results.getMatchingDocuments().get(0).get_id());
    }

    @Test
    public void testQueryMatchIntLucene() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact rating
        final String matchRatingQuery = "rating:23";
        final SearchResult results =
                client.get(matchRatingQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(1, results.getTotalHits());
        assertEquals(whiteHouseDoc.get_id(), results.getMatchingDocuments().get(0).get_id());
    }

    @Test
    public void testQueryMatchNone() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: No matches expected as no documents have a title with the
        // word "cow"
        final String noMatchesTitleQuery = QueryBuilders.matchQuery("title", "cow").toString();
        final SearchResult results =
                client.get(noMatchesTitleQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryMatchNonExistantField() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: No matches expected as field isn't part of index
        final String nonExistantFieldQuery = QueryBuilders.matchQuery("age", 23).toString();
        final SearchResult results =
                client.get(nonExistantFieldQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryMatchNonExistantFieldLucene() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: No matches expected as field isn't part of index
        final String nonExistantFieldQuery = "age:23";
        final SearchResult results =
                client.get(nonExistantFieldQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryMultiMatch() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Two matches as White House has the word in the title and Lincoln Memorial
        // has it in the comments
        final String multiMatchQuery = QueryBuilders.multiMatchQuery("white", "title", "comments").toString();
        final SearchResult results =
                client.get(multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds = Arrays.asList(whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testQueryMultiMatchLucene() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Two matches as White House has the word in the title and Lincoln Memorial
        // has it in the comments
        final String multiMatchQuery = "title:white OR comments:white";
        final SearchResult results =
                client.get(multiMatchQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds = Arrays.asList(whiteHouseDoc.get_id(), lincolnMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testQueryRangeInt() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact title
        final String rangeQuery = QueryBuilders.rangeQuery("rating").gt(20).lte(100).toString();
        final SearchResult results =
                client.get(rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds =
                Arrays.asList(columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testQueryRangeIntLucene() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact title
        final String rangeQuery = "rating:[20 TO 100]";
        final SearchResult results =
                client.get(rangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds =
                Arrays.asList(columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testQueryRangeDateNoMapping() throws Exception {
        // Prepare
        client.setTypeMapping(TEST_TYPE, "");
        populateWithTestDocs();

        final Calendar now = new GregorianCalendar();
        now.add(Calendar.HOUR, -1);
        final Long oneHourAgo = now.getTimeInMillis();
        now.add(Calendar.HOUR, 25);
        final Long oneDayFromNow = now.getTimeInMillis();

        // Action: No matches expected as the date field is simply a string at
        // this point
        final String dateRangeQuery = QueryBuilders.rangeQuery("visit").gt(oneHourAgo).lt(oneDayFromNow).toString();
        final SearchResult results =
                client.get(dateRangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(0, results.getTotalHits());
    }

    @Test
    public void testQueryRangeDateMapped() throws Exception {
        final SimpleDateFormat dtg = new SimpleDateFormat("ddHHmm'Z' MM yy");

        populateWithTestDocs();
        final Calendar now = Calendar.getInstance();
        now.add(Calendar.HOUR, 1);
        final Date oneHourFromNow = now.getTime();
        now.add(Calendar.DAY_OF_MONTH, -1);
        final Date oneDayAgo = now.getTime();

        // Action: Single expected match on exact title
        // Note: Even though elastic search stores the date internally as a
        // long, passing in ms since epoch will cause
        // this test to fail...
        final String dateRangeQuery =
                QueryBuilders.rangeQuery("visit").gt(dtg.format(oneDayAgo)).lt(dtg.format(oneHourFromNow)).toString();
        final SearchResult results =
                client.get(dateRangeQuery, TEST_TYPE, null, null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds =
                Arrays.asList(columbiaDoc.get_id(), lincolnMemorialDoc.get_id(), jeffersonMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testQueryOnFields() throws Exception {
        populateWithTestDocs();

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, Sets.newHashSet("title", "rating"), null,
                null, 0, (short) 10, null, USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        assertEquals(4, results.getMatchingDocumentsSize());
        Map<String, Object> map = new HashMap<>();
        for (final Document result : results.getMatchingDocuments()) {
            map = gson.fromJson(result.get_jsonObject(), map.getClass());
            assertTrue(map.keySet().contains("title"));
            assertTrue(map.keySet().contains("rating"));
        }
    }

    @Test
    public void testDateHistogramFacet() throws Exception {
        populateWithTestDocs();

        final List<Facet> facets = new ArrayList<>();
        final DateField dateField = new DateField();
        dateField.set_field("visit");
        final DateInterval dateInterval = new DateInterval();
        dateInterval.setStaticInterval(DateIntervalType.DAY);
        final DateHistogramFacet dhgFacet = new DateHistogramFacet(dateField, dateInterval);
        final FacetRequest request = new FacetRequest();
        request.setDateHistogramFacet(dhgFacet);
        facets.add(new Facet("Visit", request));

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, facets, null, 0, (short) 10, null,
                USER_TOKEN);

        assertEquals(4, results.getTotalHits());
        assertFalse(results.getFacets().isEmpty());
        assertTrue(results.getFacets().containsKey("Visit"));
        final FacetResult facetResult = results.getFacets().get("Visit");
        assertTrue(
                facetResult.getDateFacetResult().getEntriesSize() < 4
                        && facetResult.getDateFacetResult().getEntriesSize() > 1);
    }

    @Test
    public void testRangeDateFacets() throws Exception {
        final SimpleDateFormat dtg = new SimpleDateFormat("ddHHmm'Z' MM yy");
        populateWithTestDocs();

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null,
                Collections.singletonList(TestUtils.generateDateBucketFacet()), null, 0, (short) 10, null, USER_TOKEN);

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
    }

    @Test
    public void testDelete() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Delete single record then try to retrieve it
        client.delete(ImmutableSet.of(whiteHouseDoc.get_id()), TEST_TYPE, USER_TOKEN);
        final List<Document> results = client.get(ImmutableSet.of(whiteHouseDoc.get_id()), TEST_TYPE, USER_TOKEN);

        // Assertion
        assertEquals(0, results.size());
    }

    @Test
    public void testVisibility() throws Exception {
        final PlaceOfInterest bunker = new PlaceOfInterest();
        bunker.setTitle("Greenbrier Bunker");
        bunker.setComments("Secret bunker to keep the president safe.");
        bunker.setLocation(new Location(37.789322, -80.303414));
        bunker.setRating(65);
        bunker.setVisit(Calendar.getInstance().getTime());
        final Document bunkerDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(jeffersonMemorial));

        // NOT A REAL CLASSIFICATION
        bunkerDoc.setVisibility(new Visibility().setFormalVisibility("EF&AB&CD&(USA|GBR)"));

        client.put(Collections.singletonList(bunkerDoc));
        client.forceIndexRefresh();

        final SearchResult resultVisMismatch = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                ThriftTestUtils.generateTestSecurityToken("S", "AB"));

        assertEquals(0, resultVisMismatch.getTotalHits());

        final SearchResult resultVisMatch = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, null, 0, (short) -1, null,
                ThriftTestUtils.generateTestSecurityToken("S", "AB", "CD", "EF", "USA"));

        assertEquals(1, resultVisMatch.getTotalHits());

        assertEquals(
                "EF&AB&CD&(USA|GBR)",
                resultVisMatch.getMatchingDocuments().get(0).getVisibility().getFormalVisibility());
    }

    @Test
    public void testHighlighting() throws Exception {
        populateWithTestDocs();

        final HighlightRequest highlight = new HighlightRequest().setFields(
                new HashSet<>(
                        Arrays.asList(new HighlightedField[] {new HighlightedField("comments")})))
                .setPre_tags(Collections.singletonList("FOO>>")).setPost_tags(Collections.singletonList("<<BAR"));
        final SearchResult result = client.get(
                QueryBuilders.matchQuery("comments", "white").toString(), TEST_TYPE, null, null, null, null, 0,
                (short) -1, highlight, USER_TOKEN);

        assertEquals(1, result.getTotalHits());

        final Map<String, List<String>> highlights = result.getHighlights().get(lincolnMemorialDoc.get_id()).getResults();
        assertNotNull(highlights);

        assertEquals(
                "The Lincoln Memorial is an American national monument built to honor the 16th President of the "
                        + "United States, Abraham Lincoln. Made of FOO>>white<<BAR stone.",
                highlights.get("comments").get(0));
    }

    @Test
    public void testFacetSize() throws Exception {
        populateWithTestDocs();

        final TermsFacet termsFacetWithDefaultSize = new TermsFacet();
        termsFacetWithDefaultSize.setFields(Collections.singletonList("rating"));
        final FacetRequest requestWithDefaultSize = new FacetRequest();
        requestWithDefaultSize.setTermsFacet(termsFacetWithDefaultSize);

        final SearchResult resultsWithDefaultSize = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null,
                Collections.singletonList(new Facet("Rating", requestWithDefaultSize)), null, 0, (short) 10, null,
                USER_TOKEN);

        assertEquals(4, resultsWithDefaultSize.getTotalHits());
        assertFalse(resultsWithDefaultSize.getFacets().isEmpty());
        assertTrue(resultsWithDefaultSize.getFacets().containsKey("Rating"));
        assertEquals(3, resultsWithDefaultSize.getFacets().get("Rating").getTermsFacetResult().getEntriesSize());

        final TermsFacet termsFacetWithSetSize = new TermsFacet();
        termsFacetWithSetSize.setFields(Collections.singletonList("rating"));
        termsFacetWithSetSize.setSize(2);
        final FacetRequest requestWithSetSize = new FacetRequest();
        requestWithSetSize.setTermsFacet(termsFacetWithSetSize);

        final SearchResult resultsWithSetSize = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null,
                Collections.singletonList(new Facet("Rating", requestWithSetSize)), null, 0, (short) 10, null,
                USER_TOKEN);

        assertEquals(4, resultsWithSetSize.getTotalHits());
        assertFalse(resultsWithSetSize.getFacets().isEmpty());
        assertTrue(resultsWithSetSize.getFacets().containsKey("Rating"));
        assertEquals(2, resultsWithSetSize.getFacets().get("Rating").getTermsFacetResult().getEntriesSize());
    }

    @Test
    public void testSortInteger() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Single expected match on exact title
        final FieldSort ratingDescending = new FieldSort();
        ratingDescending.setField("rating");
        ratingDescending.setOrder(SortOrder.DESCENDING);
        final SortCriteria sortDescendingCriteria = new SortCriteria();
        sortDescendingCriteria.setFieldSort(ratingDescending);

        final SearchResult descendingResults = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, Collections.singletonList(sortDescendingCriteria),
                null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds = Arrays.asList(
                columbiaDoc.get_id(), whiteHouseDoc.get_id(), jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id());

        assertEquals(expectedIds.size(), descendingResults.getTotalHits());
        final List<Integer> descendingRatings = new ArrayList<>((int) descendingResults.getTotalHits());
        for (final Document result : descendingResults.getMatchingDocuments()) {
            final PlaceOfInterest fromJson = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
            descendingRatings.add(fromJson.getRating());
        }

        final Ordering<Integer> descendingOrdering = Ordering.natural().reverse();
        assertTrue(descendingOrdering.isOrdered(descendingRatings));

        final FieldSort sortAscending = new FieldSort();
        sortAscending.setField("rating");
        sortAscending.setOrder(SortOrder.ASCENDING);
        final SortCriteria sortAscendingCriteria = new SortCriteria();
        sortAscendingCriteria.setFieldSort(sortAscending);

        final SearchResult ascendingResults = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, Collections.singletonList(sortAscendingCriteria),
                null, null, null, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(expectedIds.size(), ascendingResults.getTotalHits());
        final List<Integer> ascendingRatings = new ArrayList<>((int) ascendingResults.getTotalHits());
        for (final Document result : ascendingResults.getMatchingDocuments()) {
            final PlaceOfInterest fromJson = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
            ascendingRatings.add(fromJson.getRating());
        }

        final Ordering<Integer> ascendingOrdering = Ordering.natural();
        assertTrue(ascendingOrdering.isOrdered(ascendingRatings));
    }

    @Test
    public void testQueryNonExistantIndex() throws Exception {
        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, null, 0, (short) 10, null,
                USER_TOKEN);

        assertEquals(0, results.getMatchingDocuments().size());
        assertEquals(QueryBuilders.matchAllQuery().toString(), results.actualQuery);
    }

    @Test
    public void testQueryWithFilter() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Two matches as Jefferson Memorial and Lincoln Memorial has the word in the title
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "memorial"))
                .toString();
        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, filter, 0, (short) -1, null,
                USER_TOKEN);

        // Assertion
        final List<String> expectedIds = Arrays.asList(jeffersonMemorialDoc.get_id(), lincolnMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testQueryWithElasticJsonAndFilter() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Filter will match two but query should only match one due to
        // rating range of query.
        final String filter = FilterBuilders.andFilter(
                FilterBuilders.rangeFilter("rating").from(0).to(100), FilterBuilders.termFilter("title", "memorial"))
                .toString();
        final String query = QueryBuilders.boolQuery().must(
                QueryBuilders.rangeQuery("rating").from(35).to(98).includeLower(true).includeLower(true))
                .must(QueryBuilders.matchQuery("comments", "memorial")).mustNot(QueryBuilders.prefixQuery("title", "W"))
                .toString();

        final SearchResult results =
                client.get(query, TEST_TYPE, null, null, null, filter, 0, (short) -1, null, USER_TOKEN);

        // Assertion
        final List<String> expectedIds = Collections.singletonList(jeffersonMemorialDoc.get_id());
        assertEquals(expectedIds.size(), results.getTotalHits());
        for (final Document result : results.getMatchingDocuments()) {
            assertTrue(expectedIds.contains(result.get_id()));
        }
    }

    @Test
    public void testTermScript() throws Exception {
        // Prepare
        populateWithTestDocs();
        final TermsScriptFacet tsf = new TermsScriptFacet();
        tsf.setFields(Collections.singletonList("title"));
        tsf.setScript(new ValueScript("term == 'memorial' ? 'memorial' : 'other'", new ArrayList<ScriptParam>()));
        final FacetRequest request = new FacetRequest();
        request.setTermsScriptFacet(tsf);
        final Facet facet = new Facet();
        facet.setLabel("magic");
        facet.setFacet(request);

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(4, results.getTotalHits());
        int otherCount = 0;
        int memorialCount = 0;

        for (final Document result : results.getMatchingDocuments()) {
            final PlaceOfInterest fromJson = gson.fromJson(result.get_jsonObject(), PlaceOfInterest.class);
            for (final String val : fromJson.getTitle().split(" ")) {
                if ("memorial".equalsIgnoreCase(val)) {
                    memorialCount++;
                } else {
                    otherCount++;
                }
            }
        }

        for (final TermsFacetEntry facetResult : results.getFacets().get("magic").getTermsFacetResult().getEntries()) {
            if ("memorial".equalsIgnoreCase(facetResult.getTerm())) {
                assertEquals(memorialCount, facetResult.getCount());
            } else {
                assertEquals(otherCount, facetResult.getCount());
            }
        }
    }

    @Test
    public void testTermsStatsStringFacet() throws Exception {
        // Prepare
        populateWithTestDocs();
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

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(4, results.getTotalHits());
        for (final TermsStatsFacetResultEntry facetResult : results.getFacets().get("magic").getTermsStatsFacetResult()
                .getEntries()) {
            assertNotEquals(facetResult.getTerm(), Integer.toString((int) facetResult.getTermAsNumber()));
        }
    }

    @Test
    public void testTermsStatsDoubleFacet() throws Exception {
        // Prepare
        populateWithTestDocs();
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

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(4, results.getTotalHits());
        for (final TermsStatsFacetResultEntry facetResult : results.getFacets().get("magic").getTermsStatsFacetResult()
                .getEntries()) {
            assertEquals(facetResult.getTerm(), Integer.toString((int) facetResult.getTermAsNumber()));
        }
    }

    @Test
    public void testTermsStatsFacetWithFilter() throws Exception {
        // Prepare
        populateWithTestDocs();

        // Action: Filter will match two but query should only match one due to
        // rating range of query.
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

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet), null,
                0, (short) -1, null, USER_TOKEN);

        // Assertion
        assertEquals(4, results.getTotalHits());
        final Set<String> uniqueTitleTerms = new HashSet<>();
        uniqueTitleTerms.addAll(Arrays.asList(lincolnMemorial.getTitle().split(" ")));
        uniqueTitleTerms.addAll(Arrays.asList(jeffersonMemorial.getTitle().split(" ")));

        assertEquals(
                uniqueTitleTerms.size(),
                results.getFacets().get("magic").getTermsStatsFacetResult().getEntries().size());
    }

    @Test
    public void testFilterShouldNotFilterFacets() throws Exception {
        populateWithTestDocs();

        final String filter = FilterBuilders.termFilter("title", "memorial").toString();

        final TermsFacet tf = new TermsFacet(Collections.singletonList("rating"));
        final FacetRequest request = new FacetRequest();
        request.setTermsFacet(tf);
        final Facet facet = new Facet().setLabel("magic").setFacet(request);

        final SearchResult results = client.get(
                QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, Collections.singletonList(facet),
                filter, 0, (short) -1, null, USER_TOKEN);

        assertEquals(2, results.getTotalHits());
        final List<TermsFacetEntry> entries = results.getFacets().get("magic").getTermsFacetResult().getEntries();
        for (final TermsFacetEntry entry : entries) {
            // if the filter gets applied to the facets this will only be 1
            if ("92".equalsIgnoreCase(entry.getTerm())) {
                assertEquals(2, entry.getCount());
            }
        }
    }

    @Test
    public void testPurge() throws Exception {
        final AdvancedMarkings markings = new AdvancedMarkings().setId(5L).setComposite(false);
        final Visibility visibility = new Visibility().setFormalVisibility("U").setAdvancedMarkings(markings);
        final Document purgeDoc = TestUtils.generateDocument(TEST_TYPE, gson.toJson(jeffersonMemorial), visibility);
        client.put(Arrays.asList(purgeDoc, lincolnMemorialDoc));
        client.forceIndexRefresh();

        // Ensure it exists before purging
        final List<Document> results = client.get(ImmutableSet.of(purgeDoc.get_id()), TEST_TYPE, USER_TOKEN);
        assertEquals(purgeDoc.get_id(), results.get(0).get_id());

        final PurgeResult purgeResult = client.purge(79472397424L, Sets.newHashSet(5L, 25L), 100);
        assertNotNull(purgeResult.getPurged());
        assertTrue(purgeResult.getPurged().contains(5L));

        assertEquals(
                1, client.get(
                        QueryBuilders.matchAllQuery().toString(), TEST_TYPE, null, null, null, null, 0, (short) -1,
                        null, USER_TOKEN).getTotalHits());
    }

    @Test
    public void testAddPercolator() throws Exception {
        Node node = nodeBuilder().node();
        Client baseClient = node.client();
        // Prepare: Setup Thrift Client and add some docs
        populateWithTestDocs();

        final FilterBuilder geoDistanceFilter =
                new GeoDistanceFilterBuilder(TEST_TYPE + ".location").distance(10, DistanceUnit.KILOMETERS)
                        .lat(whiteHouse.getLocation().getLat()).lon(whiteHouse.getLocation().getLon());

        final QueryBuilder filteredQuery = new FilteredQueryBuilder(QueryBuilders.matchAllQuery(), geoDistanceFilter);

        final PercolateQuery percolator = new PercolateQuery();
        percolator.setId(UUID.randomUUID().toString());

        final String queryDoc = jsonBuilder().startObject().field("query", filteredQuery).endObject().string();

        percolator.setQueryDocument(queryDoc);

        // Action: Create and execute an OR match query on title
        final Document doc = new Document(
                ElasticUtils.PERCOLATOR_TYPE, new Visibility().setFormalVisibility("U"), percolator.getQueryDocument());
        final IndexResponse percolateResponse = client.put(Collections.singletonList(doc)).get(0);
        assertTrue(percolateResponse.isSuccess());

        // Assert: There should be 2 matching documents
        lincolnMemorialDoc.setPercolate(new PercolateRequest());
        columbiaDoc.setPercolate(new PercolateRequest());
        whiteHouseDoc.setPercolate(new PercolateRequest());
        jeffersonMemorialDoc.setPercolate(new PercolateRequest());
        final List<PercolateQuery> matches = client.percolate(
                Arrays.asList(lincolnMemorialDoc, columbiaDoc, whiteHouseDoc, jeffersonMemorialDoc));
        assertEquals(3, matches.size());

        String queryJson = QueryBuilders.matchAllQuery().toString();
        final Query query = new Query(queryJson);
        SearchResult matchAll = client.get(
                query.getSearchString(), ElasticUtils.PERCOLATOR_TYPE, query.getSortCriteria(), query.getReturnedFields(),
                query.getFacets(), query.getFilterJson(), query.isSetPage() ? query.getPage().getOffset() : 0,
                query.isSetPage() ? query.getPage().getPageSize() : (short) -1, query.getHighlighting(), USER_TOKEN);

        Set<String> percolateIdSet = new HashSet<>();
        percolateIdSet.add(percolateResponse.get_id());
        List<Document> percolateGet = client.get(percolateIdSet, ElasticUtils.PERCOLATOR_TYPE, USER_TOKEN);

        client.delete(percolateResponse.get_id(), ElasticUtils.PERCOLATOR_TYPE, USER_TOKEN);
        boolean deleteResponse = client.deletePercolator(percolateResponse.get_id());
        assertTrue(deleteResponse);

        final List<PercolateQuery> secondMatches = client.percolate(
                Arrays.asList(
                        lincolnMemorialDoc, columbiaDoc, whiteHouseDoc, jeffersonMemorialDoc));
        assertEquals(0, secondMatches.size());
    }

    private void populateWithTestDocs() {
        client.put(Arrays.asList(lincolnMemorialDoc, columbiaDoc, whiteHouseDoc, jeffersonMemorialDoc));
        client.forceIndexRefresh();
    }

    private void setMappingForTest() throws IOException {
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

        mapping.startObject("tags");
        mapping.field("type", "string");
        mapping.field("index", "not_analyzed");
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

        client.setTypeMapping(TEST_TYPE, mapping.string());
    }
}
