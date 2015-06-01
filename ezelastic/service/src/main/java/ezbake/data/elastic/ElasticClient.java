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

import static org.elasticsearch.action.search.SearchType.SCAN;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import static ezbake.data.elastic.common.ElasticUtils.PERCOLATOR_TYPE;
import static ezbake.data.elastic.common.ElasticUtils.VISIBILITY_FIELD;
import static ezbake.data.elastic.common.ElasticUtils.addFacetsToSearch;
import static ezbake.data.elastic.common.ElasticUtils.convertFieldSort;
import static ezbake.data.elastic.common.ElasticUtils.convertGeoSort;
import static ezbake.data.elastic.common.ElasticUtils.getFacetsFromResult;
import static ezbake.data.elastic.common.ElasticUtils.getVisibilityFilter;
import static ezbake.data.elastic.common.ElasticUtils.isClusterHealthy;
import static ezbake.data.elastic.common.ElasticUtils.refreshIndex;
import static ezbake.thrift.ThriftUtils.serializeToBase64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptFilterBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.CancelStatus;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.data.common.LoggingUtils;
import ezbake.data.elastic.common.ElasticUtils;
import ezbake.data.elastic.common.VisibilityFilterConfig;
import ezbake.data.elastic.thrift.Document;
import ezbake.data.elastic.thrift.DocumentIdentifier;
import ezbake.data.elastic.thrift.Facet;
import ezbake.data.elastic.thrift.FacetRequest;
import ezbake.data.elastic.thrift.HighlightRequest;
import ezbake.data.elastic.thrift.HighlightResult;
import ezbake.data.elastic.thrift.HighlightedField;
import ezbake.data.elastic.thrift.IndexResponse;
import ezbake.data.elastic.thrift.MalformedQueryException;
import ezbake.data.elastic.thrift.PercolateQuery;
import ezbake.data.elastic.thrift.SearchResult;
import ezbake.data.elastic.thrift.SortCriteria;
import ezbake.data.elastic.thrift.UpdateOptions;
import ezbake.data.elastic.thrift.UpdateScript;
import ezbake.thrift.ThriftUtils;

@SuppressWarnings("ParameterHidesMemberVariable")
public final class ElasticClient implements DocumentStore {
    public static final short MAX_PAGE_SIZE = Short.MAX_VALUE;

    private static final String PURGE_INDEX = "active_purges";
    private static final String PURGE_TYPE = "purge";
    private static final String PURGE_SCROLL_DURATION = "5m"; // 5 minutes

    private static final Logger logger = LoggerFactory.getLogger(ElasticClient.class);
    private final String indexName; // This exists to decouple the index from the application name
    private final String applicationName;
    private final int version;
    private Client client;
    private Gson gson;
    private boolean forceRefreshOnInsert;

    public ElasticClient() {
        indexName = null;
        applicationName = null;
        version = 1;
    }

    public ElasticClient(
            String hosts, int port, String cluster, String applicationName, boolean instantRefresh, int version) {
        final TransportClient transportClient =
                new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", cluster).build());

        for (final String host : hosts.split(",")) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(host, port));
        }

        this.client = transportClient;
        this.indexName = applicationName;
        this.applicationName = applicationName;
        this.version = version;
        gson = new Gson();

        if (instantRefresh) {
            logger.warn(
                    "Starting ElasticClient with forced refresh on inserts. This is only recommended for "
                            + "controlled environments, high throughput performance will suffer.");
            forceRefreshOnInsert = true;
        }
        applyDefaultMapping();
    }

    public ElasticClient(Client client, String applicationName, boolean instantRefresh, int version) {
        this.client = client;
        this.indexName = applicationName;
        this.applicationName = applicationName;
        this.version = version;
        gson = new Gson();

        if (instantRefresh) {
            logger.warn(
                    "Starting ElasticClient with forced refresh on inserts. This is only recommended for "
                            + "controlled environments, high throughput performance will suffer.");
            forceRefreshOnInsert = true;
        }
        applyDefaultMapping();
    }

    public static ElasticClient getInstance(
            String hostname, int port, String cluster, String applicationName, boolean instantRefresh, int version) {
        return new ElasticClient(hostname, port, cluster, applicationName, instantRefresh, version);
    }

    private static Map<String, HighlightResult> getHighlightsFromResult(SearchHits hits) {
        final HashMap<String, HighlightResult> highlights = new HashMap<>();
        for (final SearchHit hit : hits) {
            final HighlightResult result = new HighlightResult(new HashMap<String, List<String>>());
            final Collection<HighlightField> highlightFields = hit.getHighlightFields().values();
            for (final HighlightField entry : highlightFields) {
                final List<String> fragments = new ArrayList<>();
                final Text[] frags = entry.fragments();
                for (final Text t : frags) {
                    fragments.add(t.toString());
                }
                result.putToResults(entry.getName(), fragments);
            }
            highlights.put(hit.getId(), result);
        }
        return highlights;
    }

    private static void addHighlightingToSearch(SearchRequestBuilder builder, HighlightRequest highlight) {
        for (final HighlightedField field : highlight.getFields()) {
            builder.addHighlightedField(field.getField(), field.getFragmentSize(), field.getNumberOfFragments());
        }

        if (highlight.isSetOrder()) {
            builder.setHighlighterOrder(highlight.getOrder());
        }

        if (highlight.isSetPre_tags()) {
            final List<String> preTags = highlight.getPre_tags();
            builder.setHighlighterPreTags(preTags.toArray(new String[preTags.size()]));
        }

        if (highlight.isSetPost_tags()) {
            final List<String> postTags = highlight.getPost_tags();
            builder.setHighlighterPostTags(postTags.toArray(new String[postTags.size()]));
        }

        if (highlight.isSetRequireFieldMatch()) {
            builder.setHighlighterRequireFieldMatch(highlight.isRequireFieldMatch());
        }
    }

    private static QueryBuilder parseQuery(String query) {
        final JsonParser parser = new JsonParser();
        try {
            final JsonElement result = parser.parse(query);
            if (result.isJsonObject()) {
                // Assume it's elasticsearch json
                return QueryBuilders.wrapperQuery(query);
            }
        } catch (final JsonSyntaxException ex) {
            // Going to treat as a lucene query
        }
        // If there was an exception or we haven't returned assume it's lucene
        return QueryBuilders.queryString(query);
    }

    private static IndexResponse convertElasticResponse(String id, String type, long version, boolean success) {
        final IndexResponse result = new IndexResponse();
        result.set_id(id);
        result.set_type(type);
        result.set_version(version);
        result.setSuccess(success);
        return result;
    }

    private static String addPlatformFields(String docId, String original, Visibility vis) {
        // I don't like string manipulation here but I also don't like that GSON and other libraries
        // mess with what was passed in (int -> double etc).
        final String original1 = original.substring(0, original.lastIndexOf('}'));
        final String visBase64;
        try {
            visBase64 = serializeToBase64(vis);
        } catch (final TException e) {
            logger.error(
                    "Document {} failed to parse! There was an error converting the visibility to base64: {}", docId,
                    vis, e);

            return null;
        }

        return original1 + ",\"" + VISIBILITY_FIELD + "\" : \"" + visBase64 + '"' + '}';
    }

    @SuppressWarnings("unchecked")
    private static void addVisibilityFilter(PercolateQuery query, Authorizations authorizations) throws TException {
        // Set visibility filter
        final VisibilityFilterConfig filterConfig =
                new VisibilityFilterConfig(VISIBILITY_FIELD, EnumSet.of(Permission.READ));
        FilterBuilder visibilityFilter = getVisibilityFilter(authorizations, filterConfig);
        JSONObject visibilityJSON = new JSONObject(visibilityFilter.toString());
        JSONObject filteredToAdd = new JSONObject();
        JSONObject outer = new JSONObject(query.getQueryDocument());
        JSONObject gettingOuterQuery = outer.getJSONObject("query");

        try {
            JSONObject gettingFiltered = gettingOuterQuery.getJSONObject("filtered");
            JSONObject gettingFilters = gettingFiltered.getJSONObject("filter");

            JSONArray filters = new JSONArray();
            filters.add(gettingFilters);
            filters.add(visibilityJSON);

            JSONObject andingFilters = new JSONObject();
            andingFilters.put("and", filters);

            filteredToAdd.put("filter", andingFilters);
            filteredToAdd.put("query", gettingFiltered.getJSONObject("query"));
        } catch (JSONException e) {
            filteredToAdd.put("query", gettingOuterQuery);
            filteredToAdd.put("filter", visibilityJSON);
        }

        JSONObject queryToAdd = new JSONObject();
        queryToAdd.put("filtered", filteredToAdd);

        outer.put("query", queryToAdd);
        query.setQueryDocument(outer.toString());
    }

    @Override
    public List<IndexResponse> put(List<Document> documents) {
        final Stopwatch watch = LoggingUtils.createStopWatch();
        final List<IndexResponse> results = new ArrayList<>();
        final BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (final Document document : documents) {
            final String original = document.get_jsonObject();
            final String source = addPlatformFields(document.get_id(), original, document.getVisibility());
            if (source != null) {
                bulkRequest.add(
                        client.prepareIndex(indexName, document.get_type(), document.get_id()).setSource(
                                source));
            }
        }

        for (final BulkItemResponse response : bulkRequest.get()) {
            final IndexResponse indexResponse;
            if (response.isFailed()) {
                final BulkItemResponse.Failure failure = response.getFailure();
                logger.warn(
                        "Indexing failed on an object ({}, {}): {}", response.getId(), response.getType(),
                        response.getFailureMessage());
                indexResponse = convertElasticResponse(failure.getId(), failure.getType(), -1, false);
            } else {
                final org.elasticsearch.action.index.IndexResponse ir = response.getResponse();
                indexResponse = convertElasticResponse(ir.getId(), ir.getType(), ir.getVersion(), true);
            }
            results.add(indexResponse);
        }

        LoggingUtils.stopAndLogStopWatch(logger, watch, "Put Documents");

        if (forceRefreshOnInsert) {
            logger.trace("Forcing index refresh");
            forceIndexRefresh();
        }

        return results;
    }

    @Override
    public IndexResponse update(
            DocumentIdentifier id, UpdateScript script, UpdateOptions options, EzSecurityToken token)
            throws TException {
        // Ensure we can see the ID
        ensureVisible(id, token);

        if (!script.isSetScript()) {
            final String errMsg = "No script was set for call to update()!";
            logger.info(errMsg);
            throw new TException(errMsg);
        }

        final UpdateRequestBuilder updateRequest =
                client.prepareUpdate(indexName, id.getType(), id.getId()).setRefresh(true);

        updateRequest.setScript(script.getScript());
        if (script.isSetParameters()) {
            for (final Map.Entry<String, String> entry : script.getParameters().entrySet()) {
                updateRequest.addScriptParam(entry.getKey(), entry.getValue());
            }
        }

        updateRequest.setRetryOnConflict(options.getRetryCount());

        final UpdateResponse response = updateRequest.get();
        return new IndexResponse().set_id(response.getId()).set_type(response.getType())
                .set_version(response.getVersion()).setSuccess(true);
    }

    @Override
    public List<Document> get(Set<String> ids, String type, EzSecurityToken userToken) throws TException {
        final String idQuery;
        idQuery = QueryBuilders.idsQuery().addIds(ids.toArray(new String[ids.size()])).toString();
        return get(idQuery, type, null, null, null, null, 0, MAX_PAGE_SIZE, null, userToken).getMatchingDocuments();
    }

    @Override
    public Document get(String id, String type, Set<String> fields, EzSecurityToken userToken) throws TException {
        final SearchResult result = get(
                QueryBuilders.idsQuery(type).addIds(id).toString(), type, null, fields, null, null, 0, MAX_PAGE_SIZE,
                null, userToken);
        if (result.getMatchingDocuments().isEmpty()) {
            logger.error("Unable to retrieve document :: {}/{}/{}", indexName, type, id);
            return EzElasticHandler.BLANK_DOCUMENT;
        }
        return result.getMatchingDocuments().get(0);
    }

    @Override
    public SearchResult get(
            String query, String type, List<SortCriteria> sortCriteria, Set<String> fields, List<Facet> facets,
            String filterJson, int offset, short pageSize, HighlightRequest highlight, EzSecurityToken userToken)
            throws TException {

        final Stopwatch watch = LoggingUtils.createStopWatch();

        // Set visibility filter
        final VisibilityFilterConfig filterConfig =
                new VisibilityFilterConfig(VISIBILITY_FIELD, EnumSet.of(Permission.READ));
        FilterBuilder visibilityFilter = getVisibilityFilter(userToken, filterConfig);

        final SearchRequestBuilder builder = client.prepareSearch(indexName).setQuery(
                QueryBuilders.filteredQuery(
                        parseQuery(query), visibilityFilter));

        // Set type restriction
        if (!StringUtils.isEmpty(type)) {
            builder.setTypes(type);
        }

        // Set field restrictions
        if (fields != null && !fields.isEmpty()) {
            fields.add(VISIBILITY_FIELD);
            // Starting in ES 1.0.0 rc1 fields will not be returned if they are not leaves; we now
            // need to filter the _source property
            // builder.addFields(fields.toArray(new String[fields.size()]));
            builder.setFetchSource(fields.toArray(new String[fields.size()]), null);
        }

        // Set sort criteria
        if (sortCriteria != null && !sortCriteria.isEmpty()) {
            for (final SortCriteria criteria : sortCriteria) {
                if (criteria.isSet(SortCriteria._Fields.FIELD_SORT)) {
                    builder.addSort(convertFieldSort(criteria.getFieldSort()));
                } else {
                    builder.addSort(convertGeoSort(criteria.getGeoSort()));
                }
            }
        }

        if (!StringUtils.isBlank(filterJson)) {
            builder.setPostFilter(
                    FilterBuilders.wrapperFilter(filterJson));
        }

        // Set facets
        Map<String, FacetRequest._Fields> facetMap = null;
        if (facets != null && !facets.isEmpty()) {
            facetMap = addFacetsToSearch(facets, builder, visibilityFilter);
        }

        // Set paging
        final short cappedPageSize = (short) Math.min(MAX_PAGE_SIZE, pageSize);
        if (pageSize > 0) {
            builder.setFrom(offset).setSize(cappedPageSize);
        }

        // Set highlighting
        if (highlight != null) {
            addHighlightingToSearch(builder, highlight);
        }

        final SearchResult result = new SearchResult();
        // Set static data
        result.setActualQuery(query);
        result.setOffset(offset);
        result.setPagesize(cappedPageSize);
        // Initialize required fields
        result.setTotalHits(0);
        result.setMatchingDocuments(new ArrayList<Document>());
        builder.setVersion(true);

        final SearchResponse response;
        try {
            response = builder.get();
        } catch (final SearchPhaseExecutionException ex) {
            throw new MalformedQueryException(query, ex.getMessage());
        } catch (final IndexMissingException ex) {
            logger.warn("Attempt to query index that doesn't exist : {}", indexName);
            return result;
        }
        logger.info(
                "Executed query : {}\nTook (ms):{}\nTotal Results:{}\n", query, response.getTookInMillis(),
                response.getHits().getTotalHits());

        result.setTotalHits(response.getHits().getTotalHits());
        result.setMatchingDocuments(getDocsFromResult(response.getHits()));

        //noinspection VariableNotUsedInsideIf
        if (highlight != null) {
            result.setHighlights(getHighlightsFromResult(response.getHits()));
        }

        if (facetMap != null && !facetMap.isEmpty()) {
            result.setFacets(getFacetsFromResult(facetMap, response.getFacets().facetsAsMap()));
        }

        LoggingUtils.stopAndLogStopWatch(logger, watch, "Get With Query");

        return result;
    }

    @Override
    public void delete(Set<String> ids, String type, EzSecurityToken userToken) throws TException {
        final String idQuery;
        if (StringUtils.isEmpty(type)) {
            idQuery = QueryBuilders.idsQuery().addIds(ids.toArray(new String[ids.size()])).toString();
        } else {
            idQuery = QueryBuilders.idsQuery(type).addIds(ids.toArray(new String[ids.size()])).toString();
        }

        delete(idQuery, type, userToken);
    }

    @Override
    public void delete(String query, String type, EzSecurityToken userToken) throws TException {
        final Stopwatch watch = LoggingUtils.createStopWatch();

        final VisibilityFilterConfig filterConfig =
                new VisibilityFilterConfig(VISIBILITY_FIELD, EnumSet.of(Permission.READ, Permission.WRITE));

        final DeleteByQueryRequestBuilder builder = client.prepareDeleteByQuery(indexName).setQuery(
                QueryBuilders.filteredQuery(parseQuery(query), getVisibilityFilter(userToken, filterConfig)));

        if (!StringUtils.isEmpty(type)) {
            builder.setTypes(type);
        }
        builder.get();
        // Refresh immediately on deletes
        forceIndexRefresh();
        LoggingUtils.stopAndLogStopWatch(logger, watch, "Delete And Refresh");
    }

    @Override
    public long count(Set<String> types, String query, Set<String> filterIds, EzSecurityToken userToken)
            throws TException {
        final Stopwatch watch = LoggingUtils.createStopWatch();
        CountRequestBuilder requestBuilder = client.prepareCount(indexName);

        final VisibilityFilterConfig filterConfig =
                new VisibilityFilterConfig(VISIBILITY_FIELD, EnumSet.of(Permission.DISCOVER));

        final ScriptFilterBuilder visibilityFilter = getVisibilityFilter(userToken, filterConfig);

        if (!types.isEmpty()) {
            requestBuilder = requestBuilder.setTypes(types.toArray(new String[types.size()]));
        }
        if (!filterIds.isEmpty()) {
            if (StringUtils.isNotEmpty(query)) {
                requestBuilder = requestBuilder.setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.boolQuery().must(parseQuery(query)).must(
                                        QueryBuilders.idsQuery().addIds(
                                                filterIds.toArray(new String[filterIds.size()]))), visibilityFilter));
            } else {
                requestBuilder = requestBuilder.setQuery(
                        QueryBuilders.filteredQuery(
                                QueryBuilders.idsQuery().addIds(filterIds.toArray(new String[filterIds.size()])),
                                visibilityFilter));
            }
        } else if (StringUtils.isNotEmpty(query)) {
            requestBuilder = requestBuilder.setQuery(QueryBuilders.filteredQuery(parseQuery(query), visibilityFilter));
        } else {
            requestBuilder = requestBuilder.setQuery(
                    QueryBuilders.filteredQuery(
                            QueryBuilders.matchAllQuery(), visibilityFilter));
        }

        final CountResponse response = requestBuilder.get();

        LoggingUtils.stopAndLogStopWatch(logger, watch, "Count");

        return response.getCount();
    }

    @Override
    public IndexResponse putPercolator(PercolateQuery query, EzSecurityToken userToken) throws TException {
        final Stopwatch watch = LoggingUtils.createStopWatch();

        Authorizations authorizations;
        if (query.isSetAuthorizations()) {
            if (!ElasticUtils.verifyAuthorizations(query.getAuthorizations(), userToken.getAuthorizations())) {
                throw new TException(
                        "Authorizations for the query are not contained within the token");
            }

            authorizations = query.getAuthorizations();
        } else {
            authorizations = userToken.getAuthorizations();
        }

        addVisibilityFilter(query, authorizations);

        final Document doc = new Document(
                ElasticUtils.PERCOLATOR_TYPE, query.getVisibility(), query.getQueryDocument());

        if (StringUtils.isBlank(query.getId())) {
            query.setId(UUID.randomUUID().toString());
        }

        doc.set_id(query.getId());

        IndexResponse response = put(Collections.singletonList(doc)).get(0);

        LoggingUtils.stopAndLogStopWatch(logger, watch, "PutPercolator");

        return response;
    }

    @Override
    public void deletePercolator(String id, EzSecurityToken userToken) throws TException {
        final Stopwatch watch = LoggingUtils.createStopWatch();

        DocumentIdentifier docId = new DocumentIdentifier().setId(id).setType(ElasticUtils.PERCOLATOR_TYPE);
        if (isVisible(docId, userToken)) {
            // NOTE: This purposefully uses a direct deletion instead of delegating to the delete() method in this
            // class. This is due to the fact that the delete() method uses a delete-by-query (with a visibility
            // filter), and Elasticsearch only deletes the percolator queries from its cache when using a direct
            // deletion.
            client.prepareDelete().setIndex(indexName).setId(id).setType(PERCOLATOR_TYPE).get();

            forceIndexRefresh(); // Refresh immediately on deletes
        }

        LoggingUtils.stopAndLogStopWatch(logger, watch, "DeletePercolator And Refresh");
    }

    @Override
    public List<PercolateQuery> percolate(List<Document> docs, boolean postFilter, EzSecurityToken userToken)
            throws TException {
        final Stopwatch watch = LoggingUtils.createStopWatch();
        List<PercolateQuery> results = new ArrayList<>();
        final MultiPercolateRequestBuilder multiPercolate = client.prepareMultiPercolate();
        List<String> docIds = new ArrayList<>();
        int docIndex = 0;

        for (final Document doc : docs) {
            if (doc.getPercolate() != null) {
                JSONObject docJson = new JSONObject(doc.get_jsonObject());
                try {
                    docJson.put(VISIBILITY_FIELD, serializeToBase64(doc.getVisibility()));
                    multiPercolate.add(
                            new PercolateRequestBuilder(client).setDocumentType(doc.get_type()).setIndices(indexName)
                                    .setSize(doc.getPercolate().getMaxMatches()).setPercolateDoc(
                                    new PercolateSourceBuilder.DocBuilder().setDoc(docJson.toString())));
                    docIds.add(docIndex, doc.get_id());
                    docIndex++;
                } catch (TException e) {
                    logger.warn("When percolating, could not serialize visibility for doc with id:" + doc.get_id());
                }
            }
        }

        docIndex = 0;
        final MultiPercolateResponse.Item[] responses = multiPercolate.get().getItems();
        for (final MultiPercolateResponse.Item response : responses) {
            PercolateResponse percolateResponse = response.getResponse();
            logger.debug(
                    "PercolateByIds: Response Time: {} ms, Shards: [{} succ {} fail {} total], match count = {}",
                    percolateResponse.getTookInMillis(), percolateResponse.getSuccessfulShards(),
                    percolateResponse.getFailedShards(), percolateResponse.getTotalShards(),
                    percolateResponse.getCount());

            if (!response.isFailure()) {
                for (final PercolateResponse.Match match : percolateResponse.getMatches()) {
                    PercolateQuery percolateQuery = new PercolateQuery();
                    percolateQuery.setId(match.getId().toString());
                    percolateQuery.setMatchingDocId(docIds.get(docIndex));
                    results.add(percolateQuery);
                }
            }
            docIndex++;
        }

        if (postFilter) {
            results = filterPercolateQueriesByVisibility(results, userToken);
        }

        LoggingUtils.stopAndLogStopWatch(logger, watch, "Percolate");

        return results;
    }

    @Override
    public List<PercolateQuery> percolateByIds(
            List<String> ids, String type, int maxMatches, boolean postFilter, EzSecurityToken userToken)
            throws TException {
        final Stopwatch watch = LoggingUtils.createStopWatch();
        List<PercolateQuery> results = new ArrayList<>();
        final MultiPercolateRequestBuilder multiPercolate = client.prepareMultiPercolate();

        for (final String id : ids) {
            multiPercolate.add(
                    new PercolateRequestBuilder(client).setIndices(indexName).setDocumentType(type).setSize(maxMatches)
                            .setGetRequest(Requests.getRequest(indexName).type(type).id(id)));
        }

        int docIndex = 0;
        final MultiPercolateResponse.Item[] responses = multiPercolate.get().getItems();
        for (final MultiPercolateResponse.Item response : responses) {
            PercolateResponse percolateResponse = response.getResponse();
            logger.debug(
                    "PercolateByIds: Response Time: {} ms, Shards: [{} succ {} fail {} total], match count = {}",
                    percolateResponse.getTookInMillis(), percolateResponse.getSuccessfulShards(),
                    percolateResponse.getFailedShards(), percolateResponse.getTotalShards(),
                    percolateResponse.getCount());

            if (!response.isFailure()) {
                for (final PercolateResponse.Match match : percolateResponse.getMatches()) {
                    PercolateQuery percolateQuery = new PercolateQuery();
                    percolateQuery.setId(match.getId().toString());
                    percolateQuery.setMatchingDocId(ids.get(docIndex));
                    results.add(percolateQuery);
                }
            }
            docIndex++;
        }

        if (postFilter) {
            results = filterPercolateQueriesByVisibility(results, userToken);
        }

        LoggingUtils.stopAndLogStopWatch(logger, watch, "PercolateByIds");

        return results;
    }

    @Override
    public void setTypeMapping(String type, String mappingJson) {
        final boolean indexExists = client.admin().indices().prepareExists(indexName).get().isExists();
        if (StringUtils.isBlank(mappingJson) && indexExists) {
            client.admin().indices().prepareDeleteMapping().setIndices(indexName).setType(type).get();
        } else {
            if (indexExists) {
                client.admin().indices().preparePutMapping().setIndices(indexName).setType(type).setSource(mappingJson)
                        .get();
            } else {
                client.admin().indices().prepareCreate(indexName).addMapping(type, mappingJson).get();
            }
        }
    }

    @Override
    public void applySettings(String settingsJson) {
        if (client.admin().indices().prepareExists(indexName).get().isExists()) {
            client.admin().indices().prepareUpdateSettings().setSettings(settingsJson).setIndices(indexName).get();
        } else {
            client.admin().indices().prepareCreate(indexName).setSettings(settingsJson).get();
        }
    }

    @Override
    public boolean ping() {
        return isClusterHealthy(client);
    }

    @Override
    public void openIndex() {
        if (indexExists(indexName)) {
            client.admin().indices().prepareOpen(indexName).get();
            // make sure our index is hot
            client.admin().cluster().prepareHealth(indexName).setTimeout("1m").setWaitForGreenStatus().get();
        }
    }

    @Override
    public void closeIndex() {
        if (indexExists(indexName)) {
            client.admin().indices().prepareClose(indexName).get();
        }
    }

    @Override
    public void forceIndexRefresh() {
        refreshIndex(client, indexName);
    }

    @Override
    public PurgeResult purge(long id, Set<Long> toPurge, int batchSize) {
        logger.info("Purging ID {} with size {} with items:\n{}", id, batchSize, toPurge);
        final String scrollId = getScrollId(id, batchSize);
        final SearchResponse response = client.prepareSearchScroll(scrollId).setScroll(PURGE_SCROLL_DURATION).get();

        final BulkRequestBuilder bulkRequest = client.prepareBulk().setRefresh(true);
        final PurgeResult result = new PurgeResult(false);

        final Set<Long> purged = new HashSet<>();
        final Set<Long> unpurged = new HashSet<>();

        for (final SearchHit hit : response.getHits()) {
            final SearchHitField field = hit.field(VISIBILITY_FIELD);
            if (field == null) {
                logger.error("Attempt to read visibility field from object {} failed.", hit.getId());
                continue;
            }

            final Visibility vis;
            try {
                vis = ThriftUtils.deserializeFromBase64(Visibility.class, field.getValue().toString());
            } catch (final TException te) {
                logger.error("Deserializing visibility during a purge of id {} failed.", hit.getId());
                continue;
            }

            if (vis.isSetAdvancedMarkings() && vis.getAdvancedMarkings().isSetId()) {
                final long purgeId = vis.getAdvancedMarkings().getId();
                if (toPurge.contains(purgeId)) {
                    if (vis.getAdvancedMarkings().isComposite()) {
                        logger.info("Composite item cannot be purged {}/{}", hit.getType(), hit.getId());
                        unpurged.add(purgeId);
                    } else {
                        logger.info("Purging item {}/{}", hit.getType(), hit.getId());
                        purged.add(purgeId);
                        bulkRequest.add(client.prepareDelete(indexName, hit.getType(), hit.getId()));
                    }
                }
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            bulkRequest.get();
        }

        result.setPurged(purged);
        result.setUnpurged(unpurged);
        if (response.getHits().getHits().length == 0) {
            result.setIsFinished(true);
        }

        return result;
    }

    @Override
    public CancelStatus cancelPurge(long purgeId) {
        // Delete purge scroll
        client.prepareDelete(PURGE_INDEX, PURGE_TYPE, getElasticPurgeId(purgeId)).get();

        return CancelStatus.CANCELED;
    }

    private List<PercolateQuery> filterPercolateQueriesByVisibility(
            List<PercolateQuery> percolateQueryList, EzSecurityToken userToken) throws TException {
        List<PercolateQuery> percolateQueriesToReturn = new ArrayList<>();
        Map<String, List<PercolateQuery>> percolatorIdToDocumentHit = new HashMap<>();
        Set<String> percolatorIds = new HashSet<>();
        for (PercolateQuery percolateQuery : percolateQueryList) {
            List<PercolateQuery> docHits = percolatorIdToDocumentHit.get(percolateQuery.getId());
            if (docHits == null) {
                docHits = new ArrayList<>();
            }

            docHits.add(percolateQuery);
            percolatorIdToDocumentHit.put(percolateQuery.getId(), docHits);
            percolatorIds.add(percolateQuery.getId());
        }

        List<Document> documents = get(percolatorIds, PERCOLATOR_TYPE, userToken);

        for (Document document : documents) {
            List<PercolateQuery> documentIdHit = percolatorIdToDocumentHit.get(document.get_id());
            percolateQueriesToReturn.addAll(documentIdHit);
        }

        return percolateQueriesToReturn;
    }

    private String getElasticPurgeId(long purgeId) {
        return String.format("%s_%s", applicationName, purgeId);
    }

    private String getScrollId(long id, int batchSize) {
        String scrollId;
        final String elasticPurgeId = getElasticPurgeId(id);
        final GetResponse scrollIdObject = client.prepareGet(PURGE_INDEX, PURGE_TYPE, elasticPurgeId).get();
        if (scrollIdObject.isExists()) {
            scrollId = scrollIdObject.getSource().get("sid").toString();
        } else {
            final SearchResponse scrollResponse =
                    client.prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery())
                            .setScroll(PURGE_SCROLL_DURATION).setSize(batchSize).setSearchType(SCAN)
                            .addFields(VISIBILITY_FIELD).get();

            scrollId = scrollResponse.getScrollId();
            client.prepareIndex(PURGE_INDEX, PURGE_TYPE, elasticPurgeId).setSource("{\"sid\":\"" + scrollId + "\"}")
                    .get();
        }
        return scrollId;
    }

    private List<Document> getDocsFromResult(SearchHits hits) throws TException {
        final List<Document> results = new ArrayList<>();
        for (final SearchHit hit : hits) {
            results.add(convertElasticSearchHit(hit));
        }
        return results;
    }

    private Document convertElasticSearchHit(SearchHit searchHit) throws TException {
        final Map<String, Object> source;
        Visibility visibility = new Visibility();
        if (searchHit.isSourceEmpty()) {
            source = new HashMap<>();
            for (final Map.Entry<String, SearchHitField> entry : searchHit.getFields().entrySet()) {
                if (entry.getKey().equals(VISIBILITY_FIELD)) {
                    visibility =
                            ThriftUtils.deserializeFromBase64(Visibility.class, entry.getValue().getValue().toString());
                } else {
                    source.put(entry.getKey(), entry.getValue().value());
                }
            }
        } else {
            source = searchHit.getSource();
            visibility =
                    ThriftUtils.deserializeFromBase64(Visibility.class, source.remove(VISIBILITY_FIELD).toString());
        }

        final Document document = new Document();
        document.set_id(searchHit.getId());
        document.set_type(searchHit.getType());
        document.set_jsonObject(gson.toJson(source));
        document.set_version(searchHit.getVersion());
        document.setVisibility(visibility);
        return document;
    }

    private boolean isVisible(DocumentIdentifier id, EzSecurityToken token) {
        try {
            ensureVisible(id, token);
            return true;
        } catch (TException e) {
            return false;
        }
    }

    private void ensureVisible(DocumentIdentifier id, EzSecurityToken token) throws TException {
        Document foundDoc;
        try {
            foundDoc = get(id.getId(), id.getType(), null, token);
        } catch (final TException e) {
            final String errMsg = "There was an error finding the given id " + id;
            logger.error(errMsg, e);
            throw new TException(errMsg, e);
        }

        if (EzElasticHandler.BLANK_DOCUMENT.equals(foundDoc)) {
            final String errMsg = "There was an attempt to get a document by ID that was not present: " + id;
            logger.error(errMsg);
            throw new TException(errMsg);
        }
    }

    private boolean indexExists(String index) {
        return client.admin().indices().prepareExists(index).get().isExists();
    }

    private void applyDefaultMapping() {
        try {
            final XContentBuilder template = jsonBuilder();
            template.startObject();
            template.field("template", "*");
            template.startObject("mappings");
            template.startObject("_default_");
            template.startObject("properties");
            template.startObject(VISIBILITY_FIELD);
            template.field("type", "string");
            template.field("index", "not_analyzed");
            template.endObject();
            template.endObject();
            template.endObject(); // end _default_
            template.startObject(PERCOLATOR_TYPE);
            template.startObject("properties");
            template.startObject(VISIBILITY_FIELD);
            template.field("type", "string");
            template.field("index", "not_analyzed");
            template.endObject(); // end visibility field
            template.startObject("query");
            template.field("type", "object");
            template.field("enabled", false);
            template.endObject(); // end query
            template.endObject(); // end properties
            template.endObject(); // end percolator

            template.startObject("percolatorInbox");
            template.startObject("properties");
            template.startObject(VISIBILITY_FIELD);
            template.field("type", "string");
            template.field("index", "not_analyzed");
            template.endObject(); // end visibility field
            template.startObject("lastChecked");
            template.field("type", "date");
            template.endObject(); // end visibility field
            template.startObject("hits");
            template.startObject("properties");
            template.startObject("docId");
            template.field("type", "string");
            template.endObject(); // end docId
            template.startObject("percolatorId");
            template.field("type", "string");
            template.endObject(); // end percolatorId
            template.startObject("timeOfIngest");
            template.field("type", "date");
            template.endObject(); // end timeOfIngest
            template.endObject(); // end properties
            template.endObject(); // end hits
            template.endObject(); // end properties
            template.endObject(); // end percolatorInbox

            template.endObject(); // end mappings
            template.endObject(); // end top-level

            client.admin().indices().preparePutTemplate("elastic_security").setSource(template).get();

            final String versionedIndex = String.format("%s_v%d", indexName, version);

            // If there is already an index called 'applicationName' then we have no choice but to use it.
            // Once the application migrates to a versioned index we can start using that
            if (!indexExists(indexName)) {
                // Create the versioned index if it does not exist
                if (!client.admin().indices().prepareExists(versionedIndex).get().isExists()) {
                    client.admin().indices().prepareCreate(versionedIndex).get();
                    client.admin().indices().prepareAliases().addAlias(versionedIndex, indexName).get();
                }
            }

            if (!indexExists(PURGE_INDEX)) {
                client.admin().indices().prepareCreate(PURGE_INDEX).get();
            }
        } catch (final IOException e) {
            throw new ElasticsearchException("Could not create mapping template", e);
        }
    }
}
