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

package ezbake.data.mongo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.mongodb.*;
import com.mongodb.util.JSON;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.base.EzbakeBaseDataService;
import ezbake.data.base.thrift.PurgeItems;
import ezbake.data.base.thrift.PurgeOptions;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.data.common.TokenUtils;
import ezbake.data.common.classification.ClassificationUtils;
import ezbake.data.mongo.conversion.MongoConverter;
import ezbake.data.mongo.driver.thrift.*;
import ezbake.data.mongo.helper.MongoFindHelper;
import ezbake.data.mongo.helper.MongoInsertHelper;
import ezbake.data.mongo.helper.MongoUpdateHelper;
import ezbake.data.mongo.redact.RedactHelper;
import ezbake.data.mongo.thrift.*;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;
import ezbakehelpers.ezconfigurationhelpers.mongo.MongoConfigurationHelper;
import ezbakehelpers.mongoutils.MongoHelper;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.bson.types.ObjectId;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

@SuppressWarnings({"rawtypes", "unchecked"})
public class EzMongoHandler extends EzbakeBaseDataService implements EzMongoDriverService.Iface {

    MongoDriverHandler driver;

    private final Logger appLog = LoggerFactory.getLogger(EzMongoHandler.class);
    private Mongo mongo;
    protected String appName;
    private String appId;
    protected String dbName;
    private MongoConfigurationHelper mongoConfigurationHelper;
    private MongoFindHelper mongoFindHelper;
    private MongoInsertHelper mongoInsertHelper;
    private MongoUpdateHelper mongoUpdateHelper;
    private static final int DEFAULT_MONGODB_PORT = 27017;
    protected DB db;

    public final static String MONGODB_APP_DATABASE_NAME = "mongodb.app.database.name";
    public final static String READ_OPERATION = "read";
    public final static String WRITE_OPERATION = "write";
    public final static String DISCOVER_OPERATION = "discover";
    public final static String MANAGE_OPERATION = "manage";

    // Metrics
    private static final String PURGE_TIMER_NAME = MetricRegistry.name(EzMongoHandler.class, "PURGE");
    private static final String FIND_TIMER_NAME = MetricRegistry.name(EzMongoHandler.class, "FIND");
    private static final String UPDATE_DOCUMENT_TIMER_NAME = MetricRegistry.name(EzMongoHandler.class, "UPDATE", "DOCUMENT");
    private static final String INSERT_TIMER_NAME = MetricRegistry.name(EzMongoHandler.class, "INSERT");
    private static final String REMOVE_TIMER_NAME = MetricRegistry.name(EzMongoHandler.class, "REMOVE");

    //purge tracking collection name
    private static final String PURGE_TRACKING_COLL_NAME = "purgetracker";

    @SuppressWarnings("resource")
    public void init() throws Exception {
        final Properties properties = getConfigurationProperties();
        mongoConfigurationHelper = new MongoConfigurationHelper(properties);
        appLog.info("ezmongo configuration: {}", properties);

        // we append the appName to the collection names to keep the apps separate within Mongo
        appName = properties.getProperty(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME);
        appId = properties.getProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID);

        // see if we are connecting to MongoDB with username/password
        final String username = mongoConfigurationHelper.getMongoDBUserName();
        final String password = mongoConfigurationHelper.getMongoDBPassword();
        final String hostname = mongoConfigurationHelper.getMongoDBHostName();
        dbName = mongoConfigurationHelper.getMongoDBDatabaseName();

        appLog.debug("Mongo username/password: {}/{}", username, password);
        appLog.debug("Mongo host(s): {}", hostname);
        appLog.debug("Mongo dbName: {}", dbName);

        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            MongoHelper mongoHelper = new MongoHelper(properties);
            mongo = mongoHelper.getMongo();
        } else {
            // connect to MongoDB that is not username/password protected
            mongo = new MongoClient(hostname);
        }
        db = mongo.getDB(dbName);

        mongoFindHelper = new MongoFindHelper(this);
        mongoInsertHelper = new MongoInsertHelper(appId);
        mongoUpdateHelper = new MongoUpdateHelper(this);

        //create purge tracking collection
        createPurgeTracker();

        //init metrics and audit logger
        initMetrics();
        initAuditLogger(EzMongoHandler.class);

        // TODO: add caching to Mongo
        // cacheManager = (CacheManager)context.getBean("cacheManager");
    }

    /**
     * Create Purge Tracker Collection to Track the purge activity
     * @throws EzMongoBaseException
     *
     */
    void createPurgeTracker() throws EzMongoBaseException {
        try {
            final String finalCollectionName = getCollectionName(PURGE_TRACKING_COLL_NAME);

            if ( !db.collectionExists(finalCollectionName) ) {
                db.createCollection(finalCollectionName, new BasicDBObject());
            }

            appLog.info("in createPurgeTracker, created collection: {}", finalCollectionName);
        } catch (final Exception e) {
            throw enrichException("createPurgeTracker", e);
        }
    }

    /**
     * Initialize our meters & timers so that we can gather statistics for EzBake
     *
     */
    private void initMetrics() {
        final MetricRegistry mr = getMetricRegistry();

        // Timers
        final Timer purgeTimer = mr.timer(EzMongoHandler.PURGE_TIMER_NAME);
        final Timer findTimer = mr.timer(EzMongoHandler.FIND_TIMER_NAME);
        final Timer updateDocTimer = mr.timer(EzMongoHandler.UPDATE_DOCUMENT_TIMER_NAME);
        final Timer insertTimer = mr.timer(EzMongoHandler.INSERT_TIMER_NAME);
        final Timer removeTimer = mr.timer(EzMongoHandler.REMOVE_TIMER_NAME);

        // Meters
        //final Meter getMeter = mr.meter(EzElasticHandler.GET_METER_NAME);
        //final Meter queryMeter = mr.meter(EzElasticHandler.QUERY_METER_NAME);
    }

    private List<ServerAddress> parseMongoServerString(String mongoServers) {
        final List<ServerAddress> servers = new ArrayList<ServerAddress>();

        for (final String server : mongoServers.split(",")) {
            final String[] tokens = server.split(":");
            final String host = tokens[0];
            int port = DEFAULT_MONGODB_PORT;
            try {
                if (tokens.length == 2) {
                    port = Integer.parseInt(tokens[1]);
                }
                servers.add(new ServerAddress(host, port));
            } catch (final NumberFormatException e) {
                appLog.error("Unable to parse port number from server string({})", server);
            } catch (final java.net.UnknownHostException e) {
                appLog.error("Unknown host exception when parsing server string({})", server);
            }
        }

        return servers;
    }

    @Override
    public boolean ping() {
        boolean result = true;
        appLog.info("ping!");

        try {
            db.getCollectionNames();
        } catch (final Exception e) {
            result = false;
        }

        return result;
    }

    @Override
    public TProcessor getThriftProcessor() {
        try {

            init();
            driver = new MongoDriverHandler(this);
            driver.init();
            return new EzMongoDriverService.Processor(this);
        } catch (final Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public MongoFindHelper getMongoFindHelper() {
        return mongoFindHelper;
    }

    public MongoInsertHelper getMongoInsertHelper() {
        return mongoInsertHelper;
    }

    public MongoUpdateHelper getMongoUpdateHelper() {
        return mongoUpdateHelper;
    }

    @Override
    public boolean authenticate_driver(EzSecurityToken ezSecurityToken) throws TException {
        return true;
    }

    protected void auditLog(EzSecurityToken userToken, AuditEventType eventType, Map<String, String> argsMap) {
        AuditEvent event = new AuditEvent(eventType, userToken);

        for (Map.Entry<String, String> entry : argsMap.entrySet())
        {
            event.arg(entry.getKey(), entry.getValue());
        }
        logEvent(event);
    }

    protected String printMongoObject(Object obj) {
        return obj == null ? "null" : obj.toString();
    }

    @Override
    public ResultsWrapper aggregate_driver(String collection, EzAggregationRequest ezAggregationRequest, EzSecurityToken token) throws TException, EzMongoDriverException {
        return driver.aggregate_driver(collection,ezAggregationRequest,token);
    }

    protected void setResponseObjectWithCursor(ResultsWrapper rw, Cursor cursor) throws IOException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        new ObjectOutputStream(bOut).writeObject(cursor);
        rw.setResponseData(bOut.toByteArray());
    }

    private boolean isNotSystemCollection(String collection) {
        return !collection.equals("$cmd") && !collection.equals("system.indexes") &&
                !collection.equals("fs.chunks") && !collection.equals("fs.files") && !collection.equals("system.namespaces");
    }

    @Override
    public ResultsWrapper find_driver(String collection, EzFindRequest ezFindRequest, EzSecurityToken token) throws TException, EzMongoDriverException {
        return driver.find_driver(collection, ezFindRequest, token);
    }

    @Override
    public EzWriteResult insert_driver(String collection, EzInsertRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {
        return driver.insert_driver(collection,req,token);
    }

    @Override
    public EzWriteResult update_driver(String collection, EzUpdateRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {
        return driver.update_driver(collection, req, token);
    }

    @Override
    public ResultsWrapper drop_driver(String collection, EzSecurityToken token) throws TException, EzMongoDriverException {
        return driver.drop_driver(collection,token);
    }

    @Override
    public EzWriteResult createIndex_driver(String collection, EzCreateIndexRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {
        return driver.createIndex_driver(collection, req, token);
    }

    @Override
    public EzGetMoreResponse getMore_driver(String collection, EzGetMoreRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {
        return driver.getMore_driver(collection, req, token);
    }

    @Override
    public EzParallelScanResponse parallelScan_driver(String collection, EzParallelScanOptions options, EzSecurityToken token) throws EzMongoDriverException, TException {
        return driver.parallelScan_driver(collection, options, token);
    }

    @Override
    public EzWriteResult remove_driver(String collection, EzRemoveRequest req, EzSecurityToken token) throws EzMongoDriverException, TException {
        return driver.remove_driver(collection, req, token);
    }

    @Override
    public int getMaxBsonObjectSize_driver(EzSecurityToken token) throws EzMongoDriverException, TException {
        return driver.getMaxBsonObjectSize_driver(token);
    }

    private void addResponseException(EzGetMoreResponse rw, Exception ex) throws TException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(bOut).writeObject(ex);
            rw.setMongoexception(bOut.toByteArray());
        } catch (Exception e) {
            throw new TException(e);
        }
    }


    protected ByteArrayOutputStream addDBCursorResult(List<DBObject> list) throws IOException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        new ObjectOutputStream(bOut).writeObject(list);
        return bOut;
    }

    /**** Below are 1.3.x Mongo Dataset methods (with the IDL structs changes) ****/

    public String getCollectionName(String collectionName) {
        return appName + "_" + collectionName;
    }

    public DB getDb() {
        return db;
    }

    @Override
    public void ensureIndex(String collectionName, String jsonKeys, String jsonOptions, EzSecurityToken security)
            throws TException, EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "ensureIndex");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("jsonKeys", jsonKeys);
            auditParamsMap.put("jsonOptions", jsonOptions);
            auditLog(security, AuditEventType.FileObjectCreate, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final String finalCollectionName = getCollectionName(collectionName);

            final DBObject indexKeys = (DBObject) JSON.parse(jsonKeys);

            if (!StringUtils.isEmpty(jsonOptions)) {
                final DBObject indexOptions = (DBObject) JSON.parse(jsonOptions);
                db.getCollection(finalCollectionName).ensureIndex(indexKeys, indexOptions);
            } else {
                db.getCollection(finalCollectionName).ensureIndex(indexKeys);
            }

            appLog.info("ensured index with keys: {}, options: {}", jsonKeys, jsonOptions);

        } catch (final Exception e) {
            throw enrichException("ensureIndex", e);
        }
    }

    /**
     * Create MongoDB indexes for the given fields in some collection.
     *
     * @param collectionName The Name of the collection the document belongs to
     * @param jsonKeys The Mongo index keys
     * @param jsonOptions Optional options for the particular fields being indexed.
     * @param security A valid EzSecurity token
     * @throws TException If authentication fails
     * @throws EzMongoBaseException If some other error occurs
     */
    @Override
    public void createIndex(String collectionName, String jsonKeys, String jsonOptions, EzSecurityToken security)
            throws TException, EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "createIndex");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("jsonKeys", jsonKeys);
            auditParamsMap.put("jsonOptions", jsonOptions);
            auditLog(security, AuditEventType.FileObjectCreate, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final String finalCollectionName = getCollectionName(collectionName);

            final DBObject indexKeys = (DBObject) JSON.parse(jsonKeys);

            // The 'options' object is optional
            if (StringUtils.isEmpty(jsonOptions)) {
                db.getCollection(finalCollectionName).createIndex(indexKeys);
            } else {
                final DBObject indexOptions = (DBObject) JSON.parse(jsonOptions);
                // check if "ns" and "name" fields are not in the jsonOptions - then need to put them in.
                // otherwise, mongodb throws this error:
                // "Cannot authorize inserting into system.indexes documents without a string-typed \"ns\" field."
                String ns = (String) indexOptions.get("ns");
                if (ns == null) {
                    ns = dbName + "." + finalCollectionName;

                    indexOptions.put("ns", ns);

                    appLog.info("putting index's ns as : {}", ns);
                }
                String name = (String) indexOptions.get("name");
                if (name == null) {
                    name = "";

                    final Set<String> keySet = indexKeys.keySet();
                    for (final String key : keySet) {
                        final Object keyValue = indexKeys.get(key);

                        if (name.length() > 0) {
                            name += "_";
                        }
                        name += key + "_" + keyValue.toString();
                    }

                    indexOptions.put("name", name);
                    appLog.info("putting index's name as : {}", name);
                }

                db.getCollection(finalCollectionName).createIndex(indexKeys, indexOptions);
            }
            appLog.info("created index with keys: {}, options: {}", jsonKeys, jsonOptions);

        } catch (final Exception e) {
            throw enrichException("createIndex", e);
        }
    }

    @Override
    public List<String> getIndexInfo(String collectionName, EzSecurityToken security) throws TException,
            EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "getIndexInfo");
            auditParamsMap.put("collectionName", collectionName);
            auditLog(security, AuditEventType.FileObjectAccess, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final String finalCollectionName = getCollectionName(collectionName);

            final List<DBObject> indexList = db.getCollection(finalCollectionName).getIndexInfo();
            final List<String> results = new ArrayList<String>();

            for (final DBObject index : indexList) {
                results.add(JSON.serialize(index));
            }

            appLog.info("got index info results: {}", results);

            return results;
        } catch (final Exception e) {
            throw enrichException("getIndexInfo", e);
        }
    }

    @Override
    public String insert(String collectionName, MongoEzbakeDocument mongoDocument,
                         EzSecurityToken security) throws TException, EzMongoBaseException {
        try {
            String document = mongoDocument.jsonDocument;
            Visibility vis = mongoDocument.getVisibility();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "insert");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("mongoDocument", mongoDocument.toString());
            auditLog(security, AuditEventType.FileObjectCreate, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final DBObject dbObject = MongoConverter.toDBObject(document);

            // check if the user can see the document visibility
            mongoInsertHelper.checkAbilityToInsert(security, vis, dbObject, null, false, false);

            final String finalCollectionName = getCollectionName(collectionName);

            Timer.Context context = getMetricRegistry().getTimers().get(INSERT_TIMER_NAME).time();
            try {
                db.getCollection(finalCollectionName).insert(dbObject);
            } finally {
                context.stop();
            }

            final String id = ((ObjectId) dbObject.get("_id")).toString();

            appLog.info("inserted - id: {}", id);

            return id;
        } catch (final Exception e) {
            throw enrichException("insert", e);
        }
    }

    public void checkAbilityToInsert(EzSecurityToken security, Visibility vis, DBObject newObject,
                                     DBObject existingObject, boolean isManageOperation, boolean fromDriver)
            throws VisibilityParseException, EzMongoBaseException {

        validateRequiredFieldsForInsert(vis, newObject, fromDriver);
        validateFormalVisibilityForInsert(security, vis, newObject, fromDriver);
        validateExternalCommunityVisibilityForInsert(security, vis, newObject, fromDriver);
        if (isManageOperation) {
            validatePlatformVisibilitiesForManagingExistingDBObject(security, vis, existingObject, fromDriver);
        }
        validatePlatformVisibilitiesForInsertingNewDBObject(security, vis, newObject, fromDriver);
    }

    private void validatePlatformVisibilitiesForManagingExistingDBObject(EzSecurityToken security, Visibility vis,
                                                                         DBObject existingObject, boolean fromDriver) throws EzMongoBaseException {
        // to further check if we can "Manage",
        // get the intersection of user's Platform auths with the doc's "Manage" visibilities
        Set<Long> platformAuths = security.getAuthorizations().getPlatformObjectAuthorizations();
        Set<Long> platformViz = null;
        if (fromDriver) {
            // mongodb has this field saved as a List - convert it to Set.
            List manageVizList = (List)existingObject.get(RedactHelper.PLATFORM_OBJECT_MANAGE_VISIBILITY_FIELD);
            if (manageVizList != null) {
                platformViz = new HashSet<Long>(manageVizList);
            }
        } else {
            if (vis.isSetAdvancedMarkings() && vis.getAdvancedMarkings().isSetPlatformObjectVisibility()) {
                platformViz = vis.getAdvancedMarkings().getPlatformObjectVisibility().getPlatformObjectManageVisibility();
            }
        }

        Set<Long> origPlatformViz = null;
        if (platformAuths != null && platformViz != null) {
            origPlatformViz = new HashSet<Long>(platformViz);
            platformViz.retainAll(platformAuths);
        }

        final boolean canManageViz = platformViz == null || platformViz.size() > 0;
        if (!canManageViz) {
            // reset the platformViz to the original value for logging purposes
            existingObject.put(RedactHelper.PLATFORM_OBJECT_MANAGE_VISIBILITY_FIELD, origPlatformViz);
            final String message =
                    "User does not have all the required Platform auths to manage the existing DBObject: " + platformAuths
                            + ", platformViz needed: " + origPlatformViz;
            appLog.error(message);
            throw new EzMongoBaseException(message);
        }
    }

    private void validatePlatformVisibilitiesForInsertingNewDBObject(EzSecurityToken security, Visibility vis, DBObject newObject,
                                                                     boolean fromDriver) throws EzMongoBaseException {
        // to further check if we can insert the new mongo doc,
        // get the intersection of user's Platform auths with the doc's "Write" visibilities
        Set<Long> platformAuths = security.getAuthorizations().getPlatformObjectAuthorizations();
        Set<Long> platformViz = null;
        if (fromDriver) {
            platformViz = (Set<Long>)newObject.get(RedactHelper.PLATFORM_OBJECT_WRITE_VISIBILITY_FIELD);
        } else {
            if (vis.isSetAdvancedMarkings() && vis.getAdvancedMarkings().isSetPlatformObjectVisibility()) {
                platformViz = vis.getAdvancedMarkings().getPlatformObjectVisibility().getPlatformObjectWriteVisibility();
            }
        }

        Set<Long> origPlatformViz = null;
        if (platformAuths != null && platformViz != null) {
            origPlatformViz = new HashSet<Long>(platformViz);
            platformViz.retainAll(platformAuths);
        }

        final boolean canInsertViz = platformViz == null || platformViz.size() > 0;
        if (!canInsertViz) {
            // reset the platformViz to the original value for logging purposes
            newObject.put(RedactHelper.PLATFORM_OBJECT_WRITE_VISIBILITY_FIELD, origPlatformViz);
            final String message =
                    "User does not have all the required Platform auths to insert: " + platformAuths
                            + ", platformViz needed: " + origPlatformViz;
            appLog.error(message);
            throw new EzMongoBaseException(message);
        }
    }

    private void validateExternalCommunityVisibilityForInsert(EzSecurityToken security, Visibility vis, DBObject dbObject, boolean fromDriver) throws EzMongoBaseException, VisibilityParseException {
        if (fromDriver) {
            Object extCVFieldObj = dbObject.get(RedactHelper.EXTERNAL_COMMUNITY_VISIBILITY_FIELD);
            if (extCVFieldObj == null) {
                return;
            }
            // we already have set the _ezExtV field with the double array [[ ]] format.
            //   iterate through the inner list elements and see if the token's auths have
            //   any of them as a whole.
            boolean canInsertExtViz = false;
            List outerList = (List)extCVFieldObj;
            Set<String> tokenAuths = security.getAuthorizations().getExternalCommunityAuthorizations();
            for (Object innerListObj : outerList) {
                // check if the token auths have all of this inner list element
                List innerList = (List)innerListObj;
                Set<String> innerSet = new HashSet<String>(innerList);
                if (tokenAuths.containsAll(innerSet)) {
                    canInsertExtViz = true;
                    break;
                }
            }
            if (!canInsertExtViz) {
                final String message =
                        "User does not have all the required External Community auths to insert: " + outerList
                                + ", auths List: " + tokenAuths;
                appLog.error(message);
                throw new EzMongoBaseException(message);
            }
        } else {
            if (!vis.isSetAdvancedMarkings()) {
                appLog.info("validateExternalCommunityVisibilityForInsert - AdvancedMarkings is not set.");
                return;
            }
            // check if the user can insert the External Community Visibility (boolean expression).
            String externalCommunityBooleanExpression = vis.getAdvancedMarkings().getExternalCommunityVisibility();
            if (!StringUtils.isEmpty(externalCommunityBooleanExpression)) {
                appLog.info("checking if the user has the required classification to insert: {}", externalCommunityBooleanExpression);

                final boolean canInsertExternalCommunityViz =
                        ClassificationUtils.confirmAuthsForAccumuloClassification(security, externalCommunityBooleanExpression, ClassificationUtils.USER_EXTERNAL_COMMUNITY_AUTHS);

                if (!canInsertExternalCommunityViz) {
                    final String message =
                            "User does not have all the required External Community auths to insert: " + externalCommunityBooleanExpression;
                    appLog.error(message);
                    throw new EzMongoBaseException(message);
                }
            }
        }
    }

    private void validateFormalVisibilityForInsert(
        EzSecurityToken security,
        Visibility vis,
        DBObject dbObject,
        boolean fromDriver) throws EzMongoBaseException, VisibilityParseException {

        if (fromDriver) {
            Object fvFieldObj = dbObject.get(RedactHelper.FORMAL_VISIBILITY_FIELD);
            if (fvFieldObj == null) {
                return;
            }
            // we already have set the _ezFV field with the double array [[ ]] format.
            //   iterate through the inner list elements and see if the token's auths have
            //   any of them as a whole.
            boolean canInsertBooleanExpression = false;
            List outerList = (List)fvFieldObj;
            Set<String> tokenAuths = security.getAuthorizations().getFormalAuthorizations();
            for (Object innerListObj : outerList) {
                // check if the token auths have all of this inner list element
                List innerList = (List)innerListObj;
                Set<String> innerSet = new HashSet<String>(innerList);
                if (tokenAuths.containsAll(innerSet)) {
                    canInsertBooleanExpression = true;
                    break;
                }
            }
            if (!canInsertBooleanExpression) {
                final String message =
                        "User does not have all the required Formal Visibility auths to insert: " + outerList
                                + ", auths List: " + tokenAuths;
                appLog.error(message);
                throw new EzMongoBaseException(message);
            }

        } else {
            // check if the user can insert the Formal Visibility (Boolean expression).
            String classification = vis.getFormalVisibility();
            if (!StringUtils.isEmpty(classification)) {
                appLog.info("checking if the user has the required classification to insert: {}", classification);

                final boolean canInsertBooleanExpression = ClassificationUtils.confirmAuthsForAccumuloClassification(
                    security,
                    classification,
                    ClassificationUtils.USER_FORMAL_AUTHS);

                if (!canInsertBooleanExpression) {
                    final String message =
                            "User does not have all the required classifications to insert: " + classification;
                    appLog.error(message);
                    throw new EzMongoBaseException(message);
                }
            }
        }
    }

    private void validateRequiredFieldsForInsert(Visibility vis, DBObject dbObject, boolean fromDriver) throws VisibilityParseException, EzMongoBaseException {
        // Check if the minimally required security fields exist:
        // at least one of: _ezFV, _ezExtV, _ezObjRV
        if (!fromDriver) {
            RedactHelper.setSecurityFieldsInDBObject(dbObject, vis, appId);
        }
        Object ezFV = dbObject.get(RedactHelper.FORMAL_VISIBILITY_FIELD);
        Object ezExtV = dbObject.get(RedactHelper.EXTERNAL_COMMUNITY_VISIBILITY_FIELD);
        Object ezObjRV = dbObject.get(RedactHelper.PLATFORM_OBJECT_READ_VISIBILITY_FIELD);
        if (ezFV == null && ezExtV == null && ezObjRV == null) {
            throw new EzMongoBaseException("At least one security field (_ezFV, _ezExtV, _ezObjRV) is required for inserts.");
        }
    }

    @Override
    public int remove(String collectionName, String jsonQuery, EzSecurityToken security) throws TException,
            EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "remove");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("jsonQuery", jsonQuery);
            auditLog(security, AuditEventType.FileObjectDelete, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final String finalCollectionName = getCollectionName(collectionName);

            int removedCount = 0;

            // see if we are able to remove the data in db with user's classification in the user token
            final List<DBObject> results =
                    mongoFindHelper.findElements(collectionName, jsonQuery, "{ _id: 1}", null, 0, 0, false, security, false, WRITE_OPERATION);
            if (results.size() > 0) {

                // construct a list of ObjectIds to use as the filter
                final List<ObjectId> idList = new ArrayList<ObjectId>();
                for (final DBObject result : results) {
                    appLog.info("can remove DBObject (_id): {}", result);

                    idList.add((ObjectId) result.get("_id"));
                }

                final DBObject inClause = new BasicDBObject("$in", idList);
                final DBObject query = new BasicDBObject("_id", inClause);

                Timer.Context context = getMetricRegistry().getTimers().get(REMOVE_TIMER_NAME).time();

                try {
                    final WriteResult writeResult = db.getCollection(finalCollectionName).remove(query);

                    appLog.info("removed - write result: {}", writeResult.toString());

                    removedCount = writeResult.getN();
                } finally {
                    context.stop();
                }
            } else {
                appLog.info("Did not find any documents to remove with the query {}", jsonQuery);
            }

            appLog.info("after remove, removedCount: {}", removedCount);

            return removedCount;
        } catch (final Exception e) {
            throw enrichException("remove", e);
        }
    }

    @Override
    public int update(String collectionName, MongoUpdateParams mongoUpdateParams, EzSecurityToken security) throws TException, EzMongoBaseException {
        try {
            String jsonQuery = StringUtils.isEmpty(mongoUpdateParams.jsonQuery) ? "{}" : mongoUpdateParams.jsonQuery;
            String jsonDocument = mongoUpdateParams.mongoDocument.jsonDocument;
            boolean updateWithOperators = mongoUpdateParams.updateWithOperators;
            Visibility vis = mongoUpdateParams.mongoDocument.getVisibility();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "update");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("mongoUpdateParams", mongoUpdateParams.toString());
            auditLog(security, AuditEventType.FileObjectModify, auditParamsMap);

            Timer.Context context = getMetricRegistry().getTimers().get(UPDATE_DOCUMENT_TIMER_NAME).time();
            int updatedCount;
            try {
                updatedCount =
                    mongoUpdateHelper.updateDocument(collectionName, jsonQuery, jsonDocument, updateWithOperators, vis, security);

                appLog.info("after update, updatedCount: {}", updatedCount);
            } finally {
                context.stop();
            }

            return updatedCount;
        } catch (final Exception e) {
            throw enrichException("update", e);
        }
    }

    @Override
    public long getCount(String collectionName, EzSecurityToken security) throws TException, EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "getCount");
            auditParamsMap.put("collectionName", collectionName);
            auditLog(security, AuditEventType.FileObjectAccess, auditParamsMap);

            final String jsonQuery = "{}";
            return getCountFromQuery(collectionName, jsonQuery, security);

        } catch (final Exception e) {
            throw enrichException("getCount", e);
        }
    }

    @Override
    public long getCountFromQuery(String collectionName, String jsonQuery, EzSecurityToken security)
            throws TException, EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "getCountFromQuery");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("jsonQuery", jsonQuery);
            auditLog(security, AuditEventType.FileObjectAccess, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final DBObject queryCommand = (DBObject) JSON.parse(jsonQuery);

            final DBObject query = new BasicDBObject("$match", queryCommand);

            final String finalCollectionName = getCollectionName(collectionName);

            appLog.info("getCountFromQuery, finalCollectionName: {}, {}", finalCollectionName, jsonQuery);

            final DBObject[] aggregationCommandsArray =
                    mongoFindHelper.getFindAggregationCommandsArray_withcounter(0, 0, null, null, security, true, READ_OPERATION);
            final AggregationOutput aggregationOutput =
                    db.getCollection(finalCollectionName).aggregate(query, aggregationCommandsArray);

            final BasicDBList resultsList = (BasicDBList) aggregationOutput.getCommandResult().get("result");
            long count = 0;
            if (resultsList.size() > 0) {
                final BasicDBObject resultsObj = (BasicDBObject) resultsList.get(0);
                count = resultsObj.getLong("count");
            }

            return count;
        } catch (final Exception e) {
            throw enrichException("getCountFromQuery", e);
        }
    }

    @Override
    public List<String> textSearch(String collectionName, String searchText, EzSecurityToken security)
            throws TException, EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "textSearch");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("searchText", searchText);
            auditLog(security, AuditEventType.FileObjectAccess, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final DBObject searchObj = new BasicDBObject("$search", searchText);
            final DBObject textObj = new BasicDBObject("$text", searchObj);
            final DBObject query = new BasicDBObject("$match", textObj);

            final String finalCollectionName = getCollectionName(collectionName);

            appLog.info("textSearch, finalCollectionName: {}, query: {}", finalCollectionName, query);

            final DBObject[] aggregationCommandsArray = mongoFindHelper.getFindAggregationCommandsArray(0, 0, null, null, security, READ_OPERATION);

            final AggregationOutput aggregationOutput =
                    db.getCollection(finalCollectionName).aggregate(query, aggregationCommandsArray);

            final BasicDBList commandResults = (BasicDBList) aggregationOutput.getCommandResult().get("result");
            final List<String> results = new ArrayList<String>();

            if (commandResults != null) {
                for (final Object obj : commandResults) {
                    final DBObject dbo = (DBObject) obj;

                    results.add(JSON.serialize(dbo));
                }
            } else {
                final String message = "Text search command results were null - there probably is no text index.";
                appLog.error(message);
                throw new EzMongoBaseException(message);
            }

            appLog.info("in textSearch, results size: {}", results.size());

            return results;
        } catch (final Exception e) {
            throw enrichException("textSearch", e);
        }
    }



    @Override
    public List<String> find(String collectionName, MongoFindParams mongoFindParams, EzSecurityToken security)
            throws EzMongoBaseException, TException {

        try {
            String jsonQuery = StringUtils.isEmpty(mongoFindParams.jsonQuery) ? "{}" : mongoFindParams.jsonQuery;
            String jsonProjection = mongoFindParams.jsonProjection;
            String jsonSort = mongoFindParams.jsonSort;
            int skip = mongoFindParams.skip;
            int limit = mongoFindParams.limit;
            boolean returnPlainObjectIdString = mongoFindParams.returnPlainObjectIdString;

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "find");
            auditParamsMap.put("collectionName", collectionName);
            auditParamsMap.put("mongoFindParams", printMongoObject(mongoFindParams));
            auditLog(security, AuditEventType.FileObjectAccess, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            Timer.Context context = getMetricRegistry().getTimers().get(FIND_TIMER_NAME).time();
            List<String> results = null;
            try {
                results = mongoFindHelper.findElements(collectionName, jsonQuery, jsonProjection, jsonSort, skip, limit,
                    returnPlainObjectIdString, security, true, READ_OPERATION);
                appLog.info("in find, results size: {}", results.size());
            } finally {
                context.stop();
            }

            return results;
        } catch (final Exception e) {
            throw enrichException("find", e);
        }

    }



    @Override
    public boolean collectionExists(String collectionName, EzSecurityToken security) throws TException,
            EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "collectionExists");
            auditParamsMap.put("collectionName", collectionName);
            auditLog(security, AuditEventType.FileObjectAccess, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final String finalCollectionName = getCollectionName(collectionName);
            final boolean exists = db.collectionExists(finalCollectionName);

            appLog.info("in collectionExists, collection exists: {}", exists);

            return exists;
        } catch (final Exception e) {
            throw enrichException("collectionExists", e);
        }
    }

    @Override
    public void createCollection(String collectionName, EzSecurityToken security) throws TException,
            EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "createCollection");
            auditParamsMap.put("collectionName", collectionName);
            auditLog(security, AuditEventType.FileObjectCreate, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final String finalCollectionName = getCollectionName(collectionName);

            db.createCollection(finalCollectionName, new BasicDBObject());

            appLog.info("in createCollection, created collection: {}", finalCollectionName);
        } catch (final Exception e) {
            throw enrichException("createCollection", e);
        }
    }

    @Override
    public void dropCollection(String collectionName, EzSecurityToken security) throws TException,
            EzMongoBaseException {
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "dropCollection");
            auditParamsMap.put("collectionName", collectionName);
            auditLog(security, AuditEventType.FileObjectDelete, auditParamsMap);

            TokenUtils.validateSecurityToken(security, this.getConfigurationProperties());

            if (StringUtils.isEmpty(collectionName)) {
                throw new EzMongoBaseException("collectionName is required.");
            }

            final String finalCollectionName = getCollectionName(collectionName);
            db.getCollection(finalCollectionName).drop();

            appLog.info("in dropCollection, dropped collection: {}", finalCollectionName);
        } catch (final Exception e) {
            throw enrichException("dropCollection", e);
        }
    }

    @Override
    public List<String> distinct(String collectionName, MongoDistinctParams mongoDistinctParams, EzSecurityToken securityToken) throws TException, EzMongoBaseException {
        List<String> results = new ArrayList<>();

        String field = mongoDistinctParams.getField();
        String jsonQuery = mongoDistinctParams.getQuery();

        if (StringUtils.isEmpty(jsonQuery)) {
            jsonQuery = "{ }";
        }
        try {
            // call the redact pipeline
            List<DBObject> findResults = mongoFindHelper.findElements(collectionName, jsonQuery, null, null, 0, 0,
                    false, securityToken, false, READ_OPERATION);

            if (findResults.size() > 0) {
                // construct a list of ObjectIds to use as the filter
                final List<ObjectId> idList = new ArrayList<ObjectId>();
                for (final DBObject findResult : findResults) {
                    appLog.info("can find DBObject (_id): {}", findResult);

                    idList.add((ObjectId) findResult.get("_id"));
                }

                final DBObject inClause = new BasicDBObject("$in", idList);
                final DBObject query = new BasicDBObject("_id", inClause);
                final String finalCollectionName = getCollectionName(collectionName);

                // get the distinct results from the redacted list
                List distinctResults = db.getCollection(finalCollectionName).distinct(field, query);

                // convert to Strings
                results = mongoFindHelper.addMongoResultsToList(distinctResults, false, true);

                appLog.info("Distinct values for field {}, jsonQuery {}: {}", field, jsonQuery, results);
            }

        } catch (Exception e) {
            throw enrichException("distinct", e);
        }

        return results;
    }

    /* Currently not used - if you want to create indexes dynamically, use the ezmongo CLI.
    private void createIndexes() throws Exception {
        InputStream inputStream = null;
        Resource resource = null;

        try {
            resource = new ClassPathResource("indexes.txt");
            inputStream = resource.getInputStream();
            final Scanner scanner0 = new Scanner(inputStream);
            DBCollection collection = null;

            while (scanner0.hasNext()) {
                final String index = scanner0.nextLine().trim();
                collection = null;

                if (index.startsWith("#") || index.length() == 0 || "".equals(index)) {
                    // skip over the comments or empty lines
                    continue;
                } else {
                    final int spaceIdx = index.indexOf(" ");
                    final String collectionName = index.substring(0, spaceIdx).trim();
                    final String indexJson = index.substring(spaceIdx).trim();

                    if (collectionName.startsWith("{")) {
                        // either someone didn't put in a collection name with the index
                        // JSON or they started the collection name with the "{"
                        // log it and continue
                        appLog.info("Invalid collection name to create index: {}", collectionName);
                        continue;
                    }

                    final String finalCollectionName = getCollectionName(collectionName);
                    if (!mongoTemplate.collectionExists(finalCollectionName)) {
                        collection = mongoTemplate.createCollection(finalCollectionName);
                    } else {
                        collection = mongoTemplate.getCollection(finalCollectionName);
                    }

                    // check the index JSON
                    // TODO: this works but it's probably better to use a regex
                    // to retrieve the parts of the JSON that we want
                    if (indexJson.indexOf("},") != -1) {
                        final String[] tokens = indexJson.split("},");

                        final DBObject keys = retrieveDBObject(tokens[0]);
                        final DBObject optionsIn = retrieveDBObject(tokens[1]);

                        collection.ensureIndex(keys, optionsIn);
                    } else {
                        // only have the keys portion
                        final DBObject keys = retrieveDBObject(indexJson);;
                        collection.ensureIndex(keys);
                    }
                }
            }
        } catch (final IOException ioe) {
            appLog.error("Encountered an IOException: " + ioe.getCause());
        } catch (final Exception e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (final IOException ignored) {
                }
            }
        }
    }

    private DBObject retrieveDBObject(String jsonString) {
        final DBObject dbo = new BasicDBObject();
        String s = jsonString.trim();
        if (s.startsWith("{")) {
            s = s.substring(1);
        }
        if (s.endsWith("}")) {
            s = s.substring(0, s.indexOf("}"));
        }

        final String[] tokens = s.split(",");
        for (final String token : tokens) {
            final String[] keyValue = token.split(":");

            if (keyValue[1].toLowerCase().trim().equalsIgnoreCase("true")
                    || keyValue[1].toLowerCase().trim().equalsIgnoreCase("false")) {
                dbo.put(keyValue[0], keyValue[1]);
            } else {
                dbo.put(keyValue[0], Integer.parseInt(keyValue[1].trim()));
            }
        }

        return dbo;
    }
    */

    /**
     * A method to add detail to the Thrift-serialized Exception.
     *
     * Since the underlying MongoDB instance may throw some unknown instance of java.lang.Exception in our usage, we
     * catch the base type in this handler and send it's classname and message back to the client.
     *
     * @param step
     * @param e
     * @return
     */
    private EzMongoBaseException enrichException(String step, Exception e) {
        final String classname = e.getClass().getCanonicalName(), messageStart =
                "Caught exception " + classname + " in " + step + ": ";
        appLog.error(messageStart, e);
        return new EzMongoBaseException(messageStart + e.getMessage());
    }

    String getCollNameOfPurgeId(long purgeId) {
        String collectionName = null;
        Set purgeIds = new HashSet();
        purgeIds.add(purgeId);

        appLog.info("Get Collection Name of Purge Id {} in Purge Tracker Collection", purgeId);
        DBObject query = new BasicDBObject();
        query.put(RedactHelper.APP_ID_FIELD, appId);
        query.put(RedactHelper.PURGE_ID, new BasicDBObject("$in", purgeIds));
        String trackingPurgeCollName = getCollectionName(PURGE_TRACKING_COLL_NAME);
        DBCursor cursor = db.getCollection(trackingPurgeCollName).find(query);
        if (cursor.length() == 1) {
            appLog.info("Found a Record getting collection name");
            collectionName = (String)cursor.one().get(RedactHelper.PURGE_TRACKING_COLL_FIELD);
        }
        appLog.info("getCollNameOfPurgeId, returning Collection Name {}", collectionName);
        return collectionName;
    }

    void updatePurgeTracker(long purgeId, String purgeCollName, EzSecurityToken token) {
        appLog.info("updatePurgeTracker with Purge Id {} and Collection Name {}", purgeId, purgeCollName);
        String trackingPurgeCollName = getCollectionName(PURGE_TRACKING_COLL_NAME);
        DBObject query = new BasicDBObject();
        query.put(RedactHelper.APP_ID_FIELD, appId);
        query.put(RedactHelper.PURGE_ID, purgeId);
        DBCursor cursor = db.getCollection(trackingPurgeCollName).find(query);
        if ( cursor.length() == 0 ) {
            //insert
            appLog.info("No Records Found for Purge Id {} in Purge Collection {}", purgeId, trackingPurgeCollName);

            try {
                JSONObject jobj = new JSONObject();
                jobj.put(RedactHelper.PURGE_ID, purgeId);
                jobj.put(RedactHelper.PURGE_TRACKING_COLL_FIELD, purgeCollName);
                jobj.put(RedactHelper.APP_ID_FIELD, appId);
                appLog.info("Dumping JSON before Insert : {}",jobj.toString());
                this.insert(PURGE_TRACKING_COLL_NAME, new MongoEzbakeDocument(jobj.toString(),
                        new Visibility().setFormalVisibility("S")), token);

            } catch (TException e) {
                e.printStackTrace();
            } catch (org.codehaus.jettison.json.JSONException je) {
                je.printStackTrace();
                appLog.error("Error updating the purge record!");
            }

        } else {
            //update
            appLog.info("Updating Purge Collection {} with Purge Id", PURGE_TRACKING_COLL_NAME, purgeId);
            DBObject content = new BasicDBObject();
            content.put(RedactHelper.PURGE_IDS_FIELD,purgeId);
            content.put(RedactHelper.PURGE_TRACKING_COLL_FIELD, purgeCollName);
            mongoUpdateHelper.updateContent(getCollectionName(PURGE_TRACKING_COLL_NAME), query, content, false, false);
        }

    }

    @Override
    public PurgeResult purge(PurgeItems items, PurgeOptions options, EzSecurityToken token) throws TException {
        HashMap<String, String> auditParamsMap = new HashMap<>();
        auditParamsMap.put("action", "purge");
        auditParamsMap.put("purgeItems", items.toString());
        auditParamsMap.put("purgeOptions", printMongoObject(options));
        auditLog(token, AuditEventType.FileObjectDelete, auditParamsMap);

        TokenUtils.validateSecurityToken(token, getConfigurationProperties());
        int batchSize = options == null ? 0 : options.getBatchSize();
        final Timer.Context context = getMetricRegistry().getTimers().get(PURGE_TIMER_NAME).time();
        try {
            return purge(items.getPurgeId(), items.getItems(), batchSize, token);
        } finally {
            context.stop();
        }
    }

    private PurgeResult purge(long id, Set<Long> toPurge, int batchSize, EzSecurityToken token) {
        appLog.info("Purging ID {} with batchSize {} with items:\n {}", id, batchSize, toPurge);

        final PurgeResult result = new PurgeResult(false);
        final Set<Long> purged = new HashSet<>();
        final Set<Long> unpurged = new HashSet<>();
        Set<String> collectionNames = db.getCollectionNames();
        //sort the collection names
        List<String> collNamesSortedList = MongoConverter.asSortedList(collectionNames);
        //get the collection name at which to start the purge
        String purgeStartCollName = getCollNameOfPurgeId(id);
        appLog.info("Collection Name of Collection in Purge Tracker: " + purgeStartCollName + " For Purge Id: " + id);

        ListIterator<String> collNamesIterator = null;
        if ( purgeStartCollName != null ) {
            collNamesIterator = collNamesSortedList.listIterator(collNamesSortedList.indexOf(purgeStartCollName));
        } else {
            collNamesIterator = collNamesSortedList.listIterator();
        }

        //batch size tracker
        int batchTracker = 0;
        while ( (collNamesIterator.hasNext()) && (batchTracker < batchSize) ) {
            String collectionName = collNamesIterator.next();
            if (!isNotSystemCollection(collectionName)) {
                continue;
            }

            DBObject query = new BasicDBObject();
            query.put(RedactHelper.APP_ID_FIELD, appId);
            query.put(RedactHelper.ID_FIELD, new BasicDBObject("$in", toPurge));
            DBCursor cursor = db.getCollection(collectionName).find(query);

            while ( (cursor.hasNext()) && (batchTracker < batchSize) ) {
                DBObject dbObject = cursor.next();

                appLog.info("Purge candidate dbObject: {}", dbObject);

                long purgeId = (Long)dbObject.get(RedactHelper.ID_FIELD);
                Object composite = dbObject.get(RedactHelper.COMPOSITE_FIELD);

                if (composite != null && (Boolean)composite) {
                    appLog.info("Composite item cannot be purged: _id {} ", dbObject.get("_id"));
                    unpurged.add(purgeId);
                } else {
                    appLog.info("Purging item _id {} and Purge Id {}", dbObject.get("_id"), purgeId);
                    purged.add(purgeId);
                    db.getCollection(collectionName).remove(dbObject);
                }

                batchTracker++;
            }

            if ( cursor.hasNext() ) {
                result.setIsFinished(false);
            } else {
                result.setIsFinished(true);
            }
            //update purge tracker collection name with purge id and collection name where purge stopped
            updatePurgeTracker(id,collectionName,token);
        }

        result.setPurged(purged);
        result.setUnpurged(unpurged);

        appLog.info("Size of Purged Items {}", result.getPurged().size());
        return result;
    }
}
