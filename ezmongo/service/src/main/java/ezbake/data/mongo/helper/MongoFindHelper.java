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

package ezbake.data.mongo.helper;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.mongo.EzMongoHandler;
import ezbake.data.mongo.conversion.MongoConverter;
import ezbake.data.mongo.redact.RedactHelper;
import org.apache.commons.lang.StringUtils;
import org.bson.types.ObjectId;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mchong on 8/25/14.
 */
public class MongoFindHelper {

    private final Logger appLog = LoggerFactory.getLogger(MongoFindHelper.class);

    private EzMongoHandler ezMongoHandler;
    private String securityExpressionViz;
    private String securityExpressionOperation;

    public MongoFindHelper(EzMongoHandler handler) {
        this.ezMongoHandler = handler;

        // Load up the JSON from the resources.  Throw RuntimeExecptions as there's no reason it should barf in prod
        try {
            final URL sevURL = Thread.currentThread().getContextClassLoader().getResource("securityExpressionViz.json");
            if(sevURL == null){
                throw new RuntimeException("The securityExpressionVis was missing from the JAR");
            }
            securityExpressionViz = Resources.toString(sevURL, Charsets.UTF_8);

            final URL seoURL = Thread.currentThread().getContextClassLoader().getResource("securityExpressionOperation.json");
            if(seoURL == null){
                throw new RuntimeException("The securityExpressionVis was missing from the JAR");
            }
            securityExpressionOperation = Resources.toString(seoURL, Charsets.UTF_8);
        } catch (IOException e) {
            appLog.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public String getSecurityExpressionViz() {
        return securityExpressionViz;
    }

    public String getSecurityExpressionOperation() {
        return securityExpressionOperation;
    }

    /**
     * Retrieve only the collections visible to the user (based on the User auths in EzSecurityToken). Note, this is
     * useful in cases of reading, updating and deleting objects; since only the objects visible to the user will be
     * returned.
     *
     * @param collectionName A Mongo collection name
     * @param jsonQuery A query to search
     * @param jsonProjection A query projection
     * @param jsonSort Fields to sort
     * @param skip Number of objects to skip
     * @param limit Max number of objects to return
     * @param returnPlainObjectIdString Whether to return the DBObject's _id in plain string or $oid format.
     * @param security The user's EzSecurityToken
     * @param useStrings Whether to return a list of JSON serialized objects as Strings or DBObjects.
     * @param operationType Read/Write/Discover/Manage operation types
     * @return The query result
     */
    public List findElements(String collectionName, String jsonQuery, String jsonProjection, String jsonSort,
                              int skip, int limit, boolean returnPlainObjectIdString, EzSecurityToken security,
                              boolean useStrings, String operationType) throws JSONException {
        List results = new ArrayList();

        final DBObject queryCommand = MongoConverter.toDBObject(jsonQuery);
        final DBObject query = new BasicDBObject("$match", queryCommand);

        final String finalCollectionName = ezMongoHandler.getCollectionName(collectionName);

        appLog.info("findElements, finalCollectionName: {}, jsonQuery: {}", finalCollectionName, jsonQuery);

        final DBObject[] aggregationCommandsArray =
                getFindAggregationCommandsArray(skip, limit, jsonProjection, jsonSort, security, operationType);
        final AggregationOutput aggregationOutput =
                ezMongoHandler.getDb().getCollection(finalCollectionName).aggregate(query, aggregationCommandsArray);

        final BasicDBList resultsList = (BasicDBList) aggregationOutput.getCommandResult().get("result");
        if (resultsList != null && resultsList.size() > 0) {
            if (returnPlainObjectIdString || useStrings) {
                results = addMongoResultsToList(resultsList, returnPlainObjectIdString, useStrings);
            } else {
                results = resultsList;
            }
        }

        return results;
    }

    /**
     *
     * @param skip
     * @param limit
     * @param jsonProjection
     * @param jsonSort
     * @param security
     * @param operationType
     * @return
     */
    public DBObject[] getFindAggregationCommandsArray(int skip, int limit, String jsonProjection, String jsonSort,
                                                      EzSecurityToken security, String operationType) {
        return getFindAggregationCommandsArray_withcounter(skip, limit, jsonProjection, jsonSort, security, false, operationType);
    }

    /**
     *
     * @param skip
     * @param limit
     * @param jsonProjection
     * @param jsonSort
     * @param security
     * @param count
     * @param operationType
     * @return
     */
    public DBObject[] getFindAggregationCommandsArray_withcounter(int skip, int limit, String jsonProjection,
                                                                   String jsonSort, EzSecurityToken security,
                                                                   boolean count, String operationType) {
        final List<DBObject> aggregationCommandsList = new ArrayList<>();

        // add the redact operator for the _ezFV field.
        DBObject redactCommand = RedactHelper.createRedactCommandViz(securityExpressionViz, RedactHelper.FORMAL_VISIBILITY_FIELD, security);
        DBObject redact = new BasicDBObject("$redact", redactCommand);
        aggregationCommandsList.add(redact);

        // add the redact operator for the _ezExtV field.
        redactCommand = RedactHelper.createRedactCommandViz(securityExpressionViz, RedactHelper.EXTERNAL_COMMUNITY_VISIBILITY_FIELD, security);
        redact = new BasicDBObject("$redact", redactCommand);
        aggregationCommandsList.add(redact);

        // add the redact operator for the Read/Write/Discover/Manage fields
        String objAuths = "[ ]";
        if (security.authorizations != null &&
                security.authorizations.platformObjectAuthorizations != null) {
            objAuths = Arrays.toString(security.authorizations.platformObjectAuthorizations.toArray());
        }

        // determine the operation field - this is for Platform Visibilities
        final String field;
        switch (operationType) {
            case EzMongoHandler.WRITE_OPERATION:
                field = RedactHelper.PLATFORM_OBJECT_WRITE_VISIBILITY_FIELD;
                break;
            case EzMongoHandler.DISCOVER_OPERATION:
                field = RedactHelper.PLATFORM_OBJECT_DISCOVER_VISIBILITY_FIELD;
                break;
            case EzMongoHandler.MANAGE_OPERATION:
                field = RedactHelper.PLATFORM_OBJECT_MANAGE_VISIBILITY_FIELD;
                break;
            default:
                field = RedactHelper.PLATFORM_OBJECT_READ_VISIBILITY_FIELD;
                break;
        }
        redactCommand = RedactHelper.createRedactCommand(securityExpressionOperation, field, objAuths, RedactHelper.REDACT_TYPE_OPERATION);
        redact = new BasicDBObject("$redact", redactCommand);
        aggregationCommandsList.add(redact);

        // add the rest of the pipeline operators
        if (skip > 0) {
            final DBObject skipCommand = new BasicDBObject("$skip", skip);
            aggregationCommandsList.add(skipCommand);
        }

        if (limit > 0) {
            final DBObject limitCommand = new BasicDBObject("$limit", limit);
            aggregationCommandsList.add(limitCommand);
        }

        if (!StringUtils.isEmpty(jsonProjection)) {
            final DBObject projectionCommand = (DBObject) JSON.parse(jsonProjection);
            final DBObject projection = new BasicDBObject("$project", projectionCommand);
            aggregationCommandsList.add(projection);
        }

        if (!StringUtils.isEmpty(jsonSort)) {
            final DBObject sortCommand = (DBObject) JSON.parse(jsonSort);
            final DBObject sort = new BasicDBObject("$sort", sortCommand);
            aggregationCommandsList.add(sort);
        }

        // counter should look like==> "{$group: {_id: null, count: {$sum: 1}}}"
        if (count) {
            final DBObject groupFields = new BasicDBObject("_id", "null");
            groupFields.put("count", new BasicDBObject("$sum", 1));
            final DBObject counter = new BasicDBObject("$group", groupFields);
            aggregationCommandsList.add(counter);
        }

        final DBObject[] aggregationCommandsArray =
                aggregationCommandsList.toArray(new DBObject[aggregationCommandsList.size()]);

        appLog.info("in getFindAggregationCommandsArray, aggregationCommandsArray: {}", aggregationCommandsArray);

        return aggregationCommandsArray;
    }

    /**
     * Adds results from MongoDB to a new List; if returnPlainObjectIdString is true, it converts the id from { _id: {
     * $oid: "..." } } to { _id: "..." }
     *
     * @param resultsList
     * @param returnPlainObjectIdString
     * @param useStrings Whether to return a list of Strings rather than list of DBObjects
     *
     * @return List of Strings or DBObjects
     */
    public List addMongoResultsToList(List resultsList, boolean returnPlainObjectIdString, boolean useStrings) {
        final List results = new ArrayList();

        for (final Object res : resultsList) {
            if (res instanceof DBObject) {
                final DBObject result = (DBObject) res;
                if (returnPlainObjectIdString) {
                    convertIdToPlainString(result);
                }
                if (useStrings) {
                    final String json = JSON.serialize(result);
                    results.add(json);
                } else {
                    results.add(result);
                }
            } else {
                results.add(res);
            }
        }

        return results;
    }

    /**
     * Converts the _id from { _id: { $oid: "..." } } to { _id: "..." }
     *
     * @param dbObject The object to convert
     */
    public void convertIdToPlainString(DBObject dbObject) {
        final ObjectId _id = (ObjectId) dbObject.get("_id");

        dbObject.put("_id", _id.toString());
    }
}
