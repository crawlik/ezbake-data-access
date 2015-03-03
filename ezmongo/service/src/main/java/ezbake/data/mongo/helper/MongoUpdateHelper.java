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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.TokenUtils;
import ezbake.data.mongo.EzMongoHandler;
import ezbake.data.mongo.redact.RedactHelper;
import ezbake.data.mongo.thrift.EzMongoBaseException;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.bson.types.ObjectId;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mchong on 8/26/14.
 */
public class MongoUpdateHelper {

    private final Logger appLog = LoggerFactory.getLogger(MongoUpdateHelper.class);

    private EzMongoHandler ezMongoHandler;

    public MongoUpdateHelper(EzMongoHandler handler) {
        this.ezMongoHandler = handler;
    }

    private boolean isUpdatingSecurityField(Object existingField, Object updaterField) {
        // We consider the user to be updating the security fields if:
        //   if existing doc doesn't have the field, and the new doc has the field, OR
        //   if both docs have the field, and the new field is different

        return (existingField == null && updaterField != null) ||
                ((existingField != null && updaterField != null) &&
                        !existingField.equals(updaterField));
    }

    public boolean isUpdatingSecurityFields(DBObject existingContent, DBObject updater) {

        // We need to compare all the security fields to see if any one of them are being updated.
        Object versionFieldObj = existingContent.get(RedactHelper.VERSION_FIELD);
        Object formalVisibilityFieldObj = existingContent.get(RedactHelper.FORMAL_VISIBILITY_FIELD);
        Object externalCommunityVisibilityFieldObj = existingContent.get(RedactHelper.EXTERNAL_COMMUNITY_VISIBILITY_FIELD);
        Object platformReadVisibilityFieldObj = existingContent.get(RedactHelper.PLATFORM_OBJECT_READ_VISIBILITY_FIELD);
        Object platformWriteVisibilityFieldObj = existingContent.get(RedactHelper.PLATFORM_OBJECT_WRITE_VISIBILITY_FIELD);
        Object platformManageVisibilityFieldObj = existingContent.get(RedactHelper.PLATFORM_OBJECT_MANAGE_VISIBILITY_FIELD);
        Object platformDiscoverVisibilityFieldObj = existingContent.get(RedactHelper.PLATFORM_OBJECT_DISCOVER_VISIBILITY_FIELD);
        Object idFieldObj = existingContent.get(RedactHelper.ID_FIELD);
        Object compositeFieldObj = existingContent.get(RedactHelper.COMPOSITE_FIELD);
        Object purgeIdsFieldObj = existingContent.get(RedactHelper.PURGE_IDS_FIELD);
        Object appIdFieldObj = existingContent.get(RedactHelper.APP_ID_FIELD);

        Object versionFieldUpdaterObj = updater.get(RedactHelper.VERSION_FIELD);
        Object formalVisibilityFieldUpdaterObj = updater.get(RedactHelper.FORMAL_VISIBILITY_FIELD);
        Object externalCommunityVisibilityFieldUpdaterObj = updater.get(RedactHelper.EXTERNAL_COMMUNITY_VISIBILITY_FIELD);
        Object platformReadVisibilityFieldUpdaterObj = updater.get(RedactHelper.PLATFORM_OBJECT_READ_VISIBILITY_FIELD);
        Object platformWriteVisibilityFieldUpdaterObj = updater.get(RedactHelper.PLATFORM_OBJECT_WRITE_VISIBILITY_FIELD);
        Object platformManageVisibilityFieldUpdaterObj = updater.get(RedactHelper.PLATFORM_OBJECT_MANAGE_VISIBILITY_FIELD);
        Object platformDiscoverVisibilityFieldUpdaterObj = updater.get(RedactHelper.PLATFORM_OBJECT_DISCOVER_VISIBILITY_FIELD);
        Object idFieldUpdaterObj = updater.get(RedactHelper.ID_FIELD);
        Object compositeFieldUpdaterObj = updater.get(RedactHelper.COMPOSITE_FIELD);
        Object purgeIdsFieldUpdaterObj = updater.get(RedactHelper.PURGE_IDS_FIELD);
        Object appIdFieldUpdaterObj = updater.get(RedactHelper.APP_ID_FIELD);

        return isUpdatingSecurityField(versionFieldObj, versionFieldUpdaterObj) ||
                isUpdatingSecurityField(formalVisibilityFieldObj, formalVisibilityFieldUpdaterObj) ||
                isUpdatingSecurityField(externalCommunityVisibilityFieldObj, externalCommunityVisibilityFieldUpdaterObj) ||
                isUpdatingSecurityField(platformReadVisibilityFieldObj, platformReadVisibilityFieldUpdaterObj) ||
                isUpdatingSecurityField(platformWriteVisibilityFieldObj, platformWriteVisibilityFieldUpdaterObj) ||
                isUpdatingSecurityField(platformManageVisibilityFieldObj, platformManageVisibilityFieldUpdaterObj) ||
                isUpdatingSecurityField(platformDiscoverVisibilityFieldObj, platformDiscoverVisibilityFieldUpdaterObj) ||
                isUpdatingSecurityField(idFieldObj, idFieldUpdaterObj) ||
                isUpdatingSecurityField(compositeFieldObj, compositeFieldUpdaterObj) ||
                isUpdatingSecurityField(purgeIdsFieldObj, purgeIdsFieldUpdaterObj) ||
                isUpdatingSecurityField(appIdFieldObj, appIdFieldUpdaterObj);
    }

    public boolean isUpdatingSecurityFields(DBObject content, Visibility vis) {

        return (vis != null &&
                (vis.isSetFormalVisibility() ||
                        vis.isSetAdvancedMarkings()));
    }

    public int updateDocument(String collectionName, String jsonQuery, String jsonDocument,
                               boolean useUpdateOperators, Visibility vis, EzSecurityToken security)
            throws TException, EzMongoBaseException, VisibilityParseException, JSONException {
        TokenUtils.validateSecurityToken(security, ezMongoHandler.getConfigurationProperties());

        if (StringUtils.isEmpty(collectionName)) {
            throw new EzMongoBaseException("collectionName is required.");
        }

        int updatedCount = 0;
        final String finalCollectionName = ezMongoHandler.getCollectionName(collectionName);
        DBObject content = (DBObject) JSON.parse(jsonDocument);

        // see if we are able to update the data in db with user's classification in the user token.
        // Also need to see if this is a WRITE or MANAGE operation - MANAGE is when users update the
        //   security fields in the Mongo document.
        String operation = EzMongoHandler.WRITE_OPERATION;
        if (isUpdatingSecurityFields(content, vis)) {
            operation = EzMongoHandler.MANAGE_OPERATION;
        }

        final List<DBObject> results =
                ezMongoHandler.getMongoFindHelper().findElements(collectionName, jsonQuery, "{ _id: 1}", null, 0, 0, false, security, false, operation);
        if (results.size() > 0) {

            // construct a list of ObjectIds to use as the filter
            final List<ObjectId> idList = new ArrayList<ObjectId>();
            for (final DBObject result : results) {
                appLog.info("can update DBObject (_id): {}", result);

                idList.add((ObjectId) result.get("_id"));
            }

            final DBObject inClause = new BasicDBObject("$in", idList);
            final DBObject query = new BasicDBObject("_id", inClause);

            if (!useUpdateOperators) {
                if (operation.equals(EzMongoHandler.MANAGE_OPERATION)) {
                    // we need to check if the user is allowed to update with the new Visibility
                    ezMongoHandler.getMongoInsertHelper().checkAbilityToInsert(security, vis, content, null, true, false);
                }
                // use $set if we are not using mongo update operators (such as $pull) in the jsonDocument
                content = new BasicDBObject("$set", content);
            } else {
                // when using an update operator such as $pull and if we are doing a MANAGE operation -
                //   we need to set the security fields in the object as well.
                if (operation.equals(EzMongoHandler.MANAGE_OPERATION)) {
                    DBObject visDBObject = new BasicDBObject();
                    // we need to check if the user is allowed to update with the new Visibility
                    ezMongoHandler.getMongoInsertHelper().checkAbilityToInsert(security, vis, visDBObject, null, true, false);
                    content.put("$set", visDBObject);
                }
            }

            final boolean upsert = false;
            final boolean multi = true;

            updatedCount = updateContent(finalCollectionName, query, content, upsert, multi);

        } else {
            appLog.info("Did not find any documents to update with the query {} and Visibility {}",
                    jsonQuery, vis);
        }

        return updatedCount;
    }

    public int updateContent(String finalCollectionName, DBObject query, DBObject content, boolean upsert,
                              boolean multi) {

        final WriteResult writeResult =
                ezMongoHandler.getDb().getCollection(finalCollectionName).update(query, content, upsert, multi);

        appLog.info("updated - write result: {}", writeResult.toString());

        return writeResult.getN();
    }

}
