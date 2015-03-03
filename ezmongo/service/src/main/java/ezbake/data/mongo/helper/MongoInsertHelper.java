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

import com.mongodb.DBObject;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.classification.ClassificationUtils;
import ezbake.data.mongo.redact.RedactHelper;
import ezbake.data.mongo.thrift.EzMongoBaseException;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by mchong on 8/26/14.
 */
public class MongoInsertHelper {

    private final Logger appLog = LoggerFactory.getLogger(MongoInsertHelper.class);

    private String appId;

    public MongoInsertHelper(String appId) {
        this.appId = appId;
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
}
