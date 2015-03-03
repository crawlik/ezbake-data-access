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

package ezbake.data.mongo.redact;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.PlatformObjectVisibilities;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.classification.VisibilityUtils;
import ezbake.data.mongo.EzMongoBasePojo;
import ezbake.data.mongo.thrift.EzMongoBaseException;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by mchong on 7/31/14.
 */
public class RedactHelper {

    public static final String REDACT_TYPE_VIZ = "REDACT_TYPE_VIZ";
    public static final String REDACT_TYPE_OPERATION = "REDACT_TYPE_OPERATION";

    public static final String VERSION_FIELD = "_ezV";

    // Formal visibility == Accumulo-style boolean expression in Visibility object
    public static final String FORMAL_VISIBILITY_FIELD = "_ezFV";
    // External Community visibility == Accumulo-style boolean expression in Visibility object
    public static final String EXTERNAL_COMMUNITY_VISIBILITY_FIELD = "_ezExtV";
    // Read / Discover / Write / Manage fields
    public static final String PLATFORM_OBJECT_READ_VISIBILITY_FIELD = "_ezObjRV";
    public static final String PLATFORM_OBJECT_DISCOVER_VISIBILITY_FIELD = "_ezObjDV";
    public static final String PLATFORM_OBJECT_WRITE_VISIBILITY_FIELD = "_ezObjWV";
    public static final String PLATFORM_OBJECT_MANAGE_VISIBILITY_FIELD = "_ezObjMV";
    // Purge-related fields
    public static final String ID_FIELD = "_ezId";
    public static final String COMPOSITE_FIELD = "_ezComposite";
    public static final String PURGE_IDS_FIELD = "_ezPurgeIds";
    public static final String APP_ID_FIELD = "_ezAppId";
    public static final String PURGE_ID = "_ezPurgeId";
    public static final String PURGE_TRACKING_COLL_FIELD = "_ezPurgeTrackCollName";

    private final static Logger appLog = LoggerFactory.getLogger(RedactHelper.class);

    /**
     * For the DBObject or EzMongoBasePojo passed in,
     * 1. Uses the boolean expression (from Visibility's formalVisibility)
     *    to generate the double array security tagging field format for Mongo's $redact operator.
     * 2. Also does the same for the External Community visibility field.
     * 3. Sets the read/discover/write/manage security fields from the Visibility object's PlatformObjectVisibilities.
     * 4. Sets Purge-related fields.
     *
     * The security tagging fields are inserted at the root document level.
     *
     * @param object DBObject or EzMongoBasePojo
     * @param vis The Visibility
     * @param appId
     * @throws VisibilityParseException, EzMongoBaseException
     */
    public static void setSecurityFieldsInDBObject(Object object, Visibility vis, String appId)
            throws VisibilityParseException, EzMongoBaseException {

        if (vis == null) {
            throw new EzMongoBaseException("Attempted to set security fields in Mongo Object - Visibility is null!");
        }

        DBObject dbObject = null;
        EzMongoBasePojo ezMongoBasePojo = null;

        if (object instanceof DBObject) {
            dbObject = (DBObject)object;
        } else if (object instanceof EzMongoBasePojo) {
            ezMongoBasePojo = (EzMongoBasePojo)object;
        }

        // put version as 1
        if (dbObject != null) {
            dbObject.put(VERSION_FIELD, 1);
        } else if (ezMongoBasePojo != null) {
            ezMongoBasePojo.set_ezV("1");
        }

        // check formal visibility
        String booleanExpression = vis.getFormalVisibility();
        if (!StringUtils.isEmpty(booleanExpression)) {
            setBooleanExpressionVisibilityField(dbObject, ezMongoBasePojo, booleanExpression, FORMAL_VISIBILITY_FIELD, true);
        }

        if (vis.isSetAdvancedMarkings()) {
            AdvancedMarkings advancedMarkings = vis.getAdvancedMarkings();

            // check for External Community visibilities
            if (advancedMarkings.isSetExternalCommunityVisibility()) {
                booleanExpression = advancedMarkings.getExternalCommunityVisibility();
                setBooleanExpressionVisibilityField(dbObject, ezMongoBasePojo, booleanExpression, EXTERNAL_COMMUNITY_VISIBILITY_FIELD, false);
            }

            // check for Platform Object visibilities
            if (advancedMarkings.isSetPlatformObjectVisibility()) {
                PlatformObjectVisibilities platformObjectVisibilities = advancedMarkings.getPlatformObjectVisibility();

                if (platformObjectVisibilities.isSetPlatformObjectReadVisibility()) {
                    Set<Long> values = platformObjectVisibilities.getPlatformObjectReadVisibility();
                    if (dbObject != null) {
                        dbObject.put(PLATFORM_OBJECT_READ_VISIBILITY_FIELD, values);
                    } else if (ezMongoBasePojo != null) {
                        ezMongoBasePojo.set_ezObjRV(values);
                    }
                }

                if (platformObjectVisibilities.isSetPlatformObjectDiscoverVisibility()) {
                    Set<Long> values = platformObjectVisibilities.getPlatformObjectDiscoverVisibility();
                    if (dbObject != null) {
                        dbObject.put(PLATFORM_OBJECT_DISCOVER_VISIBILITY_FIELD, values);
                    } else if (ezMongoBasePojo != null) {
                        ezMongoBasePojo.set_ezObjDV(values);
                    }
                }

                if (platformObjectVisibilities.isSetPlatformObjectWriteVisibility()) {
                    Set<Long> values = platformObjectVisibilities.getPlatformObjectWriteVisibility();
                    if (dbObject != null) {
                        dbObject.put(PLATFORM_OBJECT_WRITE_VISIBILITY_FIELD, values);
                    } else if (ezMongoBasePojo != null) {
                        ezMongoBasePojo.set_ezObjWV(values);
                    }
                }

                if (platformObjectVisibilities.isSetPlatformObjectManageVisibility()) {
                    Set<Long> values = platformObjectVisibilities.getPlatformObjectManageVisibility();
                    if (dbObject != null) {
                        dbObject.put(PLATFORM_OBJECT_MANAGE_VISIBILITY_FIELD, values);
                    } else if (ezMongoBasePojo != null) {
                        ezMongoBasePojo.set_ezObjMV(values);
                    }
                }
            }

            // check for Purge-related fields
            if (advancedMarkings.isSetId()) {
                if (dbObject != null) {
                    dbObject.put(ID_FIELD, advancedMarkings.getId());
                } else if (ezMongoBasePojo != null) {
                    ezMongoBasePojo.set_ezId(advancedMarkings.getId());
                }
            }
            if (advancedMarkings.isSetComposite()) {
                if (dbObject != null) {
                    dbObject.put(COMPOSITE_FIELD, advancedMarkings.isComposite());
                } else if (ezMongoBasePojo != null) {
                    ezMongoBasePojo.set_ezComposite(advancedMarkings.isComposite());
                }
            }
            if (advancedMarkings.isSetPurgeIds()) {
                if (dbObject != null) {
                    dbObject.put(PURGE_IDS_FIELD, advancedMarkings.getPurgeIds());
                } else if (ezMongoBasePojo != null) {
                    ezMongoBasePojo.set_ezPurgeIds(advancedMarkings.getPurgeIds());
                }
            }
        }

        if (dbObject != null) {
            dbObject.put(APP_ID_FIELD, appId);
        } else if (ezMongoBasePojo != null) {
            ezMongoBasePojo.set_ezAppId(appId);
        }

        appLog.info("after setSecurityFieldsInDBObject - DBObject: {} or EzMongoBasePojo: {}", dbObject, ezMongoBasePojo);
    }

    private static void setBooleanExpressionVisibilityField(DBObject dbObject,
                                                            EzMongoBasePojo ezMongoBasePojo, String classification, String field, boolean isFormalVis)
                                throws VisibilityParseException {
        appLog.info("setBooleanExpressionVisibilityField: classification: *{}*", classification);

        List classificationList = VisibilityUtils.generateVisibilityList(classification);
        appLog.info("classificationList: {}", classificationList);

        if (dbObject != null) {
            dbObject.put(field, classificationList);
        } else if (ezMongoBasePojo != null) {
            if (isFormalVis) {
                ezMongoBasePojo.set_ezFV(classificationList);
            } else {
                ezMongoBasePojo.set_ezExtV(classificationList);
            }
        }

        appLog.info("set dbObject {} or ezMongoBasePojo {} : field: {} to: {}",
                dbObject, ezMongoBasePojo, field, classificationList);
    }

    public static DBObject createRedactOperator(String securityExpression, EzSecurityToken security) {

        DBObject redactCommand = createRedactCommandViz(securityExpression, FORMAL_VISIBILITY_FIELD, security);
        DBObject redact = new BasicDBObject("$redact", redactCommand);

        return redact;
    }

    public static DBObject createRedactCommandViz(String expression, String field, EzSecurityToken ezSecurityToken) {
        String visibility = null;
        if (field.equals(RedactHelper.FORMAL_VISIBILITY_FIELD)) {
            visibility = createStringArray(ezSecurityToken.getAuthorizations().getFormalAuthorizations());
            appLog.info("createRedactCommandViz: using FV: {}", visibility);
        } else if (field.equals(RedactHelper.EXTERNAL_COMMUNITY_VISIBILITY_FIELD)) {
            visibility = createStringArray(ezSecurityToken.getAuthorizations().getExternalCommunityAuthorizations());
            appLog.info("createRedactCommandViz: using ExtV: {}", visibility);
        }
        return createRedactCommand(expression, field, visibility, REDACT_TYPE_VIZ);
    }

    public static DBObject createRedactCommand(String expression, String securityField, String auths, String redactType) {
        securityField = "$" + securityField;
        appLog.info("createRedactCommand, securityField: {}", securityField);
        appLog.info("createRedactCommand, auths: {}", auths);
        String userSecurityExpression = null;

        if (redactType.equals(REDACT_TYPE_VIZ)) {
            userSecurityExpression = String.format(expression, securityField, auths);
        } else if (redactType.equals(REDACT_TYPE_OPERATION)){
            userSecurityExpression = String.format(expression, securityField, securityField, securityField, auths);
        }

        appLog.info("createRedactCommand, userSecurityExpression: {}", userSecurityExpression);
        return (DBObject) JSON.parse(userSecurityExpression);
    }

    public static String createStringArray(Set<String> auths) {

        if (auths == null) {
            return "[ ]";
        }

        Set<String> newAuths = new HashSet<>();
        for (String auth : auths) {
            String newAuth = "'" + auth + "'";
            newAuths.add(newAuth);
        }
        return Arrays.toString(newAuths.toArray());
    }
}
