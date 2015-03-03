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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import ezbake.base.thrift.*;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.base.thrift.PurgeItems;
import ezbake.data.base.thrift.PurgeOptions;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.data.mongo.conversion.MongoConverter;
import ezbake.data.mongo.thrift.*;
import ezbake.data.test.TestUtils;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.thrift.TException;
import org.bson.types.ObjectId;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Currently these tests only cover the old 1.3.x Mongo Dataset API methods;
 * Not the ezmongo-java-driver API methods.  Those driver tests should be run within ezmongo-java-driver.
 */
public class EzMongoHandlerTest {

    private static final Logger logger = LoggerFactory.getLogger(EzMongoHandlerTest.class);

    private EzMongoHandler handler;

    private final String collectionName = getClass().getSimpleName();

    private EzSecurityToken securityToken;

    // in-memory mongodb
    private static MongodExecutable mongodExecutable = null;
    private static MongodProcess mongod = null;
    private static final int DEFAULT_MONGODB_PORT = 27017;

    @BeforeClass
    public static void embedmongo_setUp() throws Exception {

        // fire up the in-memory mongodb
        MongodStarter starter = MongodStarter.getDefaultInstance();

        try {
            IMongodConfig mongodConfig = new MongodConfigBuilder()
                    .version(Version.Main.PRODUCTION)
                    .net(new Net(DEFAULT_MONGODB_PORT, Network.localhostIsIPv6()))
                    .build();

            mongodExecutable = starter.prepare(mongodConfig);
            mongod = mongodExecutable.start();
        } catch (Exception e) {
            logger.error(">>>embedmongo_setUp.mongodExe..e.getMessage = {}<<<", e.getMessage());
        }
    }

    @Before
    public void setUp() throws Exception {
        logger.debug("in setUp()");

        securityToken = TestUtils.createTS_S_B_User();

        final Properties properties = new Properties();
        final String host = "localhost";
        final String db = "datasetdb";

        properties.setProperty(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME, "testapp");
        properties.setProperty(EzBakePropertyConstants.MONGODB_HOST_NAME, host);
        properties.setProperty(EzBakePropertyConstants.MONGODB_DB_NAME, db);
        properties.setProperty(EzBakePropertyConstants.MONGODB_USE_SSL, "false");

        TestUtils.addSettingsForMock(properties);

        handler = new EzMongoHandler();
        handler.setConfigurationProperties(properties);
        handler.getThriftProcessor(); // calls init();
    }

    @After
    public void cleanup() throws TException {
        handler.dropCollection(collectionName, securityToken);
        handler = null;
    }

    @AfterClass
    public static void embedmongo_cleanup() throws TException, InterruptedException {

        //shutdown the in-memory mongodb
        if (mongod != null)
            mongod.stop();
        if (mongodExecutable != null)
            mongodExecutable.stop();
    }

    @Test
    public void createAndDropCollectionTest() throws TException {
        final String collection = "newCollection";

        handler.createCollection(collection, securityToken);
        assertTrue(handler.collectionExists(collection, securityToken));

        handler.dropCollection(collection, securityToken);
        assertTrue(!handler.collectionExists(collection, securityToken));
    }

    @Test
    public void purgeCollectionExistsTest() throws TException {
        final String purgecollection = "purgetracker";

        assertTrue(handler.collectionExists(purgecollection, securityToken));
    }

    @Test
    public void ensureIndexTest() throws TException {
        final String indexKeys = "{ testKey: 1 }";
        final String indexOptions = " { unique: true }";

        handler.createCollection(collectionName, securityToken);
        assertTrue(handler.collectionExists(collectionName, securityToken));

        handler.ensureIndex(collectionName, indexKeys, indexOptions, securityToken);

        final List<String> indexList = handler.getIndexInfo(collectionName, securityToken);
        assertEquals(indexList.size(), 2); // expecting 2 here since there is an index on "_id" by default

        final List<DBObject> indexObjList = new ArrayList<>();
        for (final String indexString : indexList) {
            indexObjList.add((DBObject) JSON.parse(indexString));
        }

        DBObject index = indexObjList.get(0);
        DBObject indexKey = (DBObject) index.get("key");
        assertEquals(indexKey.get("_id"), 1);

        index = indexObjList.get(1);
        indexKey = (DBObject) index.get("key");
        assertEquals(indexKey.get("testKey"), 1);
        assertTrue((Boolean) index.get("unique"));
    }

    @Test
    public void createIndexTest() throws TException {
        final String indexKeys = "{ testKey: 1 }";
        final String indexOptions = " { unique: true }";

        handler.createCollection(collectionName, securityToken);
        assertTrue(handler.collectionExists(collectionName, securityToken));

        handler.createIndex(collectionName, indexKeys, indexOptions, securityToken);

        final List<String> indexList = handler.getIndexInfo(collectionName, securityToken);
        assertEquals(indexList.size(), 2); // expecting 2 here since there is an index on "_id" by default

        final List<DBObject> indexObjList = new ArrayList<>();
        for (final String indexString : indexList) {
            indexObjList.add((DBObject) JSON.parse(indexString));
        }

        DBObject index = indexObjList.get(0);
        DBObject indexKey = (DBObject) index.get("key");
        assertEquals(indexKey.get("_id"), 1);

        index = indexObjList.get(1);
        indexKey = (DBObject) index.get("key");
        assertEquals(indexKey.get("testKey"), 1);
        assertTrue((Boolean) index.get("unique"));
    }

    private void insertTestPeopleDocs() throws TException {
        final Visibility vis = new Visibility().setFormalVisibility("S");
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'blue', hair:'black', foo:true}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'blue', hair:'blonde', foo:true}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'blue', hair:'brown', foo:true}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'brown', hair:'black', foo:true}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'brown', hair:'blonde', foo:true}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'brown', hair:'brown', foo:true}",
                vis), securityToken);
    }

    private void insertTestAlphabetDocs() throws TException {
        final Visibility vis = new Visibility().setFormalVisibility("S");
        handler.insert(collectionName, new MongoEzbakeDocument("{letter:'A', display: 6}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{letter:'B', display: 5}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{letter:'C', display: 4}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{letter:'D', display: 3}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{letter:'E', display: 2}",
                vis), securityToken);
        handler.insert(collectionName, new MongoEzbakeDocument("{letter:'F', display: 1}",
                vis), securityToken);
    }

    @Test
    public void countTest() throws TException {
        insertTestPeopleDocs();

        Long count = handler.getCount("nonExistingCollection", securityToken);
        assertEquals(count, new Long(0));

        count = handler.getCount(collectionName, securityToken);
        assertEquals(count, new Long(6));

        String query = "{eyes:'blue'}";
        count = handler.getCountFromQuery(collectionName, query, securityToken);
        assertEquals(count, new Long(3));

        query = "{eyes:'blue', hair:'black'}";
        count = handler.getCountFromQuery(collectionName, query, securityToken);
        assertEquals(count, new Long(1));
    }

    @Test
    public void insertWithCAPCOClassificationANDTest() throws TException,
            UnsupportedEncodingException, VisibilityParseException {
        final String classification = "EF&AB&CD&USA";
        final Visibility vis = new Visibility().setFormalVisibility(classification);

        final EzSecurityToken token = TestUtils.createTestToken("AB", "CD", "EF", "USA");
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);

        // query to find the sl tag with value [ [ 'AB', 'CD', 'EF', 'USA' ] ]
        final String query = "{_ezFV:{$elemMatch:{$in:[ [ 'EF', 'AB', 'CD', 'USA' ] ]}}}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }

    @Test
    public void insertWithCAPCOClassificationORTest() throws TException,
            UnsupportedEncodingException, VisibilityParseException {
        final String classification = "S&(USA|AUS)";

        final Visibility vis = new Visibility().setFormalVisibility(classification);
        final EzSecurityToken token = TestUtils.createTestToken("S", "USA");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);


        // query to find the sl tag with value [ [ 'S', 'USA' ], [ 'S', 'AUS' ] ]
        final String query = "{_ezFV:{$elemMatch:{$in:[ [ 'S', 'USA' ], [ 'S', 'AUS' ] ]}}}";
        final boolean returnPlainObjectIdString = false;
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        mongoFindParams.returnPlainObjectIdString = returnPlainObjectIdString;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }

    @Test
    public void insertWithCAPCOClassificationLongerORTest() throws TException,
            UnsupportedEncodingException, VisibilityParseException {
        final String classification = "S&(USA|AUS|CAN|GBR|NZL|FVEY|ACGU)";

        final Visibility vis = new Visibility().setFormalVisibility(classification);
        final EzSecurityToken token = TestUtils.createTestToken("S", "USA");
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);


        // query to find the sl tag with value [ [ 'S', 'USA' ], [ 'S', 'AUS' ] ]
        final String query =
                "{_ezFV:{$elemMatch:{$in:[ [ 'S', 'USA' ], [ 'S', 'AUS' ], [ 'S', 'CAN' ], [ 'S', 'GBR' ],"
                        + "[ 'S', 'NZL' ], [ 'S', 'FVEY' ], [ 'S', 'ACGU' ] ]}}}";
        final boolean returnPlainObjectIdString = false;
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        mongoFindParams.returnPlainObjectIdString = returnPlainObjectIdString;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }

    @Test(expected = EzMongoBaseException.class)
    public void insertWithNoVisibilityTest() throws TException {
        final EzSecurityToken token = TestUtils.createTestToken("AB", "USA");

        // this will throw exception
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                null), token);
    }

    @Test(expected = EzMongoBaseException.class)
    public void insertWithNoVisibilityFieldsTest() throws TException {
        final EzSecurityToken token = TestUtils.createTestToken("AB", "USA");
        Visibility vis = new Visibility();

        // this will throw exception
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);
    }


    @Test
    public void insertWithCAPCOClassification_AND_OR_test() throws TException ,
            UnsupportedEncodingException, VisibilityParseException {
        final String classification = "EF&AB&CD&(USA|AUS|GBR)";
        final Visibility vis = new Visibility().setFormalVisibility(classification);

        final EzSecurityToken token = TestUtils.createTestToken("AB", "CD", "EF", "USA");
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);


        // query to find the sl tag with value [ [AB, CD, EF, USA], [AB, CD, EF, AUS], [AB, CD, EF, GBR] ]
        final String query =
                "{_ezFV:{$elemMatch:{$in: [ ['EF', 'AB', 'CD', 'USA'], ['EF', 'AB', 'CD', 'AUS'], ['EF', 'AB', 'CD', 'GBR'] ] }}}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }


    @Test
    public void insertWithExternalCommunityVisibilityTest() throws TException {
        final String externalCommunity = "TEST";
        final AdvancedMarkings advancedMarkings = new AdvancedMarkings().setExternalCommunityVisibility(externalCommunity);
        final Visibility vis = new Visibility().setAdvancedMarkings(advancedMarkings);
        final EzSecurityToken token = TestUtils.createTestToken("AB", "USA");

        // set external community in token
        Set<String> externalCommunityAuths = new HashSet<>();
        externalCommunityAuths.add(externalCommunity);
        token.getAuthorizations().setExternalCommunityAuthorizations(externalCommunityAuths);

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'testBlack', foo:true}",
                vis), token);

        // query to find the _ezExtV tag with value [ [ 'TEST' ] ]
        final String query = "{_ezExtV:{$elemMatch:{$in:[ [ 'TEST' ] ]}}}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }

    @Test
    public void insertWithClassificationTest() throws TException {
        final String classification = "AB&USA";
        final Visibility vis = new Visibility().setFormalVisibility(classification);
        final EzSecurityToken token = TestUtils.createTestToken("AB", "USA");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);

        // query to find the sl tag with value [ [ 'AB' ] ]
        final String query = "{_ezFV:{$elemMatch:{$in:[ [ 'AB', 'USA' ] ]}}}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }

    @Test(expected = EzMongoBaseException.class)
    public void insertWithInvalidCAPCOTest() throws TException {
        final String classification = "ABC";
        final Visibility vis = new Visibility().setFormalVisibility(classification);

        // this will throw exception
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), securityToken);
    }

    @Test(expected = EzMongoBaseException.class)
    public void insertWithClassificationDeniedTest() throws TException {
        final String classification = "AB";
        final Visibility vis = new Visibility().setFormalVisibility(classification);
        final EzSecurityToken token = TestUtils.createTestToken("S", "USA");

        // this will throw exception
        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);
    }

    @Test
    public void updateWithClassificationDeniedTest() throws TException {
        final String classification = "S";
        final Visibility vis = new Visibility().setFormalVisibility(classification);

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), securityToken);

        final String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 1);

        final String updateDocument = "{eyes:'unclassified'}";
        final EzSecurityToken tempToken = TestUtils.createTestToken("FAKE");

        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = query;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, vis);

        // This will not update the record, since the tempToken doesn't have the required auths "S", "USA".
        final int updatedCount = handler.update(collectionName, mongoUpdateParams, tempToken);
        assertEquals(updatedCount, 0);
    }

    @Test
    public void updateWithClassificationDenied2Test() throws TException {
        final String classification = "S";
        final Visibility vis = new Visibility().setFormalVisibility(classification);

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), securityToken);

        final String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 1);

        final String updateDocument = "{eyes:'unclassified'}";
        final EzSecurityToken tempToken = TestUtils.createTestToken("S", "USA");

        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = query;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, new Visibility().setFormalVisibility("AB"));

        // This will throw an exception, since the tempToken doesn't have the required auths "AB", "USA".
        try {
            handler.update(collectionName, mongoUpdateParams, tempToken);
            fail("should have thrown an exception!");
        }
        catch(EzMongoBaseException e) {
            // expected to get here
        }
    }

    @Test
    public void updateTest() throws TException {
        final Visibility vis = new Visibility().setFormalVisibility("S");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), securityToken);

        String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 1);

        final String updateDocument = "{ newField: 'true' }";

        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = query;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, null);
        final int updatedCount = handler.update(collectionName, mongoUpdateParams, securityToken);
        assertEquals(updatedCount, 1);

        mongoFindParams.jsonQuery = updateDocument;
        results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 1);
    }

    @Test
    public void updateWithOperatorsTest() throws TException {
        final Visibility vis = new Visibility().setFormalVisibility("S");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), securityToken);

        String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 1);

        final String updateDocument = "{ $set: {eyes:'unclassified'} }";

        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = query;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, null);
        mongoUpdateParams.updateWithOperators = true;
        final int updatedCount = handler.update(collectionName, mongoUpdateParams, securityToken);
        assertEquals(updatedCount, 1);

        mongoFindParams.jsonQuery = "{eyes:'unclassified'}";
        results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 1);
    }

    @Test
    public void updateWithOperatorsPullTest() throws TException {
        final Visibility vis = new Visibility().setFormalVisibility("S");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', list:['one', 'two', 'three']}",
                vis), securityToken);

        String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 1);

        final String updateDocument = "{ $pull: {list:'one'} }";

        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = query;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, null);
        mongoUpdateParams.updateWithOperators = true;
        final int updatedCount = handler.update(collectionName, mongoUpdateParams, securityToken);
        assertEquals(updatedCount, 1);

        mongoFindParams.jsonQuery = "{ list:{$all: ['one', 'two', 'three']} }";
        results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(results.size(), 0);
    }

    @Test
    public void updateWithOperatorsPullAndClassificationTest() throws TException {
        EzSecurityToken token = TestUtils.createTestToken("U");

        String classification = "U";
        Visibility vis = new Visibility().setFormalVisibility(classification);

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', list:['one', 'two', 'three']}",
                vis), token);

        String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);

        final String updateDocument = "{ $pull: {list:'one'} }";

        // now updating to AB
        classification = "AB&USA";
        vis = new Visibility().setFormalVisibility(classification);
        token = TestUtils.createTestToken("AB", "U",  "USA");

        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = query;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, vis);
        mongoUpdateParams.updateWithOperators = true;
        final int updatedCount = handler.update(collectionName, mongoUpdateParams, token);
        assertEquals(updatedCount, 1);

        mongoFindParams.jsonQuery = "{ list:{$all: ['one', 'two', 'three']} }";
        results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 0);

        mongoFindParams.jsonQuery = "{ _ezFV:{$all: [ ['AB','USA'] ]} }";
        results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }


    @Test
    public void updateWithClassificationTest() throws TException {
        final Visibility vis = new Visibility().setFormalVisibility("AB");
        final EzSecurityToken token = TestUtils.createTestToken("AB", "S", "USA");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);

        String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);

        final String updateDocument = "{eyes:'unclassified'}";
        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = query;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, new Visibility().setFormalVisibility("S&USA"));
        final int updatedCount = handler.update(collectionName, mongoUpdateParams, token);
        assertEquals(updatedCount, 1);

        mongoFindParams.jsonQuery = updateDocument;
        results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);

        mongoFindParams.jsonQuery = "{ _ezFV:{$all: [ ['S','USA'] ]} }";
        results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);
    }

    @Test
    public void updateNotFoundTest() throws TException {
        final String classification = "AB";
        final Visibility vis = new Visibility().setFormalVisibility(classification);
        final EzSecurityToken token = TestUtils.createTestToken("AB", "USA");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);

        final String correctQuery = "{hair:'black'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = correctQuery;
        List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);

        String incorrectQuery = "{hair:'brown'}";
        final String updateDocument = "{hair:'red'}";
        MongoUpdateParams mongoUpdateParams = new MongoUpdateParams();
        mongoUpdateParams.jsonQuery = incorrectQuery;
        mongoUpdateParams.mongoDocument = new MongoEzbakeDocument(updateDocument, null);
        final int updatedCount = handler.update(collectionName, mongoUpdateParams, token);
        assertEquals(updatedCount, 0);

        mongoFindParams.jsonQuery = updateDocument;
        results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 0);
    }

    @Test
    public void removeWithClassificationDeniedTest() throws TException {
        final String classification = "S";
        final Visibility vis = new Visibility().setFormalVisibility(classification);

        final EzSecurityToken token = TestUtils.createTestToken("S", "USA");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);

        final String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);

        final EzSecurityToken nonTSToken = TestUtils.createTestToken("B");
        final int removedCount = handler.remove(collectionName, query, nonTSToken);

        // Since the non-TS user won't be able to find
        // the document, we expect the removedCount to be zero
        assertEquals(0, removedCount);
    }

    @Test
    public void removeWithClassificationTest() throws TException {
        final String classification = "AB";
        final Visibility vis = new Visibility().setFormalVisibility(classification);
        final EzSecurityToken token = TestUtils.createTestToken("AB", "USA");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);

        final String query = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = query;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);

        final int removedCount = handler.remove(collectionName, query, token);
        assertEquals(removedCount, 1);
    }

    @Test
    public void removeNotFoundTest() throws TException {
        final String classification = "AB";
        final Visibility vis = new Visibility().setFormalVisibility(classification);

        final EzSecurityToken token = TestUtils.createTestToken("AB", "USA");

        handler.insert(collectionName, new MongoEzbakeDocument("{eyes:'classified', hair:'black', foo:true}",
                vis), token);

        final String goodQuery = "{eyes:'classified'}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = goodQuery;
        final List<String> results = handler.find(collectionName, mongoFindParams, token);
        assertEquals(results.size(), 1);

        final String badQuery = "{eyes:'superclassified'}";
        mongoFindParams.jsonQuery = badQuery;
        final List<String> noResults = handler.find(collectionName, mongoFindParams, token);
        assertEquals(noResults.size(), 0);

        final int removedCount = handler.remove(collectionName, badQuery, token);
        assertEquals(removedCount, 0);
    }

    @Test
    public void findSpecialCharactersTest() throws TException {
        final String jsonQuery = "{eyes:'test\\'s color'}";

        final Visibility vis = new Visibility().setFormalVisibility("S");

        handler.insert(collectionName, new MongoEzbakeDocument(jsonQuery, vis), securityToken);

        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);
        assertEquals(results.size(), 1);
    }

    @Test
    public void find_AllTest() throws TException {
        insertTestAlphabetDocs();

        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.returnPlainObjectIdString = false;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);
        assertEquals(results.size(), 6);
    }

    @Test
    public void find_AllSortedTest() throws TException {
        insertTestAlphabetDocs();

        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.returnPlainObjectIdString = false;
        mongoFindParams.jsonSort = "{ letter: -1 }";

        // should get "A" as the last element
        List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);
        assertEquals(results.size(), 6);

        DBObject lastElement = (DBObject) JSON.parse(results.get(5));
        assertEquals(lastElement.get("letter"), "A");

        // should get "F" as the last element
        mongoFindParams.jsonSort = "{ letter: 1 }";
        results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);
        assertEquals(results.size(), 6);

        lastElement = (DBObject) JSON.parse(results.get(5));
        assertEquals(lastElement.get("letter"), "F");

        // should get "A" as the last element
        mongoFindParams.jsonSort = "{ display: 1 }";
        results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);
        assertEquals(results.size(), 6);

        lastElement = (DBObject) JSON.parse(results.get(5));
        assertEquals(lastElement.get("letter"), "A");
    }

    @Test
    public void findAllTest() throws TException {
        insertTestPeopleDocs();
        MongoFindParams mongoFindParams = new MongoFindParams();
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);
        assertEquals(results.size(), 6);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            assertNotNull(resultObj.get("_id"));
            assertNotNull(resultObj.get("eyes"));
            assertNotNull(resultObj.get("hair"));
            assertNotNull(resultObj.get("foo"));
        }
    }

    @Test
    public void textSearchTest() throws TException {
        insertTestPeopleDocs();

        // create text index
        final DBObject obj = new BasicDBObject();
        obj.put("eyes", "text");
        final String jsonKeys = JSON.serialize(obj);
        handler.createIndex(collectionName, jsonKeys, null, securityToken);

        final String searchText = "brown";
        final List<String> results = handler.textSearch(collectionName, searchText, securityToken);
        assertNotNull(results);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            assertNotNull(resultObj.get("_id"));
            assertEquals(resultObj.get("eyes"), "brown");
        }
    }

    @Test
    public void findTest() throws TException {
        insertTestPeopleDocs();
        final String jsonQuery = "{eyes:'blue'}";
        final boolean returnPlainObjectIdString = true;
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;
        mongoFindParams.returnPlainObjectIdString = returnPlainObjectIdString;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            assertNotNull(resultObj.get("_id"));
            assertEquals(resultObj.get("eyes"), "blue");
        }
    }

    @Test
    public void findAndReturnPlainObjectIdTest() throws TException {
        insertTestPeopleDocs();
        final String jsonQuery = "{eyes:'blue'}";
        final boolean returnPlainObjectIdString = true;
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;
        mongoFindParams.returnPlainObjectIdString = returnPlainObjectIdString;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            final String _id = (String) resultObj.get("_id");
            assertNotNull(_id);
            assertTrue(!result.contains("$oid"));
            assertEquals(resultObj.get("eyes"), "blue");
        }
    }

    @Test
    public void findAndReturnOIDObjectIdTest() throws TException {
        insertTestPeopleDocs();
        final String jsonQuery = "{eyes:'blue'}";
        final boolean returnPlainObjectIdString = false;
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;
        mongoFindParams.returnPlainObjectIdString = returnPlainObjectIdString;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            final ObjectId _id = (ObjectId) resultObj.get("_id");
            assertNotNull(_id);
            assertTrue(result.contains("$oid"));
            assertEquals(resultObj.get("eyes"), "blue");
        }
    }

    @Test
    public void findWithProjectionTest() throws TException {
        insertTestPeopleDocs();
        final String jsonQuery = "{eyes:'blue'}";
        final String projection = "{_id:false, eyes:1, hair:1}";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;
        mongoFindParams.jsonProjection = projection;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            assertNull(resultObj.get("_id"));
            assertNotNull(resultObj.get("hair"));
            assertEquals(resultObj.get("eyes"), "blue");
        }
    }

    @Test
    public void findPaginatedTest() throws TException {
        insertTestPeopleDocs();
        final String jsonQuery = "{eyes:'blue'}";
        final int skip = 1;
        final int limit = 2;
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;
        mongoFindParams.skip = skip;
        mongoFindParams.limit = limit;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);
        assertEquals(results.size(), 2);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            assertNotNull(resultObj.get("_id"));
            assertEquals(resultObj.get("eyes"), "blue");
        }
    }

    @Test
    public void findPaginatedWithProjectionTest() throws TException {
        insertTestPeopleDocs();
        final String jsonQuery = "{eyes:'blue'}";
        final String projection = "{_id:false, eyes:1, hair:1}";
        final int skip = 1;
        final int limit = 2;
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;
        mongoFindParams.jsonProjection = projection;
        mongoFindParams.skip = skip;
        mongoFindParams.limit = limit;
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertNotNull(results);

        for (final String result : results) {
            final DBObject resultObj = (DBObject) JSON.parse(result);
            assertNull(resultObj.get("_id"));
            assertNotNull(resultObj.get("hair"));
            assertEquals(resultObj.get("eyes"), "blue");
        }
    }

    @Test
    public void distinctTest() throws TException {
        MongoEzbakeDocument doc = new MongoEzbakeDocument();
        Visibility viz = new Visibility().setFormalVisibility("S");
        doc.setVisibility(viz);
        String json = " { field1: 'myValue' }";
        doc.setJsonDocument(json);

        final int numRecords = 5;
        for (int i = 0; i < numRecords; i++) {
            String result = handler.insert(collectionName, doc, securityToken);
            logger.info("insert result: {}", result);
        }

        // get & verify we inserted the 5 records
        MongoFindParams mongoFindParams = new MongoFindParams().setJsonQuery(json);
        final List<String> results = handler.find(collectionName, mongoFindParams, securityToken);
        assertEquals(numRecords, results.size());

        // call distinct()
        MongoDistinctParams mongoDistinctParams = new MongoDistinctParams().setField("field1").setQuery(json);
        List<String> distinctResults = handler.distinct(collectionName, mongoDistinctParams, securityToken);
        assertEquals(1, distinctResults.size());
        assertEquals("myValue", distinctResults.get(0));

        // add another doc, with a different value
        json = " { field1: 'myValue2' }";
        doc.setJsonDocument(json);
        String insertResult = handler.insert(collectionName, doc, securityToken);
        logger.info("insert result: {}", insertResult);

        // call distinct() again, this time without a query
        mongoDistinctParams = new MongoDistinctParams().setField("field1");
        distinctResults = handler.distinct(collectionName, mongoDistinctParams, securityToken);
        assertEquals(2, distinctResults.size());
        assertEquals("myValue", distinctResults.get(0));
        assertEquals("myValue2", distinctResults.get(1));
    }

    @Test
    public void redactionTest() throws TException, IOException {
        final URL docURL = Thread.currentThread().getContextClassLoader().getResource("doc1.json");
        if(docURL == null){
            throw new IOException("The doc1.json was missing from the JAR");
        }
        final String document = Resources.toString(docURL, Charsets.UTF_8);
        final Visibility vis = new Visibility().setFormalVisibility("S");

        final String id = handler.insert(collectionName, new MongoEzbakeDocument(document, vis),
                securityToken);
        assertNotNull(id);
        logger.debug("redactionTest - inserted id: " + id);

        final Set<String> auths = new HashSet<>(Arrays.asList("U", "C", "S", "USA"));
        final String jsonQuery = "{ _id: {'$oid' :'" + id + "'} }";
        MongoFindParams mongoFindParams = new MongoFindParams();
        mongoFindParams.jsonQuery = jsonQuery;

        final List<String> inserted =
                handler.find(collectionName, mongoFindParams,
                        TestUtils.createTestToken(id, auths, "mockAppSecId"));
        assertNotNull(inserted);
        assertEquals(1, inserted.size());
        logger.debug("inserted: " + inserted);

        final DBObject redacted = (DBObject) JSON.parse(inserted.get(0));

        DBObject redactedValue = (DBObject) redacted.get("s");
        assertNotNull(redactedValue);
        final String redactedValueData = (String) redactedValue.get("data");
        assertNotNull(redactedValueData);

        redactedValue = (DBObject) redacted.get("ab");
        assertNull(redactedValue);
    }

    @Test
    public void redactTestFV_ExtV() throws TException {
        // insert some test data
        MongoEzbakeDocument doc = new MongoEzbakeDocument();
        Visibility viz = new Visibility();
        viz.setFormalVisibility("S");

        AdvancedMarkings advancedMarkings = new AdvancedMarkings();
        advancedMarkings.setExternalCommunityVisibility("AB&USA");
        viz.setAdvancedMarkings(advancedMarkings);
        doc.setVisibility(viz);

        String json = " { field1: 1 }";
        doc.setJsonDocument(json);

        // set token auths - explicitly set the External Community auths
        Set<String> externalAuthsSet = new HashSet<>();
        externalAuthsSet.add("AB");
        externalAuthsSet.add("USA");
        securityToken.getAuthorizations().setExternalCommunityAuthorizations(externalAuthsSet);

        String result = handler.insert(collectionName, doc, securityToken);
        logger.info("insert result: {}", result);

        // try a find to return the result
        MongoFindParams findParams = new MongoFindParams();
        findParams.jsonQuery = json;

        List<String> results = handler.find(collectionName, findParams, securityToken);
        logger.info("find results - should have 1 result: {}", results);
        assertEquals(1, results.size());

        // change the external auths so that it won't return the result
        Authorizations authorizations = new Authorizations();
        Set<String> extAuthsSet = new HashSet<>();
        extAuthsSet.add("ABC");
        authorizations.setExternalCommunityAuthorizations(extAuthsSet);
        securityToken.setAuthorizations(authorizations);
        results = handler.find(collectionName, findParams, securityToken);
        logger.info("find results - should be no results: {}", results);
        assertEquals(0, results.size());
    }

    @Test
    public void redactTestFV_ExtV_ObjRV() throws TException {
        Long readViz = 11L;
        Long manageViz = 33L;
        Long writeViz = 55L;
        Long wrongReadViz = 22L;

        MongoEzbakeDocument doc = insertTestDataForPlatformViz(readViz, manageViz, writeViz);

        // set the wrong Platform Object read auths so that it won't return the result
        Set<Long> readAuthSet = new HashSet<>();
        readAuthSet.add(wrongReadViz);
        Authorizations authorizations = securityToken.getAuthorizations();
        authorizations.setPlatformObjectAuthorizations(readAuthSet);

        MongoFindParams findParams = new MongoFindParams();
        findParams.jsonQuery = doc.getJsonDocument();

        List<String> results = handler.find(collectionName, findParams, securityToken);
        logger.info("find results - should be no results: {}", results);
        assertEquals(0, results.size());
    }

    @Test
    public void insertTestObjWV() throws TException {
        Long readViz = 11L;
        Long manageViz = 33L;
        Long writeViz = 55L;

        MongoEzbakeDocument doc = insertTestDataForPlatformViz(readViz, manageViz, writeViz);

        // try another insert - this time, with no Write auth so that it will fail the insert.
        Set<Long> authSet = new HashSet<>();
        authSet.add(readViz);
        Authorizations authorizations = securityToken.getAuthorizations();
        authorizations.setPlatformObjectAuthorizations(authSet);

        try {
            handler.insert(collectionName, doc, securityToken);
            fail("We should have thrown an exception since there is no Manage auth!");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("User does not have all the required Platform auths to insert"));
        }
    }

    @Test
    public void updateTestObjWV() throws TException {
        Long readViz = 11L;
        Long manageViz = 33L;
        Long writeViz = 55L;
        Long wrongWriteViz = 100L;

        MongoEzbakeDocument doc = insertTestDataForPlatformViz(readViz, manageViz, writeViz);

        // try an update with the Write auth set so it will update.
        Set<Long> authSet = new HashSet<>();
        authSet.add(writeViz);
        Authorizations authorizations = securityToken.getAuthorizations();
        authorizations.setPlatformObjectAuthorizations(authSet);

        String updateDocString = "{ field1 : 'Updated' }";
        MongoEzbakeDocument updateDoc = new MongoEzbakeDocument();
        updateDoc.setJsonDocument(updateDocString);

        MongoUpdateParams updateParams = new MongoUpdateParams();
        updateParams.setJsonQuery(doc.getJsonDocument());
        updateParams.setMongoDocument(updateDoc);
        int updatedCount = handler.update(collectionName, updateParams, securityToken);
        assertEquals(updatedCount, 1);

        // try another update - this time with a wrong Write auth set so it won't update anything.
        authSet.remove(writeViz);
        authSet.add(wrongWriteViz);
        updateParams.setJsonQuery(updateDocString);
        updatedCount = handler.update(collectionName, updateParams, securityToken);
        assertEquals(updatedCount, 0);
    }

    @Test
    public void updateTestObjMV() throws TException {
        Long readViz = 11L;
        Long manageViz = 33L;
        Long writeViz = 55L;
        Long wrongManageViz = 300L;

        MongoEzbakeDocument doc = insertTestDataForPlatformViz(readViz, manageViz, writeViz);

        // try an update with the Manage auth set so it will update the security fields.
        Set<Long> authSet = new HashSet<>();
        authSet.add(manageViz);
        Authorizations authorizations = securityToken.getAuthorizations();
        authorizations.setPlatformObjectAuthorizations(authSet);

        String updateDocString = "{ field1 : 'Updated' }";
        MongoEzbakeDocument updateDoc = new MongoEzbakeDocument();
        updateDoc.setJsonDocument(updateDocString);
        // change the Formal Visibility - this is a Manage operation.
        Visibility vis = new Visibility();
        vis.setFormalVisibility("TS");
        updateDoc.setVisibility(vis);

        MongoUpdateParams updateParams = new MongoUpdateParams();
        updateParams.setJsonQuery(doc.getJsonDocument());
        updateParams.setMongoDocument(updateDoc);
        int updatedCount = handler.update(collectionName, updateParams, securityToken);
        assertEquals(updatedCount, 1);

        // try updating the MV field with wrong Manage Auth
        MongoEzbakeDocument insufficientMVDoc = new MongoEzbakeDocument();
        insufficientMVDoc.setJsonDocument(updateDocString);

        Visibility insufficientMVVis = new Visibility();
        Set<Long> manageVizSet = new HashSet<>();
        manageVizSet.add(wrongManageViz);
        PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
        platformObjectVisibilities.setPlatformObjectManageVisibility(manageVizSet);

        Set<Long> readVizSet = new HashSet<>();
        readVizSet.add(readViz);
        platformObjectVisibilities.setPlatformObjectReadVisibility(readVizSet);

        insufficientMVVis.setAdvancedMarkings(new AdvancedMarkings().setPlatformObjectVisibility(platformObjectVisibilities));
        insufficientMVDoc.setVisibility(insufficientMVVis);

        updateParams.setJsonQuery(insufficientMVDoc.getJsonDocument());
        updateParams.setMongoDocument(insufficientMVDoc);
        try {
            // this should throw an exception since the doc requires 300L MV, but
            //   the token only has 33L "Manage" auth.
            handler.update(collectionName, updateParams, securityToken);
            fail("should have thrown an exception!");
        } catch (EzMongoBaseException e) {
            // Expect to get here
        }

        // try another update - this time with a wrong Manage auth set so it won't update anything.
        authSet.remove(manageViz);
        authSet.add(wrongManageViz);
        updateParams.setJsonQuery(updateDocString);
        updatedCount = handler.update(collectionName, updateParams, securityToken);
        assertEquals(updatedCount, 0);
    }

    @Test
    public void removeTestObjWV() throws TException {
        Long readViz = 11L;
        Long manageViz = 33L;
        Long writeViz = 55L;
        Long wrongWriteViz = 100L;

        MongoEzbakeDocument doc = insertTestDataForPlatformViz(readViz, manageViz, writeViz);

        // try a remove with wrong Write auth set so it won't remove.
        Set<Long> authSet = new HashSet<>();
        authSet.add(wrongWriteViz);
        Authorizations authorizations = securityToken.getAuthorizations();
        authorizations.setPlatformObjectAuthorizations(authSet);

        int removedCount = handler.remove(collectionName, doc.getJsonDocument(), securityToken);
        assertEquals(0, removedCount);

        // try a remove with the Write auth set so it will remove.
        authSet.remove(wrongWriteViz);
        authSet.add(writeViz);

        removedCount = handler.remove(collectionName, doc.getJsonDocument(), securityToken);
        assertEquals(1, removedCount);
    }

    private MongoEzbakeDocument insertTestDataForPlatformViz(Long readViz, Long manageViz, Long writeViz)
            throws TException {
        // insert some test data
        MongoEzbakeDocument doc = new MongoEzbakeDocument();
        Visibility viz = new Visibility();
        viz.setFormalVisibility("S");

        AdvancedMarkings advancedMarkings = new AdvancedMarkings();
        advancedMarkings.setExternalCommunityVisibility("AB&USA");

        PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
        // set Read viz
        Set<Long> readVizSet = new HashSet<>();
        readVizSet.add(readViz);
        platformObjectVisibilities.setPlatformObjectReadVisibility(readVizSet);
        // set Manage viz
        Set<Long> manageVizSet = new HashSet<>();
        manageVizSet.add(manageViz);
        platformObjectVisibilities.setPlatformObjectManageVisibility(manageVizSet);
        // set Write viz
        Set<Long> writeVizSet = new HashSet<>();
        writeVizSet.add(writeViz);
        platformObjectVisibilities.setPlatformObjectWriteVisibility(writeVizSet);

        advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);

        viz.setAdvancedMarkings(advancedMarkings);
        doc.setVisibility(viz);

        String json = " { field1: 1 }";
        doc.setJsonDocument(json);

        // set the Platform Object Write & Read auths so that we are allowed to insert & read
        Authorizations authorizations = securityToken.getAuthorizations();
        Set<Long> authSet = new HashSet<>();
        authSet.add(writeViz);
        authSet.add(readViz);
        authorizations.setPlatformObjectAuthorizations(authSet);

        // set the External Community auths
        Set<String> externalAuthsSet = new HashSet<>();
        externalAuthsSet.add("AB");
        externalAuthsSet.add("USA");
        authorizations.setExternalCommunityAuthorizations(externalAuthsSet);

        String result = handler.insert(collectionName, doc, securityToken);
        logger.info("insert result: {}", result);

        // try a find to return the result
        MongoFindParams findParams = new MongoFindParams();
        findParams.jsonQuery = json;

        List<String> results = handler.find(collectionName, findParams, securityToken);
        logger.info("find results - should have 1 result: {}", results);
        assertEquals(1, results.size());

        return doc;
    }

    private MongoEzbakeDocument insertTestDataForPurge(long purgeId, boolean composite, String jsonDoc)
            throws TException {
        // insert some test data
        MongoEzbakeDocument doc = new MongoEzbakeDocument();
        Visibility viz = new Visibility();
        viz.setFormalVisibility("S");

        AdvancedMarkings advancedMarkings = new AdvancedMarkings();
        advancedMarkings.setComposite(composite);
        advancedMarkings.setId(purgeId);
        viz.setAdvancedMarkings(advancedMarkings);

        doc.setVisibility(viz);
        doc.setJsonDocument(jsonDoc);

        String result = handler.insert(collectionName, doc, securityToken);
        logger.info("insert result: {}", result);

        // try a find to return the result
        MongoFindParams findParams = new MongoFindParams();
        findParams.jsonQuery = jsonDoc;

        List<String> results = handler.find(collectionName, findParams, securityToken);
        logger.info("find results - should have 1 result: {}", results);
        assertEquals(1, results.size());

        return doc;
    }

    @Test
    public void purgeSimpleTest() throws TException {

        Long toPurgeId = 1L;
        long purgeId = 1;
        int numRecords = 10;

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ field: " + i + " }";
            insertTestDataForPurge(toPurgeId, false, json);
        }

        PurgeItems purgeItems = new PurgeItems();
        Set<Long> itemSet = new HashSet<>();
        itemSet.add(toPurgeId);
        purgeItems.setItems(itemSet);
        purgeItems.setPurgeId(purgeId);

        PurgeOptions purgeOptions = new PurgeOptions();
        purgeOptions.setBatchSize(10);

        PurgeResult result = handler.purge(purgeItems, purgeOptions, securityToken);
        assertEquals(1, result.getPurged().size());
        assertTrue(result.getPurged().contains(toPurgeId));
    }

    @Test
    public void purgeCompositeSimpleTest() throws TException {

        Long toPurgeId = 1L;
        long purgeId = 1;
        int numRecords = 10;

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ field: " + i + " }";
            insertTestDataForPurge(toPurgeId, true, json);
        }

        PurgeItems purgeItems = new PurgeItems();
        Set<Long> itemSet = new HashSet<>();
        itemSet.add(toPurgeId);
        purgeItems.setItems(itemSet);
        purgeItems.setPurgeId(purgeId);

        PurgeOptions purgeOptions = new PurgeOptions();
        purgeOptions.setBatchSize(10);

        PurgeResult result = handler.purge(purgeItems, purgeOptions, securityToken);
        assertEquals(0, result.getPurged().size());
        assertEquals(1, result.getUnpurged().size());
        assertTrue(result.getUnpurged().contains(toPurgeId));
    }

    @Test
    public void purgeSelectiveSimpleTest() throws TException {

        Long toPurgeId = 1L;
        Long notToPurgeId = 2L;
        long purgeId = 1;
        int numRecords = 10;

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ field: " + i + " }";
            insertTestDataForPurge(toPurgeId, false, json);
        }

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ anotherField: " + i + " }";
            insertTestDataForPurge(notToPurgeId, false, json);
        }

        PurgeItems purgeItems = new PurgeItems();
        Set<Long> itemSet = new HashSet<>();
        itemSet.add(toPurgeId);
        purgeItems.setItems(itemSet);
        purgeItems.setPurgeId(purgeId);

        PurgeOptions purgeOptions = new PurgeOptions();
        purgeOptions.setBatchSize(10);

        PurgeResult result = handler.purge(purgeItems, purgeOptions, securityToken);
        assertEquals(1, result.getPurged().size());
        assertTrue(result.getPurged().contains(toPurgeId));
    }

    @Test
    public void purgeTwoPurgeIdsSimpleTest() throws TException {

        Long toPurgeId = 1L;
        Long toAlsoPurgeId = 2L;
        long purgeId = 1;
        int numRecords = 10;

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ field: " + i + " }";
            insertTestDataForPurge(toPurgeId, false, json);
        }

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ anotherField: " + i + " }";
            insertTestDataForPurge(toAlsoPurgeId, false, json);
        }

        PurgeItems purgeItems = new PurgeItems();
        Set<Long> itemSet = new HashSet<>();
        itemSet.add(toPurgeId);
        itemSet.add(toAlsoPurgeId);
        purgeItems.setItems(itemSet);
        purgeItems.setPurgeId(purgeId);

        PurgeOptions purgeOptions = new PurgeOptions();
        purgeOptions.setBatchSize(100);

        PurgeResult result = handler.purge(purgeItems, purgeOptions, securityToken);
        boolean isFinished = result.isIsFinished();
        while ( !isFinished ) {
        	isFinished = handler.purge(purgeItems, purgeOptions, securityToken).isIsFinished();
        }
        assertEquals(2, result.getPurged().size());
        assertTrue(result.getPurged().contains(toPurgeId));
        assertTrue(result.getPurged().contains(toAlsoPurgeId));
    }

    @Test
    public void purgeIsFinishedTest() throws TException {

        Long toPurgeId = 1L;
        Long toAlsoPurgeId = 2L;
        long purgeId = 1;
        int numRecords = 10;

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ field: " + i + " }";
            insertTestDataForPurge(toPurgeId, false, json);
        }

        for (int i = 1; i <= numRecords; i++) {
            String json = "{ anotherField: " + i + " }";
            insertTestDataForPurge(toAlsoPurgeId, false, json);
        }

        PurgeItems purgeItems = new PurgeItems();
        Set<Long> itemSet = new HashSet<>();
        itemSet.add(toPurgeId);
        itemSet.add(toAlsoPurgeId);
        purgeItems.setItems(itemSet);
        purgeItems.setPurgeId(purgeId);

        PurgeOptions purgeOptions = new PurgeOptions();
        purgeOptions.setBatchSize(4);

        int numOfPurges = 1;
        PurgeResult result = handler.purge(purgeItems, purgeOptions, securityToken);
        boolean isFinished = result.isIsFinished();
        while ( !isFinished ) {
        	isFinished = handler.purge(purgeItems, purgeOptions, securityToken).isIsFinished();
        	numOfPurges++;
        }
        assertEquals(5, numOfPurges);
    }

    @Test
    public void sortedCollectionTest() throws TException {
        final Set<String> setofstrings = new HashSet<>();
        setofstrings.add("testapp_collI");
        setofstrings.add("testapp_collD");
        setofstrings.add("testapp_collB");
        setofstrings.add("testapp_collY");
        setofstrings.add("testapp_collE");
        setofstrings.add("testapp_collJ");
        setofstrings.add("testapp_collL");
        setofstrings.add("testapp_collA");
        setofstrings.add("testapp_collN");
        setofstrings.add("testapp_collH");
        setofstrings.add("testapp_collM");
        setofstrings.add("testapp_collF");
        setofstrings.add("testapp_collC");
        setofstrings.add("testapp_collG");
        setofstrings.add("testapp_collK");

        final List<String> sortedList = MongoConverter.asSortedList(setofstrings);

        assertEquals(15,sortedList.size());
        assertEquals("testapp_collA",sortedList.get(0));
        assertEquals("testapp_collH",sortedList.get(7));
        assertEquals("testapp_collF",sortedList.get(5));
        assertEquals("testapp_collM",sortedList.get(12));
        assertEquals("testapp_collD",sortedList.get(3));
        assertEquals("testapp_collY",sortedList.get(14));
    }

    @Test
    public void iteratorTest() throws TException {
        final Set<String> setofstrings = new HashSet<>();
        setofstrings.add("testapp_collI");
        setofstrings.add("testapp_collD");
        setofstrings.add("testapp_collB");
        setofstrings.add("testapp_collY");
        setofstrings.add("testapp_collE");
        setofstrings.add("testapp_collJ");
        setofstrings.add("testapp_collL");
        setofstrings.add("testapp_collA");
        setofstrings.add("testapp_collN");
        setofstrings.add("testapp_collH");
        setofstrings.add("testapp_collM");
        setofstrings.add("testapp_collF");
        setofstrings.add("testapp_collC");
        setofstrings.add("testapp_collG");
        setofstrings.add("testapp_collK");

        final List<String> sortedList = MongoConverter.asSortedList(setofstrings);

        assertEquals(15,sortedList.size());
        assertEquals("testapp_collA",sortedList.get(0));
        assertEquals("testapp_collH",sortedList.get(7));
        assertEquals("testapp_collF",sortedList.get(5));
        assertEquals("testapp_collM",sortedList.get(12));
        assertEquals("testapp_collD",sortedList.get(3));
        assertEquals("testapp_collY",sortedList.get(14));

        String purgeStartCollName = "testapp_collF";

        // TODO - what is this really supposed to be
        ListIterator<String> collNamesIterator = null;
        if ( purgeStartCollName != null ) {
            int index = sortedList.indexOf(purgeStartCollName);
            collNamesIterator = sortedList.listIterator(index);
        } else {
            collNamesIterator = sortedList.listIterator();
        }

        int itemsCount = 0;
        while (collNamesIterator.hasNext()) {
            String collName = collNamesIterator.next();
            System.out.print("Collection Name: " + collName);
            itemsCount++;
        }

        assertEquals(10,itemsCount);
    }

    @Test
    public void pingTest() throws TException {
        assertTrue("Failed the ping test", handler.ping());
    }

}
