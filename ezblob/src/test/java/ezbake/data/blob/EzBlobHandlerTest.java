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

package ezbake.data.blob;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.PlatformObjectVisibilities;
import ezbake.base.thrift.Visibility;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.data.base.blob.thrift.Blob;
import ezbake.data.base.blob.thrift.BlobException;
import ezbake.data.base.thrift.PurgeItems;
import ezbake.data.base.thrift.PurgeOptions;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.data.test.TestUtils;
import ezbake.security.client.EzBakeSecurityClientConfigurationHelper;
import ezbakehelpers.accumulo.AccumuloHelper;
import ezbakehelpers.accumulo.NamespacedConnector;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EzBlobHandlerTest {
    private static final Logger logger = LoggerFactory.getLogger(EzBlobHandlerTest.class);
    private final String TEST_BUCKET = "TEST_BUCKET";
    EzBlobHandler handler;
    private EzSecurityToken USER_WITH_TS_S_B;

    @Before
    public void setUp() throws TException, AccumuloException, AccumuloSecurityException, IOException {
        configureMockAccumuloAndSecurityService();
        USER_WITH_TS_S_B = TestUtils.createTS_S_B_User();
    }

    @After
    public void tearDown() throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException, NamespaceNotEmptyException, TableNotFoundException {
        final Properties props = new Properties();

        final String namespaceName = "test";

        props.setProperty(EzBakeSecurityClientConfigurationHelper.USE_MOCK_KEY, "true");
        props.setProperty(EzBakePropertyConstants.ACCUMULO_USE_MOCK, "true");
        props.setProperty(EzBakePropertyConstants.ACCUMULO_NAMESPACE, namespaceName);
        props.setProperty(EzBakePropertyConstants.ACCUMULO_PASSWORD, "");
        props.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, TestUtils.MOCK_APP_SEC_ID);

        // create namespace in the mock accumulo
        final AccumuloHelper accumuloHelper = new AccumuloHelper(props);
        final Instance instance = new MockInstance(accumuloHelper.getAccumuloInstance());
        Connector connector = instance.getConnector(accumuloHelper.getAccumuloUsername(),
                new PasswordToken(accumuloHelper.getAccumuloPassword()));
        connector = new NamespacedConnector(connector, accumuloHelper.getAccumuloNamespace());

        connector.tableOperations().delete("null_blobstore");
        connector.tableOperations().delete("null_purgeTable");
        connector.namespaceOperations().delete(namespaceName);
        handler = null;
    }

    /**
     * Test of ping method, of class BlobStoreServiceHandler.
     */
    @Test
    public void testPing() {
        System.out.println("ping");
        final boolean expResult = true;
        final boolean result = handler.ping();
        assertEquals(expResult, result);
    }

    /**
     * Test of putBlob method, of class BlobStoreServiceHandler.
     *
     * @throws java.lang.Exception
     */
    @Test(expected = BlobException.class)
    public void testPutBlobNullClassification() throws Exception {
        System.out.println("testPutBlobNullClassification");

        // Expect an exception is thrown since the Visibility is null.
        final Blob entry = new Blob(TEST_BUCKET, "TEST_KEY", ByteBuffer.wrap("SOME BLOB DATA".getBytes()), null);

        final EzSecurityToken token = TestUtils.createTestToken("S", "USA");
        handler.putBlob(entry, token);
    }

    /**
     * Test of getBlob method, of class BlobStoreServiceHandler.
     */
    @Test
    public void testGetBlob() throws Exception {
        final byte[] byteArrayStored = "SOME BLOB DATA TO PUT & GET".getBytes(),
                // Empty at initialization
                byteArrayRetreived = new byte[byteArrayStored.length];

        final ByteBuffer blobToStore = ByteBuffer.wrap(byteArrayStored);

        final String someTestBucket = TEST_BUCKET + "_TESTING", someTestKey = "testRemovalKey";
        final Visibility visibility = new Visibility().setFormalVisibility("TS");
        final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
        final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
        platformObjectVisibilities.setPlatformObjectReadVisibility(Sets.newHashSet(1L));
        advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
        visibility.setAdvancedMarkings(advancedMarkings);
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
        final Blob entryToStore = new Blob(someTestBucket, someTestKey, blobToStore, visibility);
        handler.createBucket(someTestBucket, new Visibility().setFormalVisibility("S"), USER_WITH_TS_S_B);
        handler.putBlob(entryToStore, USER_WITH_TS_S_B);
        System.out.println("Inserted blob with subset of user auths");

        Set<ByteBuffer> blobs = handler.getBlobs(someTestBucket, someTestKey, USER_WITH_TS_S_B);
        assertEquals(1, Iterables.size(blobs));
        final ByteBuffer retreivedBlob = Iterables.getFirst(blobs, null);

        retreivedBlob.get(byteArrayRetreived);

        assertTrue(Arrays.equals(byteArrayStored, byteArrayRetreived));

        // try to get the blob with a user that doesn't have TS auth - we shouldn't be able to get it.
        final EzSecurityToken token = TestUtils.createTestToken("S", "USA");
        token.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));

        blobs = handler.getBlobs(someTestBucket, someTestKey, token);
        assertEquals(0, Iterables.size(blobs));

        // make sure if proper BV index isn't set for read no blob is returned
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));

        blobs = handler.getBlobs(someTestBucket, someTestKey, USER_WITH_TS_S_B);
        assertEquals(0, Iterables.size(blobs));
    }

    /**
     * Test putBlob method and verify that there is a maximum size enforced
     */
    @Test
    public void testMaxBlobSize() throws Exception {
        final Visibility visibility = new Visibility().setFormalVisibility("TS");
        final String bucket = TEST_BUCKET + "_TESTING_MAX_BLOB_SIZE", key = "testRemovalKey";

        // Generate byte array that's 127 MB and test getting it back
        byte[] byteArrayStored = generateData(127);
        final byte[] // Empty at initialization
                byteArrayRetreived = new byte[byteArrayStored.length];

        handler.createBucket(bucket, visibility, USER_WITH_TS_S_B);
        handler.putBlob(new Blob(bucket, key, ByteBuffer.wrap(byteArrayStored), visibility), USER_WITH_TS_S_B);

        final Set<ByteBuffer> blobs = handler.getBlobs(bucket, key, USER_WITH_TS_S_B);
        assertEquals(1, Iterables.size(blobs));
        final ByteBuffer retreivedBlob = Iterables.getFirst(blobs, null);

        retreivedBlob.get(byteArrayRetreived);
        assertArrayEquals(byteArrayStored, byteArrayRetreived);

        try {
            // Generate byte array that's 129 MB and verify it can't be stored
            byteArrayStored = generateData(129);

            handler.putBlob(new Blob(bucket, key, ByteBuffer.wrap(byteArrayStored), visibility), USER_WITH_TS_S_B);
            fail("Expected an exception to be thrown");
        } catch (final BlobException e) {
            if (!e.getMessage().contains("The blob you're trying to store is too big!")) {
                throw new RuntimeException("This test is failed", e);
            }
        }
    }

    private byte[] generateData(int sizeInMb) {
        final int size = sizeInMb * 1000 * 1000;
        final byte[] randomByteArray = new byte[size];
        new Random().nextBytes(randomByteArray);
        return randomByteArray;
    }

    /**
     * Test of removeBlob method, of class BlobStoreServiceHandler.
     */
    @Test
    public void testRemoveBlob() throws Exception {
        // Expect no exceptionn is thrown since the user does have "TS, S and B" authorizations. Also tests
        // the ability of Accumulo to normalize visibility since "TS&S&((((B))))" is logically equivalent
        // to "TS&S&B"
        try {
            final ByteBuffer blobToDelete = ByteBuffer.wrap("SOME BLOB DATA TO REMOVE".getBytes());
            final String removeMeBucket = TEST_BUCKET + "_REMOVAL", removeMeKey = "testRemovalKey";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            final Blob entryToDelete = new Blob(removeMeBucket, removeMeKey, blobToDelete, visibility);

            // Store a known blob
            handler.createBucket(removeMeBucket, visibility, USER_WITH_TS_S_B);
            handler.putBlob(entryToDelete, USER_WITH_TS_S_B);

            // Change the user permissions, attempt to remove the blob
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            int affectedRows = handler.removeBlob(removeMeBucket, removeMeKey, USER_WITH_TS_S_B);
            // Confirm 0 rows affected
            assertEquals(0, affectedRows);

            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            // Remove the blob
            affectedRows = handler.removeBlob(removeMeBucket, removeMeKey, USER_WITH_TS_S_B);
            // Confirm only 1 row affected
            assertEquals(1, affectedRows);
            // Try to remove the same blob again
            affectedRows = handler.removeBlob(removeMeBucket, removeMeKey, USER_WITH_TS_S_B);
            // Confirm 0 rows affected
            assertEquals(0, affectedRows);

        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    @Test
    public void testCopyBlob() throws Exception {
        final ByteBuffer blobToCopy = ByteBuffer.wrap("SOME BLOB DATA TO COPY".getBytes());
        final String sourceBucket = TEST_BUCKET + "_testCopyBlobSource", sourceKey = "testCopySourceKey";
        final String destBucket = TEST_BUCKET + "_testCopyBlobDest", destKey = "testCopySourceKey";
        final Visibility visibility = new Visibility().setFormalVisibility("TS");
        final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
        final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
        platformObjectVisibilities.setPlatformObjectReadVisibility(Sets.newHashSet(1L));
        platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
        advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
        visibility.setAdvancedMarkings(advancedMarkings);

        final Blob sourceBlob = new Blob(sourceBucket, sourceKey, blobToCopy, visibility);
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
        handler.createBucket(sourceBucket, visibility, USER_WITH_TS_S_B);
        handler.createBucket(destBucket, visibility, USER_WITH_TS_S_B);
        handler.putBlob(sourceBlob, USER_WITH_TS_S_B);

        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
        handler.copyBlob(sourceBucket, sourceKey, destBucket, destKey, USER_WITH_TS_S_B);
        Set<ByteBuffer> returnedBlobs = handler.getBlobs(destBucket, destKey, USER_WITH_TS_S_B);
        assertFalse(returnedBlobs.contains(blobToCopy));
        assertTrue(returnedBlobs.size() == 0);

        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
        handler.copyBlob(sourceBucket, sourceKey, destBucket, destKey, USER_WITH_TS_S_B);
        returnedBlobs = handler.getBlobs(destBucket, destKey, USER_WITH_TS_S_B);
        assertTrue(returnedBlobs.contains(blobToCopy));
        assertTrue(returnedBlobs.size() == 1);
    }

    /**
     * Test createBucket, and put get and remove a blob from it
     *
     * @throws Exception
     */
    @Test
    public void testCreateBucket() throws Exception {
        try {
            final String createBucket = TEST_BUCKET + "_CREATE";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            handler.createBucket(createBucket, visibility, USER_WITH_TS_S_B);

            final byte[] byteArrayStored = "SOME BLOB DATA TO PUT GET AND REMOVE".getBytes(), byteArrayRetreived =
                    new byte[byteArrayStored.length];
            final ByteBuffer blobToStore = ByteBuffer.wrap(byteArrayStored);

            final Blob addEntry = new Blob(createBucket, "testkey", blobToStore, visibility);
            handler.putBlob(addEntry, USER_WITH_TS_S_B);

            final Set<ByteBuffer> blobs = handler.getBlobs(createBucket, "testkey", USER_WITH_TS_S_B);
            assertEquals(1, Iterables.size(blobs));
            final ByteBuffer retreivedBlob = Iterables.getFirst(blobs, null);
            retreivedBlob.get(byteArrayRetreived);
            assertTrue(Arrays.equals(byteArrayStored, byteArrayRetreived));

            int affectedRows = handler.removeBlob(createBucket, "testkey", USER_WITH_TS_S_B);
            assertEquals(1, affectedRows);
            affectedRows = handler.removeBlob(createBucket, "testkey", USER_WITH_TS_S_B);
            assertEquals(0, affectedRows);

        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    /**
     * Test doesBucketExist with createBucket bucket
     *
     * @throws Exception
     */
    @Test
    public void testDoesBucketExist() throws Exception {
        try {
            final String checkBucket = TEST_BUCKET + "_testDoesBucketExist1";
            final String checkBucket2 = TEST_BUCKET + "_testDoesBucketExist2";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectDiscoverVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));

            handler.createBucket(checkBucket, visibility, USER_WITH_TS_S_B);
            handler.createBucket(checkBucket2, visibility, USER_WITH_TS_S_B);

            assertTrue(handler.doesBucketExist(checkBucket, USER_WITH_TS_S_B));
            assertTrue(handler.doesBucketExist(checkBucket2, USER_WITH_TS_S_B));
            assertFalse(handler.doesBucketExist("SomeNonExistentBucket", USER_WITH_TS_S_B));

            // Check returns false when incorrect permissions set
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(0L));
            assertFalse(handler.doesBucketExist(checkBucket, USER_WITH_TS_S_B));

        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    /**
     * Test deleteBucket with createBucket bucket
     *
     * @throws Exception
     */
    @Test
    public void testDeleteBucket() throws Exception {
        try {
            final String deleteBucket = TEST_BUCKET + "_testDeleteBucket", key = "testKey";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            final ByteBuffer blob = ByteBuffer.wrap("SOME BLOB DATA TO REMOVE".getBytes());

            final Blob entry = new Blob(deleteBucket, key, blob, visibility);
            handler.createBucket(deleteBucket, visibility, USER_WITH_TS_S_B);
            handler.putBlob(entry, USER_WITH_TS_S_B);

            handler.deleteBucket(deleteBucket, USER_WITH_TS_S_B);


            assertFalse(handler.listBuckets(USER_WITH_TS_S_B).contains(deleteBucket));

            // Do it over again, change the user auths and make sure it doesn't get deleted
            handler.createBucket(deleteBucket, visibility, USER_WITH_TS_S_B);
            handler.putBlob(entry, USER_WITH_TS_S_B);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            handler.deleteBucket(deleteBucket, USER_WITH_TS_S_B);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            assertTrue(handler.listBuckets(USER_WITH_TS_S_B).contains(deleteBucket));
        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    /**
     * Test doesBlobtExist and check it returns false when key or bucket does not exist
     *
     * @throws Exception
     */
    @Test
    public void testDoesBlobExist() throws Exception {
        try {
            final ByteBuffer blob = ByteBuffer.wrap("SOME BLOB DATA TO REMOVE".getBytes());
            final String bucket = TEST_BUCKET + "_TESTBUCKET", key = "testKey";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectDiscoverVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            final Blob entry = new Blob(bucket, key, blob, visibility);

            // Store a known blob
            handler.createBucket(bucket, visibility, USER_WITH_TS_S_B);
            handler.putBlob(entry, USER_WITH_TS_S_B);
            assertTrue(handler.doesBlobExist(bucket, key, USER_WITH_TS_S_B));
            assertFalse(handler.doesBlobExist(bucket, "someotherkey", USER_WITH_TS_S_B));
            assertFalse(handler.doesBlobExist("someotherbucket", key, USER_WITH_TS_S_B));

            // Change user permission and make sure it doesn't show up
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            assertFalse(handler.doesBlobExist(bucket, key, USER_WITH_TS_S_B));
        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    /**
     * Tests getBlobVisiblity()
     */
    @Test
    public void testGetBlobVisibility() {
        try {
            final String bucket = TEST_BUCKET + "_testGetBlobVisibility", key = "testKey";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            platformObjectVisibilities.setPlatformObjectManageVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            final Blob entry = new Blob(bucket, key, ByteBuffer.wrap("SOME BLOB DATA".getBytes()), visibility);
            handler.createBucket(bucket, visibility, USER_WITH_TS_S_B);
            handler.putBlob(entry, USER_WITH_TS_S_B);

            final List<Visibility> visibilities = handler.getBlobVisibility(bucket, key, USER_WITH_TS_S_B);
            assertEquals(visibility, visibilities.get(0));

            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            assertEquals(new ArrayList<Visibility>(), handler.getBlobVisibility(bucket, key, USER_WITH_TS_S_B));
        } catch (final BlobException e) {
            printAndFailUnexpectedExc(e);
        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    /**
     * Tests getBucketVisibility
     */
    @Test
    public void testGetBucketVisibility() {
        try {
            final String bucket = TEST_BUCKET + "_TESTBUCKET";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectManageVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));

            handler.createBucket(bucket, visibility, USER_WITH_TS_S_B);
            final Visibility retrievedVisibility = handler.getBucketVisibility(bucket, USER_WITH_TS_S_B);
            assertEquals(visibility, retrievedVisibility);

        } catch (final BlobException e) {
            printAndFailUnexpectedExc(e);
        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    /**
     * Tests getBucketVisibility
     */
    @Test(expected = BlobException.class)
    public void testGetBucketVisibilityThrowsBlobExceptionWhenPermissionsIncorrect() throws BlobException, TException {
        final String bucket = TEST_BUCKET + "_TESTBUCKET";
        final Visibility visibility = new Visibility().setFormalVisibility("TS");
        final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
        final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
        platformObjectVisibilities.setPlatformObjectManageVisibility(Sets.newHashSet(1L));
        advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
        visibility.setAdvancedMarkings(advancedMarkings);
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));

        handler.createBucket(bucket, visibility, USER_WITH_TS_S_B);
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
        handler.getBucketVisibility(bucket, USER_WITH_TS_S_B);
    }

    /**
     * Tests listBuckets
     */
    @Test
    public void testListBuckets() {
        try {
            final Set<String> expectedBuckets = new HashSet<>();

            final String testListBuckets1 = TEST_BUCKET + "_testListBuckets1";
            final String testListBuckets2 = TEST_BUCKET + "_testListBuckets2";
            expectedBuckets.add(testListBuckets1);
            expectedBuckets.add(testListBuckets2);
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectReadVisibility(Sets.newHashSet(1L));
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            handler.createBucket(testListBuckets1, visibility, USER_WITH_TS_S_B);
            handler.createBucket(testListBuckets2, visibility, USER_WITH_TS_S_B);

            Set<String> receivedBuckets = handler.listBuckets(USER_WITH_TS_S_B);

            assertEquals(expectedBuckets, Sets.intersection(expectedBuckets, receivedBuckets));

            // Change user permission, make sure empty list returned
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            receivedBuckets = handler.listBuckets(USER_WITH_TS_S_B);
            assertFalse(receivedBuckets.contains(testListBuckets1));
            assertFalse(receivedBuckets.contains(testListBuckets2));
        } catch (final BlobException e) {
            printAndFailUnexpectedExc(e);
        } catch (final TException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests listBlobs
     */
    @Test
    public void testListBlobs() {
        try {
            final List<Blob> expectedBlobs = new ArrayList<>();

            final String createBucket = TEST_BUCKET + "_LIST";
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectReadVisibility(Sets.newHashSet(1L));
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            visibility.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            handler.createBucket(createBucket, visibility, USER_WITH_TS_S_B);

            final byte[] byteArrayStored = "SOME BLOB DATA".getBytes();
            final ByteBuffer blobToStore = ByteBuffer.wrap(byteArrayStored);

            final Blob addEntry = new Blob(createBucket, "testkey", blobToStore, visibility);
            handler.putBlob(addEntry, USER_WITH_TS_S_B);

            expectedBlobs.add(addEntry);
            List<Blob> receivedBlobs = handler.listBlobs(createBucket, USER_WITH_TS_S_B);

            assertEquals(expectedBlobs, receivedBlobs);
            // Change user permission, make sure empty list is returned
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            receivedBlobs = handler.listBlobs(createBucket, USER_WITH_TS_S_B);
            assertEquals(0, receivedBlobs.size());
        } catch (final BlobException e) {
            printAndFailUnexpectedExc(e);
        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    /**
     * Tests setBucketVisibility
     */
    @Test
    public void testSetBucketVisibility() {
        try {
            final String bucket = TEST_BUCKET + "_testSetBucketVisibility";
            final Visibility secret = new Visibility().setFormalVisibility("S");
            AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectManageVisibility(Sets.newHashSet(1L));
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            secret.setAdvancedMarkings(advancedMarkings);
            final Visibility topSecret = new Visibility().setFormalVisibility("TS");
            advancedMarkings = new AdvancedMarkings();
            platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectManageVisibility(Sets.newHashSet(1L));
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            topSecret.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            handler.createBucket(bucket, topSecret, USER_WITH_TS_S_B);

            // Set permission incorrectly to disallow vis management, make sure setBucketVisibility doesn't change
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            handler.setBucketVisibility(bucket, secret, USER_WITH_TS_S_B); // Downgrading visibility from TS to S
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            assertEquals(topSecret, handler.getBucketVisibility(bucket, USER_WITH_TS_S_B));

            // Set permission correctly to allow vis management and make sure it changes
            handler.setBucketVisibility(bucket, secret, USER_WITH_TS_S_B); // Downgrading visibility from TS to S
            final Visibility receivedVisibility = handler.getBucketVisibility(bucket, USER_WITH_TS_S_B);
            assertEquals(secret, receivedVisibility);

        } catch (final BlobException e) {
            printAndFailUnexpectedExc(e);
        } catch (final TException e) {
            printAndFailUnexpectedExc(e);
        }
    }

    @Test
    public void testSetBlobVisibility() {
        try {
            final String bucket = TEST_BUCKET + "_testSetBlobVisibility", key = "testKey";
            final Visibility secret = new Visibility().setFormalVisibility("S");
            AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectManageVisibility(Sets.newHashSet(1L));
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            secret.setAdvancedMarkings(advancedMarkings);
            final Visibility topSec = new Visibility().setFormalVisibility("TS");
            advancedMarkings = new AdvancedMarkings();
            platformObjectVisibilities = new PlatformObjectVisibilities();
            platformObjectVisibilities.setPlatformObjectManageVisibility(Sets.newHashSet(1L));
            platformObjectVisibilities.setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
            advancedMarkings.setPlatformObjectVisibility(platformObjectVisibilities);
            topSec.setAdvancedMarkings(advancedMarkings);
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            final Blob entry = new Blob(bucket, key, ByteBuffer.wrap("SOME BLOB DATA".getBytes()), topSec);
            handler.createBucket(bucket, topSec, USER_WITH_TS_S_B);
            handler.putBlob(entry, USER_WITH_TS_S_B);

            // Set permission incorrectly to disallow vis management, make sure setBucketVisibility doesn't change
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(42L));
            handler.setBlobVisibility(bucket, key, secret, USER_WITH_TS_S_B); // Downgrading visibility from TS to S
            USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            String receivedVisibility =
                    handler.getBlobVisibility(bucket, key, USER_WITH_TS_S_B).get(0).getFormalVisibility();
            assertEquals("TS", receivedVisibility);

            // Set permission correctly to allow vis management and make sure it changes
            handler.setBlobVisibility(bucket, key, secret, USER_WITH_TS_S_B); // Downgrading visibility from TS to S
            receivedVisibility =
                    handler.getBlobVisibility(bucket, key, USER_WITH_TS_S_B).get(0).getFormalVisibility();
            assertEquals("S", receivedVisibility);

        } catch (final BlobException e) {
            printAndFailUnexpectedExc(e);
        } catch (final TException e) {
            printAndFailUnexpectedExc(e);

        }
    }

    private void configureMockAccumuloAndSecurityService() {
        try {
            final Properties props = new Properties();
            final String namespaceName = "test";

            props.setProperty(EzBakeSecurityClientConfigurationHelper.USE_MOCK_KEY, "true");
            props.setProperty(EzBakePropertyConstants.ACCUMULO_USE_MOCK, "true");
            props.setProperty(EzBakePropertyConstants.ACCUMULO_NAMESPACE, namespaceName);
            props.setProperty(EzBakePropertyConstants.ACCUMULO_PASSWORD, "");
            props.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, TestUtils.MOCK_APP_SEC_ID);

            // create namespace in the mock accumulo
            AccumuloHelper accumuloHelper = new AccumuloHelper(props);
            Instance instance = new MockInstance(accumuloHelper.getAccumuloInstance());
            Connector connector = instance.getConnector(accumuloHelper.getAccumuloUsername(),
                    new PasswordToken(accumuloHelper.getAccumuloPassword()));
            connector = new NamespacedConnector(connector, accumuloHelper.getAccumuloNamespace());
            NamespaceOperations namespaceOperations = connector.namespaceOperations();
            if (!namespaceOperations.exists(namespaceName)) {
                namespaceOperations.create(namespaceName);
            }
            handler = new EzBlobHandler();
            handler.setConfigurationProperties(props);
            // Calls init()
            handler.getThriftProcessor();

            final String effectiveProperties = handler.getConfigurationProperties().toString();
            logger.info("Effective properties for unit test: \n" + effectiveProperties);
        } catch (final Exception ex) {
            System.err.println("Exception initializing test resources: \n" + ex);
        }
    }

    private void printAndFailUnexpectedExc(Exception e) {
        e.printStackTrace();
        fail("Unexpected exception was thrown");
    }

    @Test
    public void testPutBlobThrowsExceptionWhenBucketDoesntExist() {
        boolean exceptionCaught = false;
        final String DNE_BUCKET = "DNE";
        final Visibility visibility = new Visibility().setFormalVisibility("TS");
        final EzSecurityToken token = TestUtils.createTSUser();
        final Blob entry =
                new Blob(DNE_BUCKET, "TEST_KEY", ByteBuffer.wrap("SOME BLOB DATA".getBytes()), visibility);
        try {
            handler.putBlob(entry, token);
        } catch (TException e) {
            assertEquals("Bucket: " + DNE_BUCKET +
                    " doesn't exist!  You must create it first", e.getMessage().toString());
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
    }

    @Test
    public void testPutBlobThrowsExceptionWithoutWritePermission() throws Exception {
        boolean exceptionCaught = false;
        try {
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final PlatformObjectVisibilities pov = new PlatformObjectVisibilities();
            pov.setPlatformObjectWriteVisibility(Sets.newHashSet(0L));
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            advancedMarkings.setPlatformObjectVisibility(pov);
            visibility.setAdvancedMarkings(advancedMarkings);

            final Blob entry =
                    new Blob(TEST_BUCKET, "TEST_KEY", ByteBuffer.wrap("SOME BLOB DATA".getBytes()), visibility);
            final EzSecurityToken token = TestUtils.createTSUser();
            final Authorizations authorizations = new Authorizations();
            authorizations.setFormalAuthorizations(Sets.newHashSet("TS"));
            authorizations.setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            token.setAuthorizations(authorizations);

            handler.putBlob(entry, token);
        } catch (final BlobException e) {
            assertEquals("User does not have permission for WRITE on Blob", e.getMessage().toString());
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
    }

    @Test
    public void testCreateBucketThrowsExceptionWithoutWritePermission() throws Exception {
        try {
            final Visibility visibility = new Visibility().setFormalVisibility("TS");
            final PlatformObjectVisibilities pov = new PlatformObjectVisibilities();
            pov.setPlatformObjectWriteVisibility(Sets.newHashSet(0L));
            final AdvancedMarkings advancedMarkings = new AdvancedMarkings();
            advancedMarkings.setPlatformObjectVisibility(pov);
            visibility.setAdvancedMarkings(advancedMarkings);

            final EzSecurityToken token = TestUtils.createTSUser();
            final Authorizations authorizations = new Authorizations();
            authorizations.setFormalAuthorizations(Sets.newHashSet("TS"));
            authorizations.setPlatformObjectAuthorizations(Sets.newHashSet(1L));
            token.setAuthorizations(authorizations);

            handler.createBucket("BUCKET", visibility, token);
        } catch (final BlobException e) {
            assertEquals("User does not have permission for WRITE on Bucket", e.getMessage().toString());
        }
    }

    @Test
    public void testPurgeWorksWithBatchSizeAndMultipleCallsAsExpected() throws Exception {
        final Set<Long> purgeIds = Sets.newHashSet(1L, 2L, 3L, 4L, 5L);

        final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities()
                .setPlatformObjectReadVisibility(Sets.newHashSet(1L))
                .setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
        final AdvancedMarkings advancedMarkings = new AdvancedMarkings()
                .setPlatformObjectVisibility(platformObjectVisibilities)
                .setId(0);
        final Visibility visibility = new Visibility().setFormalVisibility("TS")
                .setAdvancedMarkings(advancedMarkings);
        final PurgeItems purgeItems = new PurgeItems()
                .setItems(purgeIds);
        final PurgeOptions purgeOptions = new PurgeOptions()
                .setBatchSize(2);
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));

        //This is creating 5 entries in accumulo, 1 bucket and 4 blobs
        handler.createBucket(TEST_BUCKET, visibility, USER_WITH_TS_S_B);
        for (int counter = 1; counter < 6; counter++) {
            visibility.getAdvancedMarkings().setId(counter);
            final Blob entry =
                    new Blob(TEST_BUCKET, "TEST_KEY" + counter,
                            ByteBuffer.wrap("SOME BLOB DATA".getBytes()), visibility);
            handler.putBlob(entry, USER_WITH_TS_S_B);
        }

        //Check we can purge against objects we don't have permission to read/write
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(0L));

        //Two down, job isn't finished yet
        PurgeResult purgeResult = handler.purge(purgeItems, purgeOptions, USER_WITH_TS_S_B);
        assertFalse(purgeResult.isIsFinished());
        assertEquals("Batch size set to 2, should have removed 2 items", Sets.newHashSet(1L), purgeResult.getPurged());

        purgeResult = handler.purge(purgeItems, purgeOptions, USER_WITH_TS_S_B);
        assertFalse(purgeResult.isIsFinished());
        assertEquals("Batch size set to 2, should have removed 2 items", Sets.newHashSet(2L, 3L), purgeResult.getPurged());

        purgeResult = handler.purge(purgeItems, purgeOptions, USER_WITH_TS_S_B);
        assertTrue(purgeResult.isIsFinished());
        assertEquals("Batch size set to 2, should have removed 1 items", Sets.newHashSet(4L, 5L), purgeResult.getPurged());

    }

    @Test
    public void testPurge() throws Exception {
        final Set<Long> purgeIds = Sets.newHashSet(1L, 2L);
        final Set<Long> dontPurgeIds = Sets.newHashSet(3L, 42L);
        final PlatformObjectVisibilities platformObjectVisibilities = new PlatformObjectVisibilities()
                .setPlatformObjectReadVisibility(Sets.newHashSet(1L))
                .setPlatformObjectWriteVisibility(Sets.newHashSet(1L));
        final AdvancedMarkings advancedMarkings = new AdvancedMarkings()
                .setPlatformObjectVisibility(platformObjectVisibilities)
                .setId(0);
        final Visibility visibility = new Visibility().setFormalVisibility("TS")
                .setAdvancedMarkings(advancedMarkings);
        final PurgeItems purgeItems = new PurgeItems()
                .setItems(Sets.union(purgeIds, dontPurgeIds));
        final PurgeOptions purgeOptions = new PurgeOptions();

        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));

        handler.createBucket(TEST_BUCKET, visibility, USER_WITH_TS_S_B);

        //Make blobs with one of them (3) being marked as composite
        for (int counter = 0; counter < 4; counter++) {
            advancedMarkings.setId(counter);
            advancedMarkings.setComposite(counter == 3);
            visibility.setAdvancedMarkings(advancedMarkings);
            final Blob entry =
                    new Blob(TEST_BUCKET, "TEST_KEY" + counter,
                            ByteBuffer.wrap("SOME BLOB DATA".getBytes()), visibility);
            handler.putBlob(entry, USER_WITH_TS_S_B);
        }

        //Check we can purge against objects we don't have permission to read/write
        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(0L));

        //Call purge and attempt to delete ids 1,2,3,42, only 1,2 should purge properly
        final PurgeResult purgeResult = handler.purge(purgeItems, purgeOptions, USER_WITH_TS_S_B);

        //Make sure two ids got purged (1,2)
        assertEquals(Sets.newHashSet(1L, 2L), purgeResult.getPurged());

        //Make sure one id remained unPurged (3)
        assertEquals(Sets.newHashSet(3L), purgeResult.getUnpurged());
        assertTrue(purgeResult.getUnpurged().contains(3L));


        USER_WITH_TS_S_B.getAuthorizations().setPlatformObjectAuthorizations(Sets.newHashSet(1L));

        //Make sure keys are still there that shouldn't have been purged
        assertEquals(1, handler.getBlobs(TEST_BUCKET, "TEST_KEY0", USER_WITH_TS_S_B).size());
        assertEquals(1, handler.getBlobs(TEST_BUCKET, "TEST_KEY3", USER_WITH_TS_S_B).size());

        //Make sure keys that should have been purged got purged
        assertEquals(0, handler.getBlobs(TEST_BUCKET, "TEST_KEY1", USER_WITH_TS_S_B).size());
        assertEquals(0, handler.getBlobs(TEST_BUCKET, "TEST_KEY2", USER_WITH_TS_S_B).size());
    }

    @Test
    public void testGetBlobsThrowsExceptionWhenBucketDoesntExist() throws TException {
        boolean exceptionCaught = false;
        try {
            handler.getBlobs("DNE", "DNE", USER_WITH_TS_S_B);
        } catch (BlobException e) {
            exceptionCaught = true;
            assertEquals("Bucket: DNE doesn't exist!  You must create it first", e.getMessage());
        }
        assertTrue(exceptionCaught);
    }

    @Test
    public void testGetBlobVisibilityThrowsExceptionWhenBucketDoesntExist() throws TException {
        boolean exceptionCaught = false;
        try {
            handler.getBlobVisibility("DNE", "DNE", USER_WITH_TS_S_B);
        } catch (BlobException e) {
            exceptionCaught = true;
            assertEquals("Bucket: DNE doesn't exist!  You must create it first", e.getMessage());
        }
        assertTrue(exceptionCaught);
    }

    @Test
    public void testGetBucketVisibilityThrowsExceptionWhenBucketDoesntExist() throws TException {
        boolean exceptionCaught = false;
        try {
            handler.getBucketVisibility("DNE", USER_WITH_TS_S_B);
        } catch (BlobException e) {
            exceptionCaught = true;
            assertEquals("Bucket: DNE doesn't exist!  You must create it first", e.getMessage());
        }
        assertTrue(exceptionCaught);
    }
}
