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

import static ezbake.data.common.TokenUtils.validateSecurityToken;
import static ezbake.data.common.classification.ClassificationUtils.extractUserAuths;
import static ezbake.data.common.classification.ClassificationUtils.getAuthsFromString;
import static ezbake.security.serialize.VisibilitySerialization.deserializeVisibilityWrappedBytes;
import static ezbake.security.serialize.VisibilitySerialization.deserializeVisibilityWrappedValue;
import static ezbake.security.serialize.VisibilitySerialization.serializeVisibilityWithData;
import static ezbake.security.serialize.VisibilitySerialization.serializeVisibilityWithDataToValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.data.base.EzbakeBaseDataService;
import ezbake.data.base.blob.thrift.Blob;
import ezbake.data.base.blob.thrift.BlobException;
import ezbake.data.base.blob.thrift.EzBlob;
import ezbake.data.base.thrift.PurgeItems;
import ezbake.data.base.thrift.PurgeOptions;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.data.iterator.EzBakeVisibilityFilter;
import ezbake.security.permissions.PermissionUtils;
import ezbake.security.serialize.thrift.VisibilityWrapper;
import ezbake.util.AuditEvent;
import ezbake.util.AuditEventType;

import ezbakehelpers.accumulo.AccumuloHelper;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

/**
 * A simple blob store dataset which is backed by Accumulo. The Accumulo layout for the store looks like:
 * <p/>
 * RowId Column Family Column Qualifier Security Value bucket key "" user_security blob
 * <p/>
 * This class then has some basic CRUD operations to work with the blob store.
 */
public class EzBlobHandler extends EzbakeBaseDataService implements EzBlob.Iface {
    private static final int MAX_BLOB_SIZE_MB = 128;
    private static final int MAX_BLOB_SIZE_BYTES = MAX_BLOB_SIZE_MB * 1000 * 1000;

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]).asReadOnlyBuffer();
    private static final Text EMPTY_TEXT = new Text("");

    // For EzMetrics
    private static final String BLOB_GET_TIMER_NAME = MetricRegistry.name(EzBlobHandler.class, "blob", "timer", "get");

    private static final String BLOB_PUT_TIMER_NAME = MetricRegistry.name(EzBlobHandler.class, "blob", "timer", "put");

    private static final String BLOB_PUT_METER_NAME = MetricRegistry.name(EzBlobHandler.class, "blob", "meter", "put");

    private static final String BLOB_PUT_HISTOGRAM_NAME =
            MetricRegistry.name(EzBlobHandler.class, "blobsize", "histogram", "put");

    // Size in bytes of the maximum memory to batch before writing
    private static final long MAX_MEMORY = 2560000;

    // Maximum latency time (for batch writer) in milliseconds; set to 0 or Long.MAX_VALUE to allow the maximum time
    // to hold a batch before writing
    private static final long MAX_LATENCY = 300;

    // The maximum number of threads to use for writing data to the tablet servers
    private static final int MAX_WRITE_THREADS = 4;
    private static final BatchWriterConfig BATCH_WRITER_CONFIG =
            new BatchWriterConfig().setMaxLatency(MAX_LATENCY, TimeUnit.MILLISECONDS).setMaxMemory(MAX_MEMORY)
                    .setMaxWriteThreads(MAX_WRITE_THREADS);
    private static final Logger logger = LoggerFactory.getLogger(EzBlobHandler.class);
    private final IteratorSetting iteratorSetting =
            new IteratorSetting(42, "ezBlobIterator", EzBakeVisibilityFilter.class);
    private Connector connector;
    private String tableName;
    private String purgeTableName;

    @Override
    public TProcessor getThriftProcessor() {
        try {
            init();
            return new EzBlob.Processor<EzBlob.Iface>(this);
        } catch (final TException e) {
            logger.error("Could not create Thrift processor", e);
            return null;
        }
    }

    @Override
    public boolean ping() {
        // TOOD (soup): Replace this with a better way to check pings
        return true;
    }

    /**
     * Puts a blob into the datastore
     *
     * @param entry a blob store entry to put into the data store consists of a bucket, key, blob, and visibility
     * @param security the security label of the user looking up the blob
     * @throws TException in case of an unexpected error
     * @throws ezbake.data.base.blob.thrift.BlobException in case of an Accumulo Exception
     */
    @Override
    public void putBlob(Blob entry, EzSecurityToken security) throws TException {
        if (entry == null) {
            // No-op, since we don't want to store 'null' and we don't want to throw an exception.
            return;
        }

        putBlob(entry, security, true);
    }

    /**
     * Gets a blob from the data store using the bucket and key.
     *
     * @param bucket that is being looked up
     * @param key that will be used for the look up
     * @param security of the the user who is doing the look up
     * @return a ByteBuffer containing the blob of the user
     * @throws TException in case of an unexpected error
     * @throws ezbake.data.base.blob.thrift.BlobException in case of an Accumulo Exception
     */
    @Override
    public Set<ByteBuffer> getBlobs(String bucket, String key, EzSecurityToken security) throws TException {
        final Timer.Context context = getMetricRegistry().getTimers().get(EzBlobHandler.BLOB_GET_TIMER_NAME).time();

        checkBucketExists(bucket, security);

        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucket));
            scanner.fetchColumnFamily(new Text(key));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ));

            final Iterator<Entry<Key, Value>> scanIterator = scanner.iterator();
            final Set<ByteBuffer> scanResult = new HashSet<>();
            while (scanIterator.hasNext()) {
                // Non-empty result, return an empty set
                final Entry<Key, Value> entry = scanIterator.next();
                if (entry == null) {
                    // Since nothing was found return an empty ByteBuffer
                    scanResult.add(EMPTY_BYTE_BUFFER);
                } else {
                    final VisibilityWrapper deserialized = deserializeVisibilityWrappedValue(entry.getValue());
                    scanResult.add(ByteBuffer.wrap(deserialized.getValue()));
                }
            }

            return scanResult;
        } catch (final TableNotFoundException e) {
            logger.error("No table named '" + tableName + "' found", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not read visibility", e);
            throw new BlobException(e.getMessage());
        } finally {
            context.stop();
        }
    }

    /**
     * Removes a blob from the datastore
     *
     * @param bucket that is being looked up
     * @param key that will be used for the look up
     * @param security of the the user who is doing the look up
     * @return The number of affected rows upon delete.
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public int removeBlob(String bucket, String key, EzSecurityToken security) throws TException {
        return removeBlob(bucket, key, security, false);
    }

    /**
     * Copies a blob from the datastore
     *
     * @param sourceBucketName bucket to copy blob from
     * @param sourceKey key to copy blob from
     * @param destinationBucketName bucket to copy blob to
     * @param destinationKey key to copy blob to
     * @param security token of the user performing the operation
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public void copyBlob(
            String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey,
            EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(sourceBucketName));
            scanner.fetchColumnFamily(new Text(sourceKey));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ, Permission.WRITE));

            for (final Entry<Key, Value> entry : scanner) {
                final Visibility viz = deserializeVisibilityWrappedValue(entry.getValue()).getVisibilityMarkings();
                final Blob blob =
                        new Blob(destinationBucketName, destinationKey, ByteBuffer.wrap(entry.getValue().get()), viz);
                putBlob(blob, security, false);
            }

            // audit log
            final String description = String.format(
                    "source bucket: %s  sourceKey: %s   destination bucket: %s  destination key: %s", sourceBucketName,
                    sourceKey, destinationBucketName, destinationKey);

            auditLog(security, AuditEventType.FileObjectAccess, "copyBlob", description);
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not read visibility", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Create a bucket in the datastore
     *
     * @param bucketName name of the bucket
     * @param visibility visibility on the bucket
     * @param security token of the user performing the operation
     * @return String of the bucket created
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public String createBucket(String bucketName, Visibility visibility, EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            if (!hasPermissions(security.getAuthorizations(), visibility, EnumSet.of(Permission.WRITE))) {
                throw new BlobException("User does not have permission for WRITE on Bucket");
            }

            final Mutation bucketMutation = new Mutation(bucketName);

            bucketMutation.put(
                    EMPTY_TEXT, new Text("{bucketVisibility:" + visibility.getFormalVisibility() + "}"),
                    new ColumnVisibility(visibility.getFormalVisibility()),
                    serializeVisibilityWithDataToValue(visibility, new byte[0]));

            final BatchWriter writer = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);
            writer.addMutation(bucketMutation);
            writer.flush();
            writer.close();

            // audit log
            final String description = String.format("bucket: %s", bucketName);
            auditLog(security, AuditEventType.FileObjectCreate, "createBucket", description);

            return bucketName;
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not write visibility", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Delete bucket from datastore
     *
     * @param bucketName name of bucket to delete
     * @param security token of the user performing the operation
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public void deleteBucket(String bucketName, EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final BatchDeleter deleter = connector.createBatchDeleter(
                    tableName, getAuthsFromString(extractUserAuths(security)), 1, BATCH_WRITER_CONFIG);

            addEzBakeVisibilityFilter(deleter, security, EnumSet.of(Permission.WRITE));

            final List<Range> b = new ArrayList<>();
            b.add(new Range(bucketName));

            deleter.setRanges(b);
            deleter.delete();
            deleter.close();

            // audit log
            final String description = String.format("bucket: %s", bucketName);
            auditLog(security, AuditEventType.FileObjectDelete, "deleteBucket", description);
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Checks if bucket exists in datastore
     *
     * @param bucketName bucket to check in datastore
     * @param security token of the user performing the operation
     * @return boolean for if token exists in datastore and is visibile to the user performing the operation
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @SuppressWarnings("unused")
    @Override
    public boolean doesBucketExist(String bucketName, EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucketName));
            scanner.fetchColumnFamily(new Text(""));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.DISCOVER));

            int affectedRows = 0;
            for (final Entry<Key, Value> entry : scanner) {
                affectedRows++;
            }

            return affectedRows > 0;
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Checks if blob exists in datastore
     *
     * @param bucketName bucket to check in datastore
     * @param key key to check in datastore
     * @param security token of the user performing the operation
     * @return boolean for if token exists in datastore and is visible to the user performing the operation
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @SuppressWarnings("unused")
    @Override
    public boolean doesBlobExist(String bucketName, String key, EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucketName));
            scanner.fetchColumnFamily(new Text(key));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.DISCOVER));

            final BatchWriter writer = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);

            int affectedRows = 0;
            for (final Entry<Key, Value> entry : scanner) {
                affectedRows++;
            }

            writer.flush();
            writer.close();

            return affectedRows > 0;
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Get visibility of blob(s) in the datastore
     *
     * @param bucketName bucket of the blob
     * @param key key of the blob
     * @param security token of the user performing the operation
     * @return List of visibilities on the blob(s)
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public List<Visibility> getBlobVisibility(String bucketName, String key, EzSecurityToken security)
            throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            checkBucketExists(bucketName, security);

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucketName));
            scanner.fetchColumnFamily(new Text(key));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.MANAGE_VISIBILITY));

            final BatchWriter writer = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);

            final List<Visibility> visibilities = new ArrayList<>();
            for (final Entry<Key, Value> entry : scanner) {
                visibilities.add(deserializeVisibilityWrappedValue(entry.getValue()).getVisibilityMarkings());
            }

            writer.flush();
            writer.close();

            return visibilities;
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not read visibility", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Get visibility of bucket in the datastore
     *
     * @param bucketName name of the bucket
     * @param security token of the user performing the operation
     * @return visibility of the bucket
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public Visibility getBucketVisibility(String bucketName, EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            checkBucketExists(bucketName, security);

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucketName));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.MANAGE_VISIBILITY));

            final BatchWriter writer = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);

            Visibility visibility = null;
            for (final Entry<Key, Value> entry : scanner) {
                if (visibility == null) {
                    visibility = deserializeVisibilityWrappedValue(entry.getValue()).getVisibilityMarkings();
                }
            }

            writer.flush();
            writer.close();

            if (visibility == null) {
                throw new BlobException("Could not find visibility");
            }

            return visibility;
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not read visibility", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Lists out the buckets int he datastore
     *
     * @param security token of the user performing the operation
     * @return list of bucket names
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public Set<String> listBuckets(EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range());
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ));

            final Set<String> list = new HashSet<>();
            for (final Entry<Key, Value> entry : scanner) {
                list.add(entry.getKey().getRow().toString());
            }

            return list;
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * List blobs in the datastore for a passed in bucket
     *
     * @param bucketName bucket to get blobs from
     * @param security token of the user performing the operation
     * @return list of blobs in the matching bucket
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public List<Blob> listBlobs(String bucketName, EzSecurityToken security) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucketName));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.READ));

            final BatchWriter deleteWriter = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);

            final List<Blob> list = new ArrayList<>();
            for (final Entry<Key, Value> entry : scanner) {
                final JSONObject json = new JSONObject(entry.getKey().getColumnQualifier().toString());
                if (json.has("blobVisibility")) {
                    final VisibilityWrapper visibilityWrapper = deserializeVisibilityWrappedValue(entry.getValue());
                    final Blob blob = new Blob(
                            bucketName, entry.getKey().getColumnFamily().toString(),
                            ByteBuffer.wrap(visibilityWrapper.getValue()), visibilityWrapper.getVisibilityMarkings());
                    list.add(blob);
                }
            }
            deleteWriter.flush();
            deleteWriter.close();

            return list;
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final JSONException e) {
            logger.error("Error converting json", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not read visibility", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Set the visibility on a bucket
     *
     * @param bucketName name of the bucket
     * @param visibility
     * @param security
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public void setBucketVisibility(String bucketName, Visibility visibility, EzSecurityToken security)
            throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucketName));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.MANAGE_VISIBILITY));

            final String bucketViz = visibility.getFormalVisibility();
            final BatchWriter writer = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);
            for (final Entry<Key, Value> entry : scanner) {
                final JSONObject json = new JSONObject(entry.getKey().getColumnQualifier().toString());
                String blobViz;
                ColumnVisibility newViz;
                if (json.has("blobVisibility")) {
                    blobViz = json.getString("blobVisibility");
                    newViz = new ColumnVisibility("(" + bucketViz + ")&(" + blobViz + ")");
                } else {
                    newViz = new ColumnVisibility(bucketViz);
                }

                final Value newValue = serializeVisibilityWithDataToValue(
                        visibility, deserializeVisibilityWrappedBytes(entry.getValue().get()).getValue());

                final JSONObject newJson = json.put("bucketVisibility", visibility.getFormalVisibility());
                final Mutation blobMutation = new Mutation(bucketName);

                blobMutation.put(
                        new Text(entry.getKey().getColumnFamily()), new Text(newJson.toString()), newViz, newValue);

                blobMutation.putDelete(
                        entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(),
                        entry.getKey().getColumnVisibilityParsed());

                writer.addMutation(blobMutation);
            }
            writer.flush();
            writer.close();

            // audit log
            final String description = String.format("bucket: %s", bucketName);
            auditLog(security, AuditEventType.FileObjectPermissionModifications, "setBucketVisibility", description);
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final JSONException e) {
            logger.error("Error converting json", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not read/write visibility", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Set the visibility on a blob
     *
     * @param bucketName bucket of the blob
     * @param key key of the blob
     * @param visibility visibility to set on the blob
     * @param security token of the user performing the operation
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    @Override
    public void setBlobVisibility(String bucketName, String key, Visibility visibility, EzSecurityToken security)
            throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucketName));
            scanner.fetchColumnFamily(new Text(key));
            addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.MANAGE_VISIBILITY));

            final String blobViz = visibility.getFormalVisibility();
            final BatchWriter writer = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);
            for (final Entry<Key, Value> entry : scanner) {
                final JSONObject json = new JSONObject(entry.getKey().getColumnQualifier().toString());
                final String bucketViz = json.getString("bucketVisibility");
                final ColumnVisibility newViz = new ColumnVisibility(bucketViz + "&" + blobViz);
                final JSONObject newJson = json.put("blobVisibility", visibility.getFormalVisibility());

                final Value newValue = serializeVisibilityWithDataToValue(
                        visibility, deserializeVisibilityWrappedBytes(entry.getValue().get()).getValue());

                final Mutation blobMutation = new Mutation(bucketName);

                blobMutation.put(
                        new Text(entry.getKey().getColumnFamily()), new Text(newJson.toString()), newViz, newValue);

                blobMutation.putDelete(
                        entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier(),
                        entry.getKey().getColumnVisibilityParsed());

                writer.addMutation(blobMutation);
            }

            writer.flush();
            writer.close();

            // audit log
            final String description = String.format("bucket: %s    key: %s", bucketName, key);
            auditLog(security, AuditEventType.FileObjectPermissionModifications, "setBlobVisibility", description);
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final JSONException e) {
            logger.error("Error converting json", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not read/write visibility", e);
            throw new BlobException(e.getMessage());
        }
    }

    /**
     * Purges the provenance ids passed in via object with the PurgeOptions.  This method should be called with the
     * application's security token.  The formal visibilities in the application's security token should be a superset
     * of all of the formal visibilities of tokens used to persist data via EzBlob.  On each call, the current row of
     * the purgeJobId is persisted to the purge table, and the purge resumes from that row.
     *
     * @param items PurgeItems including which purgeIds to purge.
     * @param options PurgeOptions including batchSize, not used.
     * @param security Application's security token.
     * @throws TException in case of an unexpected error.
     */
    @Override
    public PurgeResult purge(PurgeItems items, PurgeOptions options, EzSecurityToken security) throws TException {
        final PurgeResult purgeResult =
                new PurgeResult(true).setPurged(new HashSet<Long>()).setUnpurged(new HashSet<Long>());
        final Set<Long> toPurge = new HashSet<>(items.getItems());
        final int backendBatchSize = options.getBatchSize();
        final Scanner scanner;
        final Iterator<Entry<Key, Value>> iterator;
        final long purgeJobId = items.getPurgeId();
        final Authorizations auths = getAuthsFromString(extractUserAuths(security));

        int backendBatchCounter = 0;

        validateSecurityToken(security, this.getConfigurationProperties());
        auditLog(
                security, AuditEventType.FileObjectDelete, "purge",
                String.format("Purging items: %s with options %s", items, options.toString()));
        try {
            scanner = connector.createScanner(tableName, auths);
            setupPurgeScanner(purgeJobId, scanner);
            iterator = scanner.iterator();

            while (iterator.hasNext() && (backendBatchSize == 0 || backendBatchCounter < backendBatchSize)) {
                backendBatchCounter++;
                processRow(security, purgeResult, toPurge, iterator.next());
            }
            finalizePurgeResult(purgeResult, iterator, purgeJobId);
        } catch (TableNotFoundException | IOException | MutationsRejectedException e) {
            logger.error(e.getMessage());
            throw new BlobException(e.getMessage());
        }

        return purgeResult;
    }

    private void checkBucketExists(String bucket, EzSecurityToken security) throws TException {
        if (!doesBucketExist(bucket, security)) {
            throw new BlobException("Bucket: " + bucket + " doesn't exist!  You must create it first");
        }
    }

    /**
     * Removes a blob from the datastore
     *
     * @param bucket that is being looked up
     * @param key that will be used for the look up
     * @param security of the the user who is doing the look up
     * @param isPurge flag for adding visibility filter
     * @return The number of affected rows upon delete.
     * @throws TException in case of an unexpected error
     * @throws BlobException in case of an Accumulo Exception
     */
    private int removeBlob(String bucket, String key, EzSecurityToken security, boolean isPurge) throws TException {
        try {
            validateSecurityToken(security, this.getConfigurationProperties());

            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(extractUserAuths(security)));

            scanner.setRange(new Range(bucket));
            scanner.fetchColumnFamily(new Text(key));
            if (!isPurge) {
                addEzBakeVisibilityFilter(scanner, security, EnumSet.of(Permission.WRITE));
            }

            final BatchWriter deleteWriter = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);

            int affectedRows = 0;
            for (final Entry<Key, Value> entry : scanner) {
                final ColumnVisibility viz = entry.getKey().getColumnVisibilityParsed();
                final Text qual = entry.getKey().getColumnQualifier();
                final Mutation deleteMutation = new Mutation(bucket);
                System.out.println("Deleting.... Key: " + key + " qual: " + qual + " vis:" + viz);
                deleteMutation.putDelete(new Text(key), qual, viz);
                deleteWriter.addMutation(deleteMutation);

                affectedRows++;
            }

            deleteWriter.flush();
            deleteWriter.close();

            // audit log
            final String action = String.format("bucket: %s   key: %s", bucket, key);
            auditLog(security, AuditEventType.FileObjectDelete, "removeBlob", action);

            return affectedRows;
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        }
    }

    private void init() throws TException {
        final Properties props = getConfigurationProperties();
        try {
            final String properties = getConfigurationProperties().toString();
            logger.debug("Initializing BlobStoreServiceHandler with EZConfiguration properties: \n" + properties);
            connector = new AccumuloHelper(props).getConnector(true);
        } catch (final IOException e) {
            logger.error("Exception getting connector form AccumuloConfiguration", e);
        }

        final String appName = new EzBakeApplicationConfigurationHelper(props).getApplicationName();
        tableName = String.format("%s_blobstore", appName);
        purgeTableName = String.format("%s_purgeTable", appName);
        logger.info("Ensuring blob store table {} for application {}", tableName, appName);
        ensureTable(Lists.newArrayList(tableName, purgeTableName));

        initMetrics();

        initAuditLogger(EzBlobHandler.class);
    }

    /**
     * Set up the MetricRegistry object for EzMetrics
     */
    private void initMetrics() {
        final MetricRegistry mr = getMetricRegistry();

        mr.timer(EzBlobHandler.BLOB_GET_TIMER_NAME);
        mr.timer(EzBlobHandler.BLOB_PUT_TIMER_NAME);
        mr.meter(EzBlobHandler.BLOB_PUT_METER_NAME);
        mr.histogram(EzBlobHandler.BLOB_PUT_HISTOGRAM_NAME);
    }

    private boolean hasPermissions(
            ezbake.base.thrift.Authorizations authorizations, Visibility visibility, Set<Permission> Permissions)
            throws TException {
        final Set<Permission> userPerms =
                PermissionUtils.getPermissions(authorizations, visibility, false, Permissions);

        return !userPerms.isEmpty();
    }

    private void ensureTable(List<String> tables) {
        for (String tableName : tables) {
            if (!connector.tableOperations().exists(tableName)) {
                try {
                    connector.tableOperations().create(tableName);
                } catch (final TableExistsException e) {
                    logger.error("Table already exists", e);
                    throw new RuntimeException(e);
                } catch (final AccumuloException e) {
                    logger.error("Accumulo Exception", e);
                    throw new RuntimeException(e);
                } catch (final AccumuloSecurityException e) {
                    logger.error("Accumulo Security Exception", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void addEzBakeVisibilityFilter(ScannerBase scanner, EzSecurityToken token, Set<Permission> permissions)
            throws TException {
        iteratorSetting.clearOptions();
        EzBakeVisibilityFilter.setOptions(iteratorSetting, token.getAuthorizations(), permissions);
        scanner.addScanIterator(iteratorSetting);
    }

    private void auditLog(EzSecurityToken userToken, AuditEventType eventType, String action, String description) {
        final AuditEvent auditEvent =
                new AuditEvent(eventType, userToken).arg("action", action).arg("description", description);

        auditLogger.logEvent(auditEvent);
    }

    /**
     * Puts a blob into the datastore
     *
     * @param entry a blob store entry to put into the data store consists of a bucket, key, blob, and visibility
     * @param security the security label of the user looking up the blob
     * @param useVisibility whether or not to serialize token's bitvector into value field, this is skipped for the
     * copyBlob method
     * @throws TException in case of an unexpected error
     * @throws ezbake.data.base.blob.thrift.BlobException in case of an Accumulo Exception or Permission Exception
     */
    private void putBlob(Blob entry, EzSecurityToken security, boolean useVisibility) throws TException {
        if (entry.visibility == null || StringUtils.isEmpty(entry.visibility.getFormalVisibility())) {
            throw new BlobException("Visibility was not provided on putBlob!");
        }

        if (useVisibility && !hasPermissions(
                security.getAuthorizations(), entry.getVisibility(), EnumSet.of(Permission.WRITE))) {
            throw new BlobException("User does not have permission for WRITE on Blob");
        }

        // EzMetrics: keep track of how often and how long
        getMetricRegistry().getMeters().get(EzBlobHandler.BLOB_PUT_METER_NAME).mark();
        final Timer.Context context = getMetricRegistry().getTimers().get(EzBlobHandler.BLOB_PUT_TIMER_NAME).time();

        validateSecurityToken(security, this.getConfigurationProperties());
        try {
            final byte[] entryBlob = entry.getBlob();

            final byte[] blob =
                    useVisibility ? serializeVisibilityWithData(entry.getVisibility(), entryBlob) : entryBlob;

            final Visibility visibility = entry.visibility;

            // Throw an Exception if the blob is larger than the maximum
            if (blob.length > MAX_BLOB_SIZE_BYTES) {
                throw new BlobException(
                        String.format(
                                "The blob you're trying to store is too big! Please only store BLOB data less than or"
                                        + " equal to %d MB", MAX_BLOB_SIZE_MB));
            }

            final BatchWriter writer = connector.createBatchWriter(tableName, BATCH_WRITER_CONFIG);

            checkBucketExists(entry.getBucket(), security);

            final String bucketVis = getBucketVisibility(entry.getBucket(), security).getFormalVisibility();

            final String booleanExpressionString = "(" + bucketVis + ")&(" + visibility.getFormalVisibility() + ")";

            logger.debug("in putBlob, booleanExpressionString: " + booleanExpressionString);
            final Mutation m = new Mutation(entry.getBucket());

            m.put(
                    new Text(entry.getKey()), new Text(
                            "{bucketVisibility:" + entry.visibility.getFormalVisibility() + ", blobVisibility:"
                                    + visibility.getFormalVisibility() + "}"),
                    new ColumnVisibility(booleanExpressionString), new Value(blob));

            writer.addMutation(m);
            writer.flush();
            writer.close();

            // keep track of how big the blobs are that we are storing for metrics
            getMetricRegistry().getHistograms().get(EzBlobHandler.BLOB_PUT_HISTOGRAM_NAME)
                    .update(entry.getBlob().length);

            // audit log
            final String description = String.format("key: %s size: %s", entry.getKey(), entry.getBlob().length);

            auditLog(security, AuditEventType.FileObjectCreate, "putBlob", description);
        } catch (final MutationsRejectedException e) {
            logger.error("Accumulo Exception", e);
            throw new BlobException(e.getMessage());
        } catch (final TableNotFoundException e) {
            logger.error("No table named " + tableName + " found", e);
            throw new BlobException(e.getMessage());
        } catch (final IOException e) {
            logger.error("Could not write visibility", e);
            throw new BlobException(e.getMessage());
        } finally {
            context.stop(); // EzMetrics
        }
    }

    /**
     * Save the current bucket / key of the purge job that is currently running in the purge table as a comma delimited
     * string from the entry passed in.
     *
     * @param purgeJobId id of the current purgeJob to lookup.
     * @param entry Entry containing bucket / key to persist for next purge scan.
     * @throws TableNotFoundException if purge table isn't found.
     * @throws MutationsRejectedException if mutations aren't persisted.
     */
    private void persistPurgeStatus(long purgeJobId, Entry<Key, Value> entry)
            throws TableNotFoundException, MutationsRejectedException {
        final BatchWriter writer = connector.createBatchWriter(purgeTableName, BATCH_WRITER_CONFIG);
        final BatchDeleter deleter =
                connector.createBatchDeleter(tableName, new Authorizations(), 1, BATCH_WRITER_CONFIG);
        deleter.setRanges(Lists.newArrayList(new Range(String.valueOf(purgeJobId))));
        deleter.delete();
        deleter.close();

        final Mutation purgeStatus = new Mutation(String.valueOf(purgeJobId));
        purgeStatus.put(
                new Text(), new Text(), new Value(
                        (String.format(
                                "%s,%s", entry.getKey().getRow().toString(),
                                entry.getKey().getColumnFamily().toString())).getBytes()));
        writer.addMutation(purgeStatus);
        writer.flush();
        writer.close();
    }

    /**
     * If an entry exists for purgeId, set the start of the scanner to the persisted bucket / key.
     *
     * @param purgeJobId id of the current purgeJob to lookup.
     * @param scanner scanner to update with new range corresponding to the persisted bucket / key.
     * @throws TableNotFoundException in case of an error with the purge table.
     */
    private void setupPurgeScanner(long purgeJobId, Scanner scanner) throws TableNotFoundException {
        final Scanner purgeScanner = connector.createScanner(purgeTableName, new Authorizations());
        purgeScanner.setRange(new Range(String.valueOf(purgeJobId)));

        if (purgeScanner.iterator().hasNext()) {
            final Entry<Key, Value> entry = purgeScanner.iterator().next();
            final String[] scannerArgs = entry.getValue().toString().split(",");
            final Key startKey = new Key(scannerArgs[0], scannerArgs[1]);
            scanner.setRange(new Range(startKey, null));
        }
    }

    /**
     * If purge is finished set isFinished to true, else persist current entry of purge for next call.
     *
     * @param purgeResult purgeResult object to return.
     * @param iterator iterator to check if purge is finished and fetch current entry.
     * @param purgeJobId purge job currently running.
     * @throws TableNotFoundException in case of unexpected error.
     * @throws MutationsRejectedException in case of unexpected error.
     */
    private void finalizePurgeResult(PurgeResult purgeResult, Iterator<Entry<Key, Value>> iterator, long purgeJobId)
            throws TableNotFoundException, MutationsRejectedException {
        if (iterator.hasNext()) {
            purgeResult.setIsFinished(false);
            final Entry<Key, Value> entry = iterator.next();
            persistPurgeStatus(purgeJobId, entry);
        } else {
            purgeResult.setIsFinished(true);
        }
    }

    /**
     * If the current row is part of the list of provenanceIds to purge, delete it from the store and add it to the
     * purged list.  If the current record is composite, add it to the unpurged list.
     *
     * @param security Token to potentially call remove blob with.
     * @param purgeResult purgeResult to add to.
     * @param provenanceIdsToPurge list of items to purge.
     * @param entry current entry being processed.
     * @throws TException in case of unexpectedError.
     * @throws IOException in case of unexpectedError.
     */
    private void processRow(
            EzSecurityToken security, PurgeResult purgeResult, Set<Long> provenanceIdsToPurge, Entry<Key, Value> entry)
            throws TException, IOException {
        final Visibility visibility = deserializeVisibilityWrappedValue(entry.getValue()).getVisibilityMarkings();
        final long entryProvenanceId = visibility.getAdvancedMarkings().getId();
        if (provenanceIdsToPurge.contains(entryProvenanceId)) {
            final String bucket = entry.getKey().getRow().toString();
            final String key = entry.getKey().getColumnFamily().toString();
            if (visibility.getAdvancedMarkings().isComposite()) {
                logger.info(String.format("Composite item cannot be purged, bucket: %s and key: %s", bucket, key));
                purgeResult.addToUnpurged(entryProvenanceId);
            } else {
                logger.info(String.format("Purging bucket: %s and key: %s", bucket, key));
                removeBlob(bucket, key, security, true);
                purgeResult.addToPurged(entryProvenanceId);
            }
            provenanceIdsToPurge.remove(entryProvenanceId);
        }
    }
}
