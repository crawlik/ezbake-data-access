package com.mongodb;

import com.mongodb.gridfs.GridFSDBFile;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.data.mongo.driver.thrift.*;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;
import org.bson.types.ObjectId;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by jagius on 6/4/14.
 */
public class EzDBCollectionImpl extends DBCollectionImpl {

    protected static ThriftClientPool pool;
    protected static EzbakeSecurityClient securityClient;
    EzConfiguration configuration = null;

    // this property is set in the pom.xml's surefire plugin for running unit tests.
    private static final String UNIT_TEST_MODE = "unitTestMode";

    void createClient() {
        try {
            configuration = new EzConfiguration();
            System.out.println("in EzDBCollectionImpl.createClient, configuration: " + configuration.getProperties());

            if (securityClient == null) {
                securityClient = new EzbakeSecurityClient(configuration.getProperties());
            }

            if (pool == null) {
                pool = new ThriftClientPool(configuration.getProperties());
            }

            System.out.println("pool: " + pool);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    protected EzMongoDriverService.Client getThriftClient() throws TException {
        return pool.getClient("ezmongo", EzMongoDriverService.Client.class);
    }

    public EzDBCollectionImpl(DBApiLayer db, String name) {
        super(db, name);
        createClient();
        System.out.println("EzDBCollectionImpl() constructor");
    }

    byte[] ser(Object o) throws IOException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        ObjectOutput output = new ObjectOutputStream(bOut);
        output.writeObject(o);
        return bOut.toByteArray();
    }

    EzSecurityToken getToken() throws Exception {
        String tokenSource = System.getProperty("tokenSource", "userInfo");
        if (tokenSource.equals("userInfo")){
            return securityClient.fetchTokenForProxiedUser();
        } else {
            return securityClient.fetchAppToken();
        }
    }

    public void findAndAddDBObjectToDBRefNodes(BasicDBObject bdo) {
        Set<String> keys = bdo.keySet();
        for (String key : keys) {
            Object subdoc = bdo.get(key);
            if (subdoc instanceof DBRef) {
                DBRef r = (DBRef)subdoc;
                r.set_db(db);
            }
        }
    }

    @Override
    public QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options, ReadPreference readPref, DBDecoder decoder) {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {

                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzFindRequest req = new EzFindRequest();

                if (ref instanceof BasicDBObject){
                    BasicDBObject b = (BasicDBObject)ref;
                    if (b.size() == 0) {
                        //throw new MongoException("PROBLEM WITH FIND ref");
                    }
                }

                req.setRef(ser(ref));
                req.setFields(ser(fields));
                req.setNumToSkip(numToSkip);
                req.setBatchSize(batchSize);
                req.setLimit(limit);
                req.setOptions(options);
                req.setReadPref(ser(readPref));
                req.setDecoder(ser(decoder));

                ResultsWrapper rw = client.find_driver(getName(),req, token);

                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(rw.getResponseData()));
                QueryResultIterator qri = (QueryResultIterator) ois.readObject();
                System.out.println("find1 QRI hashcode: " + qri.hashCode());

                ois = new ObjectInputStream(new ByteArrayInputStream(rw.getResultSet()));
                Object o = ois.readObject();

                if (o instanceof ArrayList) {
                    List<DBObject> results = (ArrayList<DBObject>) o;
                    for (DBObject d : results) {
                        if (d instanceof BasicDBObject) {
                            findAndAddDBObjectToDBRefNodes((BasicDBObject)d);
                        }
                    }
                    qri.setIterator(results.iterator());
                } else if (o instanceof GridFSDBFile) {
                    GridFSDBFile file = (GridFSDBFile)o;
                    List<DBObject> results = new ArrayList<DBObject>();
                    results.add(file);
                    qri.setIterator(results.iterator());
                }


                qri.setCollection(this);
                qri.set_host(new ServerAddress("localhost"));

                return qri;
            } catch (EzMongoDriverException e) {
                Object o = deser(e.getEx());
                if (o == null){
                    throw new MongoException("Unknown Exception");
                }
                if (o instanceof IllegalArgumentException){
                    throw (IllegalArgumentException)o;
                } else {
                    throw new MongoException(o.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            return super.find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder);
        }
    }

    @Override
    public QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options, ReadPreference readPref, DBDecoder decoder, DBEncoder encoder) {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzFindRequest req = new EzFindRequest();

                req.setRef(ser(ref));
                req.setFields(ser(fields));
                req.setNumToSkip(numToSkip);
                req.setBatchSize(batchSize);
                req.setLimit(limit);
                req.setOptions(options);
                req.setReadPref(ser(readPref));
                req.setDecoder(ser(decoder));
                req.setEncoder(ser(encoder));

                ResultsWrapper rw = client.find_driver(getName(),req, token);

                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(rw.getResponseData()));
                QueryResultIterator qri = (QueryResultIterator) ois.readObject();
                System.out.println("find2 QRI hashcode: " + qri.hashCode());


                ois = new ObjectInputStream(new ByteArrayInputStream(rw.getResultSet()));
                Object o = ois.readObject();

                List<DBObject> results = (ArrayList<DBObject>) o;

                qri.setIterator(results.iterator());
                qri.setCollection(this);
                qri.set_host(new ServerAddress("localhost"));

                return qri;

            } catch (EzMongoDriverException e) {
                Object o = deser(e.getEx());
                if (o == null){
                    throw new MongoException("Unknown Exception");
                }
                if (o instanceof IllegalArgumentException){
                    throw (IllegalArgumentException)o;
                } else {
                    throw new MongoException(o.toString());
                }
            }  catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            return super.find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder, encoder);
        }
    }

    @Override
    public Cursor aggregate(List<DBObject> pipeline, AggregationOptions options, ReadPreference readPreference) {
		if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzAggregationRequest req = new EzAggregationRequest();

                req.setPipeline(ser(pipeline));
                req.setOptions(ser(options));
                if (readPreference != null) {
                	req.setReadPref(ser(readPreference));
                }
                
                ResultsWrapper rw = client.aggregate_driver(getName(),req, token);

                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(rw.getResponseData()));
                Object cursorObj = ois.readObject();
                
                if (cursorObj instanceof QueryResultIterator) {
                	QueryResultIterator qri = (QueryResultIterator) cursorObj;
                	System.out.println("aggregate QRI hashcode: " + qri.hashCode());

                    ois = new ObjectInputStream(new ByteArrayInputStream(rw.getResultSet()));
                    Object o = ois.readObject();

                    List<DBObject> results = (ArrayList<DBObject>) o;

                    qri.setIterator(results.iterator());
                    qri.setCollection(this);
                    
                    return qri;
                } 
                
                // the aggregation pipeline had "$out" operator as the last command; 
                // see DBCollectionImpl.java's "aggregate" method
                if (cursorObj instanceof DBCursor) {

                    // Need to pull $out collection name so that future find() calls target the correct
                    // aggregation collection
                    DBCursor c = (DBCursor)cursorObj;
                    DBObject last = pipeline.get(pipeline.size() - 1);
                    String outCollection = (String) last.get("$out");
                    this._name = outCollection;
                    c.setCollection(this);
                    return c;
                }
                return (Cursor)cursorObj;
                
            } catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            return super.aggregate(pipeline, options, readPreference);
        }
    }

    @Override
    public List<Cursor> parallelScan(ParallelScanOptions options) {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzParallelScanOptions opts = new EzParallelScanOptions();
                opts.setOptions(ser(options));
                EzParallelScanResponse r = client.parallelScan_driver(getName(), opts, token);
                List<Cursor> list = (List<Cursor>) deser(r.getListOfCursors());

                Map<String, List<DBObject>> itMap = (Map<String, List<DBObject>>)deser(r.getMapOfIterators());

                List<Cursor> newList = new ArrayList<Cursor>();
                for (Cursor c : list){
                    QueryResultIterator qri = null;
                    if (c instanceof QueryResultIterator){
                        qri = (QueryResultIterator)c;
                        qri.setCollection(this);
                        qri.set_host(new ServerAddress("localhost"));

                        List<DBObject> results = (List<DBObject> )itMap.get(""+qri.getOriginalHashCode());
                        qri.setIterator(results.iterator());
                    }
                    newList.add(qri);
                }

                return list;
            } catch (EzMongoDriverException e) {
                Object o = deser(e.getEx());
                if (o == null){
                    throw new MongoException("Unknown Exception");
                }
                if (o instanceof IllegalArgumentException){
                    throw (IllegalArgumentException)o;
                } else if (o instanceof MongoException.DuplicateKey){
                    throw (MongoException.DuplicateKey)o;
                } else {
                    throw new MongoException(o.toString());
                }
            }  catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            return super.parallelScan(options);
        }
    }

    @Override
    BulkWriteResult executeBulkWriteOperation(boolean ordered, List<WriteRequest> writeRequests, WriteConcern writeConcern, DBEncoder encoder) {
        return super.executeBulkWriteOperation(ordered, writeRequests, writeConcern, encoder);
    }

    @Override
    public WriteResult insert(List<DBObject> list, WriteConcern concern, DBEncoder encoder) {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzInsertRequest req = new EzInsertRequest();

                for (DBObject obj : list){
                    Object _id = obj.get("_id");
                    if (_id == null) {
                        obj.put("_id", ObjectId.get());
                    }

                    if (!Boolean.getBoolean(UNIT_TEST_MODE)) {
                        // Check if the minimally required security fields exist:
                        // at least one of: _ezFV, _ezExtV, _ezObjRV
                        Object ezFV = obj.get("_ezFV");
                        Object ezExtV = obj.get("_ezExtV");
                        Object ezObjRV = obj.get("_ezObjRV");

                        if (ezFV == null && ezExtV == null && ezObjRV == null) {
                            throw new MongoException("At least one security field (_ezFV, _ezExtV, _ezObjRV) is required for inserts.");
                        }
                    }
                }

                ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(list);
                req.setDbObjectList(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(concern);
                req.setWriteConcern(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(encoder);
                req.setDbEncoder(bOut.toByteArray());

                req.setIsUnitTestMode(Boolean.getBoolean(UNIT_TEST_MODE));

                EzWriteResult res = client.insert_driver(this.getName(), req, token);

                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(res.getWriteResult()));
                WriteResult wr = (WriteResult)ois.readObject();

                return wr;

            } catch (EzMongoDriverException e) {
                Object o = deser(e.getEx());
                if (o == null){
                    throw new MongoException("Unknown Exception");
                }
                if (o instanceof IllegalArgumentException){
                    throw (IllegalArgumentException)o;
                } else if (o instanceof MongoException.DuplicateKey){
                    throw (MongoException.DuplicateKey)o;
                }  else if (o instanceof MongoInternalException){
                    throw (MongoInternalException)o;
                } else {
                    throw new MongoException(o.toString());
                }
            }  catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            return super.insert(list, concern, encoder);
        }
    }

    private Object deser(byte[] ex) {
        try {
            return new ObjectInputStream(new ByteArrayInputStream(ex)).readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected WriteResult insert(List<DBObject> list, boolean shouldApply, WriteConcern concern, DBEncoder encoder) {
        return super.insert(list, shouldApply, concern, encoder);
    }

    @Override
    public WriteResult remove(DBObject query, WriteConcern concern, DBEncoder encoder) {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzRemoveRequest req = new EzRemoveRequest();

                ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(query);
                req.setDbObjectQuery(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(concern);
                req.setWriteConcern(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(encoder);
                req.setDbEncoder(bOut.toByteArray());

                EzWriteResult res = client.remove_driver(this.getName(), req, token);

                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(res.getWriteResult()));
                WriteResult wr = (WriteResult)ois.readObject();

                return wr;

            } catch (EzMongoDriverException e) {
                Object o = deser(e.getEx());
                if (o == null){
                    throw new MongoException("Unknown Exception");
                }
                if (o instanceof IllegalArgumentException){
                    throw (IllegalArgumentException)o;
                } else if (o instanceof MongoException.DuplicateKey){
                    throw (MongoException.DuplicateKey)o;
                } else {
                    throw new MongoException(o.toString());
                }
            }  catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            return super.remove(query, concern, encoder);
        }
    }

    @Override
    public WriteResult remove(DBObject query, boolean multi, WriteConcern concern, DBEncoder encoder) {
        return super.remove(query, multi, concern, encoder);
    }

    @Override
    public WriteResult update(DBObject query, DBObject o, boolean upsert, boolean multi, WriteConcern concern, DBEncoder encoder) {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzUpdateRequest req = new EzUpdateRequest();

                ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(query);
                req.setQuery(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(o);
                req.setDbUpdateObject(bOut.toByteArray());

                req.setUpsert(upsert);
                req.setMulti(multi);

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(concern);
                req.setWriteConcern(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(encoder);
                req.setDbEncoder(bOut.toByteArray());

                req.setIsUnitTestMode(Boolean.getBoolean(UNIT_TEST_MODE));

                EzWriteResult res = client.update_driver(this.getName(), req, token);

                if (!res.isSetMongoexception()){
                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(res.getWriteResult()));
                    WriteResult wr = (WriteResult)ois.readObject();

                    return wr;
                } else {
                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(res.getMongoexception()));
                    MongoException ex = (MongoException)ois.readObject();
                    throw ex;
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            return super.update(query, o, upsert, multi, concern, encoder);
        }
    }

    @Override
    public void drop() {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                ResultsWrapper r = client.drop_driver(this.getName(),token);

                System.out.println("Drop result: " + r);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            super.drop();
        }
    }

    @Override
    public void doapply(DBObject o) {
        super.doapply(o);
    }

    @Override
    public void createIndex(DBObject keys, DBObject options, DBEncoder encoder) {
        if (Mongo.isClientModeEnabled()) {
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();
                EzCreateIndexRequest req = new EzCreateIndexRequest();

                ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(keys);
                req.setDbObjectKeys(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(options);
                req.setDbObjectOptions(bOut.toByteArray());

                bOut = new ByteArrayOutputStream();
                new ObjectOutputStream(bOut).writeObject(encoder);
                req.setDbEncoder(bOut.toByteArray());

                EzWriteResult res = client.createIndex_driver(this.getName(), req, token);

//                if (res.isSetMongoexception()) {
//                    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(res.getMongoexception()));
//                    MongoException ex = (MongoException) ois.readObject();
//                    throw ex;
//                }
            } catch (EzMongoDriverException e) {
                Object o = deser(e.getEx());
                if (o == null){
                    throw new MongoException("Unknown Exception");
                }
                if (o instanceof MongoException.DuplicateKey){
                    throw (MongoException.DuplicateKey)o;
                } else {
                    throw new MongoException(o.toString());
                }
            }  catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            }  finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            super.createIndex(keys, options, encoder);
        }
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param arr     {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @dochub insert Insert
     */
    @Override
    public WriteResult insert(DBObject[] arr, WriteConcern concern) {
        return super.insert(arr, concern);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param arr     {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @param encoder {@code DBEncoder} to be used
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @dochub insert Insert
     */
    @Override
    public WriteResult insert(DBObject[] arr, WriteConcern concern, DBEncoder encoder) {
        return super.insert(arr, concern, encoder);
    }

    /**
     * Insert a document into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param o       {@code DBObject} to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @dochub insert Insert
     */
    @Override
    public WriteResult insert(DBObject o, WriteConcern concern) {
        return super.insert(o, concern);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added. Collection wide {@code WriteConcern} will be used.
     *
     * @param arr {@code DBObject}'s to be inserted
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    @Override
    public WriteResult insert(DBObject... arr) {
        return super.insert(arr);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param concern {@code WriteConcern} to be used during operation
     * @param arr     {@code DBObject}'s to be inserted
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    @Override
    public WriteResult insert(WriteConcern concern, DBObject... arr) {
        return super.insert(concern, arr);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param list list of {@code DBObject} to be inserted
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    @Override
    public WriteResult insert(List<DBObject> list) {
        return super.insert(list);
    }

    /**
     * Insert documents into a collection. If the collection does not exists on the server, then it will be created. If the new document
     * does not contain an '_id' field, it will be added.
     *
     * @param list    list of {@code DBObject}'s to be inserted
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @mongodb.driver.manual tutorial/insert-documents/ Insert
     */
    @Override
    public WriteResult insert(List<DBObject> list, WriteConcern concern) {
        return super.insert(list, concern);
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link com.mongodb.DBCollection#find(DBObject)}.
     *
     * @param q       the selection criteria for the update
     * @param o       the modifications to apply
     * @param upsert  when true, inserts a document if no document matches the update query criteria
     * @param multi   when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    @Override
    public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi, WriteConcern concern) {
        return super.update(q, o, upsert, multi, concern);
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link com.mongodb.DBCollection#find(DBObject)}.  Calls {@link com.mongodb.DBCollection#update(com.mongodb.DBObject,
     * com.mongodb.DBObject, boolean, boolean, com.mongodb.WriteConcern)} with default WriteConcern.
     *
     * @param q      the selection criteria for the update
     * @param o      the modifications to apply
     * @param upsert when true, inserts a document if no document matches the update query criteria
     * @param multi  when true, updates all documents in the collection that match the update query criteria, otherwise only updates one
     * @return the result of the operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    @Override
    public WriteResult update(DBObject q, DBObject o, boolean upsert, boolean multi) {
        return super.update(q, o, upsert, multi);
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link com.mongodb.DBCollection#find(DBObject)}.  Calls {@link com.mongodb.DBCollection#update(com.mongodb.DBObject,
     * com.mongodb.DBObject, boolean, boolean)} with upsert=false and multi=false
     *
     * @param q the selection criteria for the update
     * @param o the modifications to apply
     * @return the result of the operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    @Override
    public WriteResult update(DBObject q, DBObject o) {
        return super.update(q, o);
    }

    /**
     * Modify an existing document or documents in collection. By default the method updates a single document. The query parameter employs
     * the same query selectors, as used in {@link com.mongodb.DBCollection#find()}.  Calls {@link com.mongodb.DBCollection#update(com.mongodb.DBObject,
     * com.mongodb.DBObject, boolean, boolean)} with upsert=false and multi=true
     *
     * @param q the selection criteria for the update
     * @param o the modifications to apply
     * @return the result of the operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/modify-documents/ Modify
     */
    @Override
    public WriteResult updateMulti(DBObject q, DBObject o) {
        return super.updateMulti(q, o);
    }

    /**
     * Remove documents from a collection.
     *
     * @param o       the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all
     *                documents in the collection.
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/remove-documents/ Remove
     */
    @Override
    public WriteResult remove(DBObject o, WriteConcern concern) {
        return super.remove(o, concern);
    }

    /**
     * Remove documents from a collection. Calls {@link com.mongodb.DBCollection#remove(com.mongodb.DBObject, com.mongodb.WriteConcern)} with the
     * default WriteConcern
     *
     * @param o the deletion criteria using query operators. Omit the query parameter or pass an empty document to delete all documents in
     *          the collection.
     * @return the result of the operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/remove-documents/ Remove
     */
    @Override
    public WriteResult remove(DBObject o) {
        return super.remove(o);
    }

    /**
     * Calls {@link com.mongodb.DBCollection#find(com.mongodb.DBObject, com.mongodb.DBObject, int, int)} and applies the query options
     *
     * @param query     query used to search
     * @param fields    the fields of matching objects to return
     * @param numToSkip number of objects to skip
     * @param batchSize the batch size. This option has a complex behavior, see {@link DBCursor#batchSize(int) }
     * @param options   see {@link com.mongodb.Bytes} QUERYOPTION_*
     * @return the cursor
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     * @deprecated use {@link com.mongodb.DBCursor#skip(int)}, {@link com.mongodb.DBCursor#batchSize(int)} and {@link
     * com.mongodb.DBCursor#setOptions(int)} on the {@code DBCursor} returned from {@link com.mongodb.DBCollection#find(DBObject,
     * DBObject)}
     */
    @Override
    public DBCursor find(DBObject query, DBObject fields, int numToSkip, int batchSize, int options) {
        return super.find(query, fields, numToSkip, batchSize, options);
    }

    /**
     * Finds objects from the database that match a query. A DBCursor object is returned, that can be iterated to go through the results.
     *
     * @param query     query used to search
     * @param fields    the fields of matching objects to return
     * @param numToSkip number of objects to skip
     * @param batchSize the batch size. This option has a complex behavior, see {@link DBCursor#batchSize(int) }
     * @return the cursor
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     * @deprecated use {@link com.mongodb.DBCursor#skip(int)} and {@link com.mongodb.DBCursor#batchSize(int)} on the {@code DBCursor}
     * returned from {@link com.mongodb.DBCollection#find(DBObject, DBObject)}
     */
    @Override
    public DBCursor find(DBObject query, DBObject fields, int numToSkip, int batchSize) {
        return super.find(query, fields, numToSkip, batchSize);
    }

    /**
     * Finds an object by its id.
     * This compares the passed in value to the _id field of the document
     *
     * @param obj any valid object
     * @return the object, if found, otherwise null
     * @throws com.mongodb.MongoException
     */
    @Override
    public DBObject findOne(Object obj) {
        return super.findOne(obj);
    }

    /**
     * Finds an object by its id.
     * This compares the passed in value to the _id field of the document
     *
     * @param obj    any valid object
     * @param fields fields to return
     * @return the object, if found, otherwise null
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBObject findOne(Object obj, DBObject fields) {
        return super.findOne(obj, fields);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query     specifies the selection criteria for the modification
     * @param fields    a subset of fields to return
     * @param sort      determines which document the operation will modify if the query selects multiple documents
     * @param remove    when true, removes the selected document
     * @param update    the modifications to apply
     * @param returnNew when true, returns the modified document rather than the original
     * @param upsert    when true, operation creates a new document if the query returns no documents
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert) {
        return super.findAndModify(query, fields, sort, remove, update, returnNew, upsert);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.
     *
     * @param query       specifies the selection criteria for the modification
     * @param fields      a subset of fields to return
     * @param sort        determines which document the operation will modify if the query selects multiple documents
     * @param remove      when {@code true}, removes the selected document
     * @param update      performs an update of the selected document
     * @param returnNew   when true, returns the modified document rather than the original
     * @param upsert      when true, operation creates a new document if the query returns no documents
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it. A non-zero value requires
     *                    a server version >= 2.6
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the document as it was before the modifications, unless {@code returnNew} is true, in which case it returns the document
     * after the changes were made
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     * @since 2.12.0
     */
    @Override
    public DBObject findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert, long maxTime, TimeUnit maxTimeUnit) {
        return super.findAndModify(query, fields, sort, remove, update, returnNew, upsert, maxTime, maxTimeUnit);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.  Calls {@link com.mongodb.DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean,
     * com.mongodb.DBObject, boolean, boolean)} with fields=null, remove=false, returnNew=false, upsert=false
     *
     * @param query  specifies the selection criteria for the modification
     * @param sort   determines which document the operation will modify if the query selects multiple documents
     * @param update the modifications to apply
     * @return the document as it was before the modifications.
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Override
    public DBObject findAndModify(DBObject query, DBObject sort, DBObject update) {
        return super.findAndModify(query, sort, update);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.  Calls {@link com.mongodb.DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean,
     * com.mongodb.DBObject, boolean, boolean)} with fields=null, sort=null, remove=false, returnNew=false, upsert=false
     *
     * @param query  specifies the selection criteria for the modification
     * @param update the modifications to apply
     * @return the document as it was before the modifications.
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Override
    public DBObject findAndModify(DBObject query, DBObject update) {
        return super.findAndModify(query, update);
    }

    /**
     * Atomically modify and return a single document. By default, the returned document does not include the modifications made on the
     * update.  Ccalls {@link com.mongodb.DBCollection#findAndModify(com.mongodb.DBObject, com.mongodb.DBObject, com.mongodb.DBObject, boolean,
     * com.mongodb.DBObject, boolean, boolean)} with fields=null, sort=null, remove=true, returnNew=false, upsert=false
     *
     * @param query specifies the selection criteria for the modification
     * @return the document as it was before it was removed
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
     */
    @Override
    public DBObject findAndRemove(DBObject query) {
        return super.findAndRemove(query);
    }

    /**
     * Calls {@link com.mongodb.DBCollection#createIndex(com.mongodb.DBObject, com.mongodb.DBObject)} with default index options
     *
     * @param keys a document that contains pairs with the name of the field or fields to index and order of the index
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    @Override
    public void createIndex(DBObject keys) {
        super.createIndex(keys);
    }

    /**
     * Forces creation of an index on a set of fields, if one does not already exist.
     *
     * @param keys    a document that contains pairs with the name of the field or fields to index and order of the index
     * @param options a document that controls the creation of the index.
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     */
    @Override
    public void createIndex(DBObject keys, DBObject options) {
        super.createIndex(keys, options);
    }

    /**
     * Creates an ascending index on a field with default options, if one does not already exist.
     *
     * @param name name of field to index on
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link com.mongodb.DBCollection#createIndex(com.mongodb.DBObject)} instead
     */
    @Override
    public void ensureIndex(String name) {
        super.ensureIndex(name);
    }

    /**
     * Calls {@link com.mongodb.DBCollection#ensureIndex(com.mongodb.DBObject, com.mongodb.DBObject)} with default options
     *
     * @param keys an object with a key set of the fields desired for the index
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link com.mongodb.DBCollection#createIndex(DBObject)} instead
     */
    @Override
    public void ensureIndex(DBObject keys) {
        super.ensureIndex(keys);
    }

    /**
     * Calls {@link com.mongodb.DBCollection#ensureIndex(com.mongodb.DBObject, String, boolean)} with unique=false
     *
     * @param keys fields to use for index
     * @param name an identifier for the index
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link com.mongodb.DBCollection#createIndex(DBObject, DBObject)} instead
     */
    @Override
    public void ensureIndex(DBObject keys, String name) {
        super.ensureIndex(keys, name);
    }

    /**
     * Ensures an index on this collection (that is, the index will be created if it does not exist).
     *
     * @param keys   fields to use for index
     * @param name   an identifier for the index. If null or empty, the default name will be used.
     * @param unique if the index should be unique
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link com.mongodb.DBCollection#createIndex(DBObject, DBObject)} instead
     */
    @Override
    public void ensureIndex(DBObject keys, String name, boolean unique) {
        super.ensureIndex(keys, name, unique);
    }

    /**
     * Creates an index on a set of fields, if one does not already exist.
     *
     * @param keys      an object with a key set of the fields desired for the index
     * @param optionsIN options for the index (name, unique, etc)
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /administration/indexes-creation/ Index Creation Tutorials
     * @deprecated use {@link com.mongodb.DBCollection#createIndex(DBObject, DBObject)} instead
     */
    @Override
    public void ensureIndex(DBObject keys, DBObject optionsIN) {
        super.ensureIndex(keys, optionsIN);
    }

    /**
     * Clears all indices that have not yet been applied to this collection.
     *
     * @deprecated This will be removed in 3.0
     */
    @Override
    public void resetIndexCache() {
        super.resetIndexCache();
    }

    @Override
    DBObject defaultOptions(DBObject keys) {
        return super.defaultOptions(keys);
    }

    /**
     * Set hint fields for this collection (to optimize queries).
     *
     * @param lst a list of {@code DBObject}s to be used as hints
     */
    @Override
    public void setHintFields(List<DBObject> lst) {
        super.setHintFields(lst);
    }

    /**
     * Get hint fields for this collection (used to optimize queries).
     *
     * @return a list of {@code DBObject} to be used as hints.
     */
    @Override
    protected List<DBObject> getHintFields() {
        return super.getHintFields();
    }

    /**
     * Queries for an object in this collection.
     *
     * @param ref A document outlining the search query
     * @return an iterator over the results
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBCursor find(DBObject ref) {
        return super.find(ref);
    }

    /**
     * Queries for an object in this collection.
     * <p>
     * An empty DBObject will match every document in the collection.
     * Regardless of fields specified, the _id fields are always returned.
     * </p>
     * <p>
     * An example that returns the "x" and "_id" fields for every document
     * in the collection that has an "x" field:
     * </p>
     * <pre>
     * {@code
     * BasicDBObject keys = new BasicDBObject();
     * keys.put("x", 1);
     *
     * DBCursor cursor = collection.find(new BasicDBObject(), keys);}
     * </pre>
     *
     * @param ref  object for which to search
     * @param keys fields to return
     * @return a cursor to iterate over results
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBCursor find(DBObject ref, DBObject keys) {
        return super.find(ref, keys);
    }

    /**
     * Queries for all objects in this collection.
     *
     * @return a cursor which will iterate over every object
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBCursor find() {
        return super.find();
    }

    /**
     * Returns a single object from this collection.
     *
     * @return the object found, or {@code null} if the collection is empty
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBObject findOne() {
        return super.findOne();
    }

    /**
     * Returns a single object from this collection matching the query.
     *
     * @param o the query object
     * @return the object found, or {@code null} if no such object exists
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBObject findOne(DBObject o) {
        return super.findOne(o);
    }

    /**
     * Returns a single object from this collection matching the query.
     *
     * @param o      the query object
     * @param fields fields to return
     * @return the object found, or {@code null} if no such object exists
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBObject findOne(DBObject o, DBObject fields) {
        return super.findOne(o, fields);
    }

    /**
     * Returns a single object from this collection matching the query.
     *
     * @param o       the query object
     * @param fields  fields to return
     * @param orderBy fields to order by
     * @return the object found, or {@code null} if no such object exists
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBObject findOne(DBObject o, DBObject fields, DBObject orderBy) {
        return super.findOne(o, fields, orderBy);
    }

    /**
     * Get a single document from collection.
     *
     * @param o        the selection criteria using query operators.
     * @param fields   specifies which fields MongoDB will return from the documents in the result set.
     * @param readPref {@link ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method, or {@code null} if no such object exists
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBObject findOne(DBObject o, DBObject fields, ReadPreference readPref) {
        return super.findOne(o, fields, readPref);
    }

    /**
     * Get a single document from collection.
     *
     * @param o        the selection criteria using query operators.
     * @param fields   specifies which projection MongoDB will return from the documents in the result set.
     * @param orderBy  A document whose fields specify the attributes on which to sort the result set.
     * @param readPref {@code ReadPreference} to be used for this operation
     * @return A document that satisfies the query specified as the argument to this method, or {@code null} if no such object exists
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual tutorial/query-documents/ Query
     */
    @Override
    public DBObject findOne(DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref) {
        return super.findOne(o, fields, orderBy, readPref);
    }

    /**
     * Get a single document from collection.
     *
     * @param o           the selection criteria using query operators.
     * @param fields      specifies which projection MongoDB will return from the documents in the result set.
     * @param orderBy     A document whose fields specify the attributes on which to sort the result set.
     * @param readPref    {@code ReadPreference} to be used for this operation
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return A document that satisfies the query specified as the argument to this method.
     * @mongodb.driver.manual tutorial/query-documents/ Query
     * @since 2.12.0
     */
    @Override
    DBObject findOne(DBObject o, DBObject fields, DBObject orderBy, ReadPreference readPref, long maxTime, TimeUnit maxTimeUnit) {
        return super.findOne(o, fields, orderBy, readPref, maxTime, maxTimeUnit);
    }

    @Override
    DBDecoder getDecoder() {
        return super.getDecoder();
    }

    /**
     * calls {@link com.mongodb.DBCollection#apply(com.mongodb.DBObject, boolean)} with ensureID=true
     *
     * @param o {@code DBObject} to which to add fields
     * @return the modified parameter object
     */
    @Override
    public Object apply(DBObject o) {
        return super.apply(o);
    }

    /**
     * calls {@link com.mongodb.DBCollection#doapply(com.mongodb.DBObject)}, optionally adding an automatic _id field
     *
     * @param jo       object to add fields to
     * @param ensureID whether to add an {@code _id} field
     * @return the modified object {@code o}
     */
    @Override
    public Object apply(DBObject jo, boolean ensureID) {
        return super.apply(jo, ensureID);
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectid value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field: <ul> <li>If a
     * document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the document.</li>
     * <li>If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record
     * with the fields from the document.</li> </ul>. Calls {@link com.mongodb.DBCollection#save(com.mongodb.DBObject, com.mongodb.WriteConcern)} with
     * default WriteConcern
     *
     * @param jo {@link DBObject} to save to the collection.
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    @Override
    public WriteResult save(DBObject jo) {
        return super.save(jo);
    }

    /**
     * Update an existing document or insert a document depending on the parameter. If the document does not contain an '_id' field, then
     * the method performs an insert with the specified fields in the document as well as an '_id' field with a unique objectid value. If
     * the document contains an '_id' field, then the method performs an upsert querying the collection on the '_id' field: <ul> <li>If a
     * document does not exist with the specified '_id' value, the method performs an insert with the specified fields in the document.</li>
     * <li>If a document exists with the specified '_id' value, the method performs an update, replacing all field in the existing record
     * with the fields from the document.</li> </ul>
     *
     * @param jo      {@link DBObject} to save to the collection.
     * @param concern {@code WriteConcern} to be used during operation
     * @return the result of the operation
     * @throws com.mongodb.MongoException if the operation fails
     * @mongodb.driver.manual tutorial/modify-documents/#modify-a-document-with-save-method Save
     */
    @Override
    public WriteResult save(DBObject jo, WriteConcern concern) {
        return super.save(jo, concern);
    }

    /**
     * Drops all indices from this collection
     *
     * @throws com.mongodb.MongoException
     */
    @Override
    public void dropIndexes() {
        super.dropIndexes();
    }

    /**
     * Drops an index from this collection
     *
     * @param name the index name
     * @throws com.mongodb.MongoException
     */
    @Override
    public void dropIndexes(String name) {
        super.dropIndexes(name);
    }

    /**
     * Get the number of documents in the collection.
     *
     * @return the number of documents
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long count() {
        return super.count();
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long count(DBObject query) {
        return super.count(query);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query     specifies the selection criteria
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long count(DBObject query, ReadPreference readPrefs) {
        return super.count(query, readPrefs);
    }

    /**
     * Get the count of documents in a collection.  Calls {@link com.mongodb.DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject)} with an
     * empty query and null fields.
     *
     * @return the number of documents in the collection
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long getCount() {
        return super.getCount();
    }

    /**
     * Get the count of documents in a collection. Calls {@link com.mongodb.DBCollection#getCount(com.mongodb.DBObject, com.mongodb.DBObject,
     * com.mongodb.ReadPreference)} with empty query and null fields.
     *
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long getCount(ReadPreference readPrefs) {
        return super.getCount(readPrefs);
    }

    /**
     * Get the count of documents in collection that would match a criteria. Calls {@link com.mongodb.DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject)} with null fields.
     *
     * @param query specifies the selection criteria
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long getCount(DBObject query) {
        return super.getCount(query);
    }

    /**
     * Get the count of documents in collection that would match a criteria. Calls {@link com.mongodb.DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject, long, long)} with limit=0 and skip=0
     *
     * @param query  specifies the selection criteria
     * @param fields this is ignored
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long getCount(DBObject query, DBObject fields) {
        return super.getCount(query, fields);
    }

    /**
     * Get the count of documents in collection that would match a criteria.  Calls {@link com.mongodb.DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject, long, long, com.mongodb.ReadPreference)} with limit=0 and skip=0
     *
     * @param query     specifies the selection criteria
     * @param fields    this is ignored
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long getCount(DBObject query, DBObject fields, ReadPreference readPrefs) {
        return super.getCount(query, fields, readPrefs);
    }

    /**
     * Get the count of documents in collection that would match a criteria.  Calls {@link com.mongodb.DBCollection#getCount(com.mongodb.DBObject,
     * com.mongodb.DBObject, long, long, com.mongodb.ReadPreference)} with the DBCollection's ReadPreference
     *
     * @param query  specifies the selection criteria
     * @param fields this is ignored
     * @param limit  limit the count to this value
     * @param skip   number of documents to skip
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long getCount(DBObject query, DBObject fields, long limit, long skip) {
        return super.getCount(query, fields, limit, skip);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query     specifies the selection criteria
     * @param fields    this is ignored
     * @param limit     limit the count to this value
     * @param skip      number of documents to skip
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     */
    @Override
    public long getCount(DBObject query, DBObject fields, long limit, long skip, ReadPreference readPrefs) {
        return super.getCount(query, fields, limit, skip, readPrefs);
    }

    /**
     * Get the count of documents in collection that would match a criteria.
     *
     * @param query       specifies the selection criteria
     * @param fields      this is ignored
     * @param limit       limit the count to this value
     * @param skip        number of documents to skip
     * @param readPrefs   {@link ReadPreference} to be used for this operation
     * @param maxTime     the maximum time that the server will allow this operation to execute before killing it
     * @param maxTimeUnit the unit that maxTime is specified in
     * @return the number of documents that matches selection criteria
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/count/ Count
     * @since 2.12
     */
    @Override
    long getCount(DBObject query, DBObject fields, long limit, long skip, ReadPreference readPrefs, long maxTime, TimeUnit maxTimeUnit) {
        return super.getCount(query, fields, limit, skip, readPrefs, maxTime, maxTimeUnit);
    }

    @Override
    CommandResult command(DBObject cmd, int options, ReadPreference readPrefs) {
        return super.command(cmd, options, readPrefs);
    }

    /**
     * Calls {@link DBCollection#rename(String, boolean)} with dropTarget=false
     *
     * @param newName new collection name (not a full namespace)
     * @return the new collection
     * @throws com.mongodb.MongoException
     */
    @Override
    public DBCollection rename(String newName) {
        return super.rename(newName);
    }

    /**
     * Renames of this collection to newName
     *
     * @param newName    new collection name (not a full namespace)
     * @param dropTarget if a collection with the new name exists, whether or not to drop it
     * @return the new collection
     * @throws com.mongodb.MongoException
     */
    @Override
    public DBCollection rename(String newName, boolean dropTarget) {
        return super.rename(newName, dropTarget);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL. Calls {@link com.mongodb.DBCollection#group(com.mongodb.DBObject,
     * com.mongodb.DBObject, com.mongodb.DBObject, String, String)} with finalize=null
     *
     * @param key     specifies one or more document fields to group
     * @param cond    specifies the selection criteria to determine which documents in the collection to process
     * @param initial initializes the aggregation result document
     * @param reduce  specifies an $reduce Javascript function, that operates on the documents during the grouping operation
     * @return a document with the grouped records as well as the command meta-data
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    @Override
    public DBObject group(DBObject key, DBObject cond, DBObject initial, String reduce) {
        return super.group(key, cond, initial, reduce);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param key      specifies one or more document fields to group
     * @param cond     specifies the selection criteria to determine which documents in the collection to process
     * @param initial  initializes the aggregation result document
     * @param reduce   specifies an $reduce Javascript function, that operates on the documents during the grouping operation
     * @param finalize specifies a Javascript function that runs each item in the result set before final value will be returned
     * @return a document with the grouped records as well as the command meta-data
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    @Override
    public DBObject group(DBObject key, DBObject cond, DBObject initial, String reduce, String finalize) {
        return super.group(key, cond, initial, reduce, finalize);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param key       specifies one or more document fields to group
     * @param cond      specifies the selection criteria to determine which documents in the collection to process
     * @param initial   initializes the aggregation result document
     * @param reduce    specifies an $reduce Javascript function, that operates on the documents during the grouping operation
     * @param finalize  specifies a Javascript function that runs each item in the result set before final value will be returned
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return a document with the grouped records as well as the command meta-data
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    @Override
    public DBObject group(DBObject key, DBObject cond, DBObject initial, String reduce, String finalize, ReadPreference readPrefs) {
        return super.group(key, cond, initial, reduce, finalize, readPrefs);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param cmd the group command containing the details of how to perform the operation.
     * @return a document with the grouped records as well as the command meta-data
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    @Override
    public DBObject group(GroupCommand cmd) {
        return super.group(cmd);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param cmd       the group command containing the details of how to perform the operation.
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return a document with the grouped records as well as the command meta-data
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     */
    @Override
    public DBObject group(GroupCommand cmd, ReadPreference readPrefs) {
        return super.group(cmd, readPrefs);
    }

    /**
     * Group documents in a collection by the specified key and performs simple aggregation functions such as computing counts and sums.
     * This is analogous to a {@code SELECT ... GROUP BY} statement in SQL.
     *
     * @param args object representing the arguments to the group function
     * @return a document with the grouped records as well as the command meta-data
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/group/ Group Command
     * @deprecated use {@link com.mongodb.DBCollection#group(com.mongodb.GroupCommand)} instead.  This method will be removed in 3.0
     */
    @Override
    public DBObject group(DBObject args) {
        return super.group(args);
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key Specifies the field for which to return the distinct values
     * @return A {@code List} of the distinct values
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    @Override
    public List distinct(String key) {
        return super.distinct(key);
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key       Specifies the field for which to return the distinct values
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return A {@code List} of the distinct values
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    @Override
    public List distinct(String key, ReadPreference readPrefs) {
        return super.distinct(key, readPrefs);
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key   Specifies the field for which to return the distinct values
     * @param query specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @return A {@code List} of the distinct values
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    @Override
    public List distinct(String key, DBObject query) {
        return super.distinct(key, query);
    }

    /**
     * Find the distinct values for a specified field across a collection and returns the results in an array.
     *
     * @param key       Specifies the field for which to return the distinct values
     * @param query     specifies the selection query to determine the subset of documents from which to retrieve the distinct values
     * @param readPrefs {@link ReadPreference} to be used for this operation
     * @return A {@code List} of the distinct values
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual reference/command/distinct Distinct Command
     */
    @Override
    public List distinct(String key, DBObject query, ReadPreference readPrefs) {
        return super.distinct(key, query, readPrefs);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection.  Runs the command in REPLACE output mode (saves to named
     * collection).
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation (optional) - leave null if want to use temp
     *                     collection
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map
     *                     function.
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    @Override
    public MapReduceOutput mapReduce(String map, String reduce, String outputTarget, DBObject query) {
        return super.mapReduce(map, reduce, outputTarget, query);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     * Specify an outputType to control job execution<ul>
     * <li>INLINE - Return results inline</li>
     * <li>REPLACE - Replace the output collection with the job output</li>
     * <li>MERGE - Merge the job output with the existing contents of outputTarget</li>
     * <li>REDUCE - Reduce the job output with the existing contents of outputTarget</li>
     * </ul>
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation (optional) - leave null if want to use temp
     *                     collection
     * @param outputType   specifies the type of job output
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map function.
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    @Override
    public MapReduceOutput mapReduce(String map, String reduce, String outputTarget, MapReduceCommand.OutputType outputType, DBObject query) {
        return super.mapReduce(map, reduce, outputTarget, outputType, query);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     * Specify an outputType to control job execution<ul>
     * <li>INLINE - Return results inline</li>
     * <li>REPLACE - Replace the output collection with the job output</li>
     * <li>MERGE - Merge the job output with the existing contents of outputTarget</li>
     * <li>REDUCE - Reduce the job output with the existing contents of outputTarget</li>
     * </ul>
     *
     * @param map          a JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduce       a JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param outputTarget specifies the location of the result of the map-reduce operation (optional) - leave null if want to use temp
     *                     collection
     * @param outputType   specifies the type of job output
     * @param query        specifies the selection criteria using query operators for determining the documents input to the map
     *                     function.
     * @param readPrefs    the read preference specifying where to run the query.  Only applied for Inline output type
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    @Override
    public MapReduceOutput mapReduce(String map, String reduce, String outputTarget, MapReduceCommand.OutputType outputType, DBObject query, ReadPreference readPrefs) {
        return super.mapReduce(map, reduce, outputTarget, outputType, query, readPrefs);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection and saves to a named collection.
     *
     * @param command object representing the parameters to the operation
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     */
    @Override
    public MapReduceOutput mapReduce(MapReduceCommand command) {
        return super.mapReduce(command);
    }

    /**
     * Allows you to run map-reduce aggregation operations over a collection
     *
     * @param command document representing the parameters to this operation.
     * @return A MapReduceOutput which contains the results of this map reduce operation
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual core/map-reduce/ Map-Reduce
     * @deprecated Use {@link com.mongodb.DBCollection#mapReduce(com.mongodb.MapReduceCommand)} instead
     */
    @Override
    public MapReduceOutput mapReduce(DBObject command) {
        return super.mapReduce(command);
    }

    /**
     * Method implements aggregation framework.
     *
     * @param firstOp       requisite first operation to be performed in the aggregation pipeline
     * @param additionalOps additional operations to be performed in the aggregation pipeline
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     * @deprecated Use {@link com.mongodb.DBCollection#aggregate(java.util.List)} instead
     */
    @Override
    public AggregationOutput aggregate(DBObject firstOp, DBObject... additionalOps) {
        return super.aggregate(firstOp, additionalOps);
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline operations to be performed in the aggregation pipeline
     * @return the aggregation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    @Override
    public AggregationOutput aggregate(List<DBObject> pipeline) {
    	return super.aggregate(pipeline);
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline       operations to be performed in the aggregation pipeline
     * @param readPreference the read preference specifying where to run the query
     * @return the aggregation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    @Override
    public AggregationOutput aggregate(List<DBObject> pipeline, ReadPreference readPreference) {
        return super.aggregate(pipeline, readPreference);
    }

    /**
     * Method implements aggregation framework.
     *
     * @param pipeline operations to be performed in the aggregation pipeline
     * @param options  options to apply to the aggregation
     * @return the aggregation operation's result set
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.server.release 2.2
     */
    @Override
    public Cursor aggregate(List<DBObject> pipeline, AggregationOptions options) {
        return super.aggregate(pipeline, options);
    }

    /**
     * Return the explain plan for the aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline to explain
     * @param options  the options to apply to the aggregation
     * @return the command result.  The explain output may change from release to release, so best to simply log this.
     * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation
     * @mongodb.driver.manual reference/operator/meta/explain/ Explain query
     * @mongodb.server.release 2.6
     */
    @Override
    public CommandResult explainAggregate(List<DBObject> pipeline, AggregationOptions options) {
        return super.explainAggregate(pipeline, options);
    }

    /**
     * Creates a builder for an ordered bulk operation.  Write requests included in the bulk operations will be executed in order,
     * and will halt on the first failure.
     *
     * @return the builder
     * @since 2.12
     */
    @Override
    public BulkWriteOperation initializeOrderedBulkOperation() {
        return super.initializeOrderedBulkOperation();
    }

    /**
     * Creates a builder for an unordered bulk operation. Write requests included in the bulk operation will be executed in an undefined
     * order, and all requests will be executed even if some fail.
     *
     * @return the builder
     * @since 2.12
     */
    @Override
    public BulkWriteOperation initializeUnorderedBulkOperation() {
        return super.initializeUnorderedBulkOperation();
    }

    @Override
    BulkWriteResult executeBulkWriteOperation(boolean ordered, List<WriteRequest> requests) {
        return super.executeBulkWriteOperation(ordered, requests);
    }

    @Override
    BulkWriteResult executeBulkWriteOperation(boolean ordered, List<WriteRequest> requests, WriteConcern writeConcern) {
        return super.executeBulkWriteOperation(ordered, requests, writeConcern);
    }

    @Override
    DBObject prepareCommand(List<DBObject> pipeline, AggregationOptions options) {
        return super.prepareCommand(pipeline, options);
    }

    /**
     * Return a list of the indexes for this collection.  Each object in the list is the "info document" from MongoDB
     *
     * @return list of index documents
     * @throws com.mongodb.MongoException
     */
    @Override
    public List<DBObject> getIndexInfo() {
        return super.getIndexInfo();
    }

    /**
     * Drops an index from this collection
     *
     * @param keys keys of the index
     * @throws com.mongodb.MongoException
     */
    @Override
    public void dropIndex(DBObject keys) {
        super.dropIndex(keys);
    }

    /**
     * Drops an index from this collection
     *
     * @param name name of index to drop
     * @throws com.mongodb.MongoException
     */
    @Override
    public void dropIndex(String name) {
        super.dropIndex(name);
    }

    /**
     * The collStats command returns a variety of storage statistics for a given collection
     *
     * @return a CommandResult containing the statistics about this collection
     * @mongodb.driver.manual /reference/command/collStats/ collStats command
     */
    @Override
    public CommandResult getStats() {
        return super.getStats();
    }

    /**
     * Checks whether this collection is capped
     *
     * @return true if this is a capped collection
     * @throws com.mongodb.MongoException
     * @mongodb.driver.manual /core/capped-collections/#check-if-a-collection-is-capped Capped Collections
     */
    @Override
    public boolean isCapped() {
        return super.isCapped();
    }

    /**
     * @param o
     * @param canBeNull
     * @param query
     * @deprecated This method should not be a part of API.
     * If you override one of the {@code DBCollection} methods please rely on superclass
     * implementation in checking argument correctness and validity.
     */
    @Override
    protected DBObject _checkObject(DBObject o, boolean canBeNull, boolean query) {
        return super._checkObject(o, canBeNull, query);
    }

    /**
     * Find a collection that is prefixed with this collection's name. A typical use of this might be
     * <pre>{@code
     *    DBCollection users = mongo.getCollection( "wiki" ).getCollection( "users" );
     * }</pre>
     * Which is equivalent to
     * <pre>{@code
     *   DBCollection users = mongo.getCollection( "wiki.users" );
     * }</pre>
     *
     * @param n the name of the collection to find
     * @return the matching collection
     */
    @Override
    public DBCollection getCollection(String n) {
        return super.getCollection(n);
    }

    /**
     * Returns the name of this collection.
     *
     * @return the name of this collection
     */
    @Override
    public String getName() {
        return super.getName();
    }

    /**
     * Returns the full name of this collection, with the database name as a prefix.
     *
     * @return the name of this collection
     */
    @Override
    public String getFullName() {
        return super.getFullName();
    }

    /**
     * Returns the database this collection is a member of.
     *
     * @return this collection's database
     */
    @Override
    public DB getDB() {
        return super.getDB();
    }

    /**
     * Returns if this collection's database is read-only
     *
     * @param strict if an exception should be thrown if the database is read-only
     * @return if this collection's database is read-only
     * @throws RuntimeException if the database is read-only and {@code strict} is set
     * @deprecated See {@link com.mongodb.DB#setReadOnly(Boolean)}
     */
    @Override
    protected boolean checkReadOnly(boolean strict) {
        return super.checkReadOnly(strict);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    /**
     * Sets a default class for objects in this collection; null resets the class to nothing.
     *
     * @param c the class
     * @throws IllegalArgumentException if {@code c} is not a DBObject
     */
    @Override
    public void setObjectClass(Class c) {
        super.setObjectClass(c);
    }

    /**
     * Gets the default class for objects in the collection
     *
     * @return the class
     */
    @Override
    public Class getObjectClass() {
        return super.getObjectClass();
    }

    /**
     * Sets the internal class for the given path in the document hierarchy
     *
     * @param path the path to map the given Class to
     * @param c    the Class to map the given path to
     */
    @Override
    public void setInternalClass(String path, Class c) {
        super.setInternalClass(path, c);
    }

    /**
     * Gets the internal class for the given path in the document hierarchy
     *
     * @param path the path to map the given Class to
     * @return the class for a given path in the hierarchy
     */
    @Override
    protected Class getInternalClass(String path) {
        return super.getInternalClass(path);
    }

    /**
     * Set the write concern for this collection. Will be used for
     * writes to this collection. Overrides any setting of write
     * concern at the DB level. See the documentation for
     * {@link WriteConcern} for more information.
     *
     * @param concern write concern to use
     */
    @Override
    public void setWriteConcern(WriteConcern concern) {
        super.setWriteConcern(concern);
    }

    /**
     * Get the {@link WriteConcern} for this collection.
     *
     * @return the default write concern for this collection
     */
    @Override
    public WriteConcern getWriteConcern() {
        return super.getWriteConcern();
    }

    /**
     * Sets the read preference for this collection. Will be used as default
     * for reads from this collection; overrides DB & Connection level settings.
     * See the * documentation for {@link ReadPreference} for more information.
     *
     * @param preference Read Preference to use
     */
    @Override
    public void setReadPreference(ReadPreference preference) {
        super.setReadPreference(preference);
    }

    /**
     * Gets the {@link ReadPreference}.
     *
     * @return the default read preference for this collection
     */
    @Override
    public ReadPreference getReadPreference() {
        return super.getReadPreference();
    }

    /**
     * Makes this query ok to run on a slave node
     *
     * @deprecated Replaced with {@link com.mongodb.ReadPreference#secondaryPreferred()}
     */
    @Override
    public void slaveOk() {
        super.slaveOk();
    }

    /**
     * Adds the given flag to the query options.
     *
     * @param option value to be added
     */
    @Override
    public void addOption(int option) {
        super.addOption(option);
    }

    /**
     * Sets the query options, overwriting previous value.
     *
     * @param options bit vector of query options
     */
    @Override
    public void setOptions(int options) {
        super.setOptions(options);
    }

    /**
     * Resets the default query options
     */
    @Override
    public void resetOptions() {
        super.resetOptions();
    }

    /**
     * Gets the default query options
     *
     * @return bit vector of query options
     */
    @Override
    public int getOptions() {
        return super.getOptions();
    }

    /**
     * Set a customer decoder factory for this collection.  Set to null to use the default from MongoOptions.
     *
     * @param fact the factory to set.
     */
    @Override
    public synchronized void setDBDecoderFactory(DBDecoderFactory fact) {
        super.setDBDecoderFactory(fact);
    }

    /**
     * Get the decoder factory for this collection.  A null return value means that the default from MongoOptions is being used.
     *
     * @return the factory
     */
    @Override
    public synchronized DBDecoderFactory getDBDecoderFactory() {
        return super.getDBDecoderFactory();
    }

    /**
     * Set a customer encoder factory for this collection.  Set to null to use the default from MongoOptions.
     *
     * @param fact the factory to set.
     */
    @Override
    public synchronized void setDBEncoderFactory(DBEncoderFactory fact) {
        super.setDBEncoderFactory(fact);
    }

    /**
     * Get the encoder factory for this collection.  A null return value means that the default from MongoOptions is being used.
     *
     * @return the factory
     */
    @Override
    public synchronized DBEncoderFactory getDBEncoderFactory() {
        return super.getDBEncoderFactory();
    }

    public Response getMore(int qriHashcode, OutMessage m, int remainingRetries, ReadPreference readPref, DBDecoder decoder) {
        if (Mongo.isClientModeEnabled()){
            EzMongoDriverService.Client client = null;
            try {
                client = getThriftClient();
                EzSecurityToken token = getToken();

                EzGetMoreRequest req = new EzGetMoreRequest();
                req.setOutmessage(ser(m));
                req.setDecoder(ser(decoder));
                req.setQueryResultIteratorHashcode(""+qriHashcode);
                EzGetMoreResponse res = client.getMore_driver(this.getName(), req, token);

                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(res.getResponse()));
                Response response = (Response) ois.readObject();
                return response;
            }  catch (EzMongoDriverException e) {
                Object o = deser(e.getEx());
                if (o == null){
                    throw new MongoException("Unknown Exception");
                }
                if (o instanceof IllegalArgumentException){
                    throw (IllegalArgumentException)o;
                } else {
                    throw new MongoException(o.toString());
                }
            }  catch (Exception e) {
                e.printStackTrace();
                throw new MongoException(e.toString());
            } finally {
                if (client != null) {
                    pool.returnToPool(client);
                }
            }
        } else {
            throw new MongoException("something wrong, must be in service mode mode in order to get here");
        }
    }
}
