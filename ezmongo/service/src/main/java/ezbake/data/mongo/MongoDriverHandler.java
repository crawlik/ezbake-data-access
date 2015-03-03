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

import com.mongodb.*;
import com.mongodb.gridfs.GridFSInputFile;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.common.TokenUtils;
import ezbake.data.mongo.driver.thrift.*;
import ezbake.data.mongo.redact.RedactHelper;
import ezbake.data.mongo.thrift.EzMongoBaseException;
import ezbake.util.AuditEventType;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jagius on 8/5/14.
 */
public class MongoDriverHandler {

    public static final String GRID_FS_INPUT_FILE_MAP_CACHE = "gridFSInputFileMapCache";
    public static final String IT_MAP_CACHE = "iteratorMapCache";
    private final Logger appLog = LoggerFactory.getLogger(MongoDriverHandler.class);

    final protected EzMongoHandler handler;
    final private String appName, dbName;

    CacheManager cm = null;

    private HandlerForDriverFindCalls findHandler;

    protected MongoDriverHandler(EzMongoHandler handler){
        this.handler = handler;
        this.appName = handler.appName;
        this.dbName = handler.dbName;

        cm = CacheManager.create();

        // TODO This enables JMX monitoring, perhaps we should think about introducing this paradigm across all services
        // but for now, this could be uncommented of we insight on the caching behavior.
       // net.sf.ehcache.management.ManagementService.registerMBeans(cm, ManagementFactory.getPlatformMBeanServer(), false, false, false, true);
    }

    protected void init(){
        findHandler = new HandlerForDriverFindCalls(this);
    }

    protected String normalizeCollection(String collectionName) {
        return collectionName; // maybe never do anything here
    }

    public EzWriteResult insert_driver(String collection, EzInsertRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {
//        if (!collection.equals("fs.chunks") && !collection.equals("fs.files")) {
            collection = appName + "_" + collection;
//        }

        appLog.debug("insert_driver() to collection: {}", collection);

        TokenUtils.validateSecurityToken(token, handler.getConfigurationProperties());
        EzWriteResult ewr = new EzWriteResult();
        try{
            DBCollection c = handler.db.getCollection(normalizeCollection(collection));

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbObjectList()));
            List<DBObject> list = (List<DBObject>)ois.readObject();

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getWriteConcern()));
            WriteConcern writeConcern = (WriteConcern)ois.readObject();

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbEncoder()));
            DBEncoder dbEncoder = (DBEncoder)ois.readObject();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "insert_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("list", handler.printMongoObject(list));
            auditParamsMap.put("writeConcern", handler.printMongoObject(writeConcern));
            auditParamsMap.put("dbEncoder", handler.printMongoObject(dbEncoder));
            handler.auditLog(token, AuditEventType.FileObjectCreate, auditParamsMap);

            WriteResult res = null;

            Boolean isDriverUnitTestMode = req.isIsUnitTestMode();

            if (!isDriverUnitTestMode) {
                // check if the user has the auths to insert these records
                List<DBObject> insertableList = new ArrayList<>();
                for (DBObject dbObject : list) {
                    try {
                        handler.getMongoInsertHelper().checkAbilityToInsert(token, null, dbObject, null, false, true);
                        insertableList.add(dbObject);
                    } catch(ClassCastException | VisibilityParseException | EzMongoBaseException e) {
                        appLog.error(e.toString());
                        appLog.debug("User does not have the auths to insert record: {}", dbObject);
                    }
                }
                res = c.insert(insertableList, writeConcern, dbEncoder);
            } else {
                res = c.insert(list, writeConcern, dbEncoder);
            }

            if (list != null && list.size() == 1 && list.get(0) instanceof GridFSInputFile){
                GridFSInputFile g = (GridFSInputFile)list.get(0);
                Object o = g.getId();
                if (o instanceof ObjectId){
                    ObjectId id = (ObjectId)o;
                    cm.getCache(GRID_FS_INPUT_FILE_MAP_CACHE).put(new Element(id.toString(), id));
                }
            }

            appLog.debug("WriteResult: {}", res);

            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            new ObjectOutputStream(bOut).writeObject(res);

            ewr.setWriteResult(bOut.toByteArray());
        } catch (Exception e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }
        return ewr;
    }

    public ResultsWrapper find_driver(String collection, EzFindRequest ezFindRequest, EzSecurityToken token) throws TException, EzMongoDriverException {
        return findHandler.find_driver(collection,ezFindRequest,token);
    }

    public EzWriteResult update_driver(String collection, EzUpdateRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {
        if (!collection.equals("fs.chunks") && !collection.equals("fs.files")) {
            collection = appName + "_" + collection;
        }

        appLog.debug("update_driver() collection: {}", collection);

        TokenUtils.validateSecurityToken(token, handler.getConfigurationProperties());
        EzWriteResult ewr = new EzWriteResult();

        try{
            DBCollection c = handler.db.getCollection(normalizeCollection(collection));

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(req.getQuery()));
            DBObject query = (DBObject)ois.readObject();

            appLog.debug("QUERY {}", query);

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbUpdateObject()));
            DBObject updateObject = (DBObject)ois.readObject();

            appLog.debug("UPDATE OBJECT {}", updateObject);

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getWriteConcern()));
            WriteConcern concern = (WriteConcern)ois.readObject();

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbEncoder()));
            DBEncoder encoder = (DBEncoder)ois.readObject();

            Boolean isDriverUnitTestMode = req.isIsUnitTestMode();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "update_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("query", handler.printMongoObject(query));
            auditParamsMap.put("updateObject", handler.printMongoObject(updateObject));
            auditParamsMap.put("concern", handler.printMongoObject(concern));
            auditParamsMap.put("encoder", handler.printMongoObject(encoder));
            handler.auditLog(token, AuditEventType.FileObjectModify, auditParamsMap);

            // assume it's a WRITE operation - then we need to iterate through the redacted results
            //   and see if they are updating the security fields - then it becomes MANAGE operation.
            QueryResultIterator qri = findHandler.convertFindForDriver(normalizeCollection(collection),
                    query, null, "", 0, 0, 0, null, token, EzMongoHandler.WRITE_OPERATION);

            final List<Object> idList = new ArrayList<Object>();
            while (qri.hasNext()){
                DBObject o = qri.next();
                if (!isDriverUnitTestMode) {
                    // also need to check if the user has the auths to update the record
                    try {
                        boolean isManageOperation = handler.getMongoUpdateHelper().isUpdatingSecurityFields(o, updateObject);
                        handler.getMongoInsertHelper().checkAbilityToInsert(token, null, updateObject, o, isManageOperation, true);
                        appLog.debug("can update DBObject (_id): {}", o.get("_id"));
                        idList.add(o.get("_id"));
                    } catch(ClassCastException | VisibilityParseException | EzMongoBaseException e) {
                        appLog.error(e.toString());
                        appLog.debug("User does not have the auths to update record: {}", o);
                    }
                } else {
                    appLog.debug("can update DBObject (_id): {}", o.get("_id"));
                    idList.add(o.get("_id"));
                }
            }

            final DBObject inClause = new BasicDBObject("$in", idList);
            final DBObject redactedQuery = new BasicDBObject("_id", inClause);

            WriteResult res = null;
            // only update the objects that were returned after performing the redact
            if (idList.size() > 0) {
                res = c.update(redactedQuery, updateObject, req.isUpsert(), req.isMulti(), concern, encoder);
            } else {
                throw new MongoException("Nothing to update, perhaps redact prohibited. " +
                        "Also note that upsert is not supported. If you used save() to make this call," +
                        " try using insert() instead is possible");
            }

            appLog.debug("WriteResult: {}", res);

            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            new ObjectOutputStream(bOut).writeObject(res);

            ewr.setWriteResult(bOut.toByteArray());
        } catch (MongoException e) {
            appLog.error(e.toString());
            addWriteResultException(ewr, e);
        } catch (Exception e) {
            appLog.error(e.toString());
            throw new TException(e);
        }
        return ewr;
    }

    public ResultsWrapper aggregate_driver(String collection, EzAggregationRequest ezAggregationRequest, EzSecurityToken token) throws TException, EzMongoDriverException {

        collection = appName + "_" + collection;

        appLog.debug("aggregate_driver() from collection: {}", collection); // + ezFindRequest);

        TokenUtils.validateSecurityToken(token, handler.getConfigurationProperties());
        ResultsWrapper rw = new ResultsWrapper();

        try {
            List<DBObject> pipeline = (List<DBObject>) new ObjectInputStream
                    (new ByteArrayInputStream(ezAggregationRequest.getPipeline())).readObject();
            for (DBObject p : pipeline) {
                Object o = p.get("$out");
                if (o != null) {
                    String dollarOut = appName + "_" + (String)o;
                    appLog.debug("newDollarOut: {}", dollarOut);
                    p.put("$out",dollarOut);
                    break;
                }
            }

            AggregationOptions options = (AggregationOptions) new ObjectInputStream
                    (new ByteArrayInputStream(ezAggregationRequest.getOptions())).readObject();
            ReadPreference readPref = (ReadPreference) new ObjectInputStream
                    (new ByteArrayInputStream(ezAggregationRequest.getReadPref())).readObject();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "aggregate_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("pipeline", handler.printMongoObject(pipeline));
            auditParamsMap.put("options", handler.printMongoObject(options));
            auditParamsMap.put("readPref", handler.printMongoObject(readPref));
            handler.auditLog(token, AuditEventType.FileObjectAccess, auditParamsMap);

            List<DBObject> mutablePipelineList = new ArrayList<DBObject>(pipeline);

            // apply $redact if necessary
            for(int i = 0; i < mutablePipelineList.size(); ) {
                DBObject pipelineCommand = mutablePipelineList.get(i++);
                // if there is a $match operator (are any others needed to check?), add a $redact operator right after it.
                if (pipelineCommand.get("$match") != null) {
                    DBObject redact = RedactHelper.createRedactOperator(handler.getMongoFindHelper().getSecurityExpressionViz(), token);
                    mutablePipelineList.add(i, redact);
                }
            }

            Cursor cursor = handler.db.getCollection(collection).aggregate(mutablePipelineList, options, readPref);
            // NOTE: the "response" is not set in the QueryResultIterator since the "aggregate" method
            // in DBCollectionImpl.java does not use the QRI constructor that sets the "response".
            // The QRI constructor calls "initFromCursorDocument".

            appLog.debug("cursor: {}", cursor);

            List<DBObject> results = null;
            if (cursor instanceof QueryResultIterator) {
                int originalHashcode = cursor.hashCode();
                ((QueryResultIterator) cursor).setOriginalHashCode(originalHashcode);

                handler.setResponseObjectWithCursor(rw, cursor);

                results = putIntoITmap((QueryResultIterator) cursor, originalHashcode);

            } else {
                handler.setResponseObjectWithCursor(rw, cursor);

                results = new ArrayList<DBObject>();
                while (cursor.hasNext()){
                    DBObject o = cursor.next();
                    appLog.debug("DBObject: {}", o);
                    results.add(o);
                }
            }

            rw.setResultSet(handler.addDBCursorResult(results).toByteArray());

        } catch (ClassNotFoundException e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        } catch (IOException e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }


        return rw;
    }

    public ResultsWrapper drop_driver(String collection, EzSecurityToken token) throws TException, EzMongoDriverException {
        if (!collection.equals("fs.files") && !collection.equals("fs.chunks")) {
            collection = appName + "_" + collection;
        }

        appLog.debug("drop_driver(): {}", collection);

        TokenUtils.validateSecurityToken(token, handler.getConfigurationProperties());
        ResultsWrapper rw = new ResultsWrapper();
        try {
            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "drop_driver");
            auditParamsMap.put("collectionName", collection);
            handler.auditLog(token, AuditEventType.FileObjectDelete, auditParamsMap);

            handler.db.getCollection(normalizeCollection(collection)).drop();
        } catch (MongoException e) {
            appLog.error(e.toString());
            addResultException(rw, e);
        } catch (Exception e) {
            appLog.error(e.toString());
            throw new TException(e);
        }
        return rw;
    }

    public EzWriteResult createIndex_driver(String collection, EzCreateIndexRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {
//        if (!collection.equals("fs.files")) {
            collection = appName + "_" + collection;
//        }

        appLog.debug("createIndex_driver(): {}", collection);

        TokenUtils.validateSecurityToken(token, handler.getConfigurationProperties());
        EzWriteResult ewr = new EzWriteResult();
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbObjectKeys()));
            DBObject keys = (DBObject)ois.readObject();

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbObjectOptions()));
            DBObject options = (DBObject)ois.readObject();

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbEncoder()));
            DBEncoder dbEncoder = (DBEncoder)ois.readObject();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "createIndex_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("keys", handler.printMongoObject(keys));
            auditParamsMap.put("options", handler.printMongoObject(options));
            auditParamsMap.put("dbEncoder", handler.printMongoObject(dbEncoder));
            handler.auditLog(token, AuditEventType.FileObjectCreate, auditParamsMap);


            if (options != null) {     // i know this can be in next block, will refactor later
                String ns = (String) options.get("ns");
                if (ns != null) {
                    //if (!collection.equals("fs.files")) {
                        String[] parts = StringUtils.split(ns, ".", 2);
                        String newNS = dbName + "." + appName +  "_" + parts[1];
                        appLog.debug("createIndex newNS: {}", newNS);
                        options.put("ns", newNS);
                    //}
                }
            }

            if (options != null) {
                handler.db.getCollection(normalizeCollection(collection)).createIndex(keys, options, dbEncoder);
            } else {
                handler.db.getCollection(normalizeCollection(collection)).createIndex(keys);
            }
        } catch (Exception e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }

        return ewr;
    }

    public EzGetMoreResponse getMore_driver(String collection, EzGetMoreRequest req, EzSecurityToken token) throws TException, EzMongoDriverException {

        appLog.debug("getMore_driver: {}", collection);

        TokenUtils.validateSecurityToken(token, handler.getConfigurationProperties());
        EzGetMoreResponse response = new EzGetMoreResponse();
        try{
            String hash = req.getQueryResultIteratorHashcode();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "getMore_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("hash", hash);
            handler.auditLog(token, AuditEventType.FileObjectAccess, auditParamsMap);

            Element element = cm.getCache(IT_MAP_CACHE).get(hash);
            QueryResultIterator qri = (QueryResultIterator)element.getObjectValue();

            if (qri == null)   {
                throw new Exception("Could not find QRI for hash: " + hash);
            }

            long curId = qri.getCursorId();
            int curSize = qri.getCurSize();
            List<DBObject> results = new ArrayList<DBObject>();
            int count = 0;
            boolean passBackCursorId = false;
            while (qri.hasNext()){
                count++;
                if (count <= curSize) {
                    DBObject o = qri.next();
                    appLog.debug("getMore() DBObject: {}", o);
                    results.add(o);
                } else {
                    appLog.debug("getMore() Dont get anymore for qri {}, let getmore() happen again",qri.getOriginalHashCode());
                    passBackCursorId = true;
                    break;
                }
            }

            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            new ObjectOutputStream(bOut).writeObject(results);
            response.setResultSet(bOut.toByteArray());

            Response r = qri.getResponse();
            if (r != null) {
                r.set_objects(results);
                if (passBackCursorId) {
                    r.setCursorId(curId);
                } else {
                    r.setCursorId(0);
                }
            } else {
                // do what?
            }

            response.setResponse(ser(r));

        } catch (Exception e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }
        return response;
    }

    public EzParallelScanResponse parallelScan_driver(String collection, EzParallelScanOptions options, EzSecurityToken token) throws EzMongoDriverException, TException {
        collection = appName + "_" + collection;

        appLog.debug("parallelScan_driver: collection = {}", collection);

        EzParallelScanResponse res = new EzParallelScanResponse();
        try {
            ParallelScanOptions opts = (ParallelScanOptions) new ObjectInputStream
                    (new ByteArrayInputStream(options.getOptions())).readObject();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "parallelScan_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("opts", handler.printMongoObject(opts));
            handler.auditLog(token, AuditEventType.FileObjectAccess, auditParamsMap);

            List<Cursor> curs  = handler.db.getCollection(normalizeCollection(collection)).parallelScan(opts);
            List<QueryResultIterator> qris = new ArrayList<QueryResultIterator>();
            Map<String,List<DBObject>> qriResultsMap = new HashMap<String, List<DBObject>>();
            for (Cursor c : curs) {
                if (c instanceof QueryResultIterator) {
                    QueryResultIterator qri = (QueryResultIterator)c;

                    // This qri will not have a Response object, only CursorDocument.

                    int originalHashcode = qri.hashCode();
                    qri.setOriginalHashCode(originalHashcode);
                    qris.add(qri);


                    List<DBObject> results = putIntoITmap(qri, originalHashcode);

                    qriResultsMap.put(""+originalHashcode,results);
                }
            }

            res.setMapOfIterators(ser(qriResultsMap));
            res.setListOfCursors(ser(qris));
            return res;
        } catch (Exception e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }
    }

    public EzWriteResult remove_driver(String collection, EzRemoveRequest req, EzSecurityToken token) throws EzMongoDriverException, TException {
//        if (!collection.equals("fs.chunks") && !collection.equals("fs.files")) {
            collection = appName + "_" + collection;
//        }

        appLog.debug("remove_driver() to collection: {}", collection);

        TokenUtils.validateSecurityToken(token, handler.getConfigurationProperties());
        EzWriteResult ewr = new EzWriteResult();
        try{
            DBCollection c = handler.db.getCollection(normalizeCollection(collection));

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbObjectQuery()));
            DBObject query = (DBObject)ois.readObject();

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getWriteConcern()));
            WriteConcern writeConcern = (WriteConcern)ois.readObject();

            ois = new ObjectInputStream(new ByteArrayInputStream(req.getDbEncoder()));
            DBEncoder dbEncoder = (DBEncoder)ois.readObject();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "remove_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("query", handler.printMongoObject(query));
            auditParamsMap.put("writeConcern", handler.printMongoObject(writeConcern));
            auditParamsMap.put("dbEncoder", handler.printMongoObject(dbEncoder));
            handler.auditLog(token, AuditEventType.FileObjectDelete, auditParamsMap);
            WriteResult res = c.remove(query, writeConcern, dbEncoder);

            appLog.debug("remove() WriteResult: {}", res);

            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            new ObjectOutputStream(bOut).writeObject(res);

            ewr.setWriteResult(bOut.toByteArray());
        } catch (Exception e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }
        return ewr;
    }

    public int getMaxBsonObjectSize_driver(EzSecurityToken token) throws EzMongoDriverException, TException {

        HashMap<String, String> auditParamsMap = new HashMap<>();
        auditParamsMap.put("action", "getMaxBsonObjectSize_driver");
        handler.auditLog(token, AuditEventType.FileObjectAccess, auditParamsMap);

        try {
            return handler.db.getMongo().getMaxBsonObjectSize();
        } catch (Exception e) {
            appLog.error(e.toString());
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }
    }

    protected void addResultException(ResultsWrapper rw, Exception ex) throws EzMongoDriverException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(bOut).writeObject(ex);
            rw.setMongoexception(bOut.toByteArray());
        } catch (Exception e) {
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }
    }

    protected void addWriteResultException(EzWriteResult rw, Exception ex) throws TException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(bOut).writeObject(ex);
            rw.setMongoexception(bOut.toByteArray());
        } catch (Exception e) {
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(ser(e));
            throw eme;
        }
    }

    protected byte[] ser(Object o) {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        ObjectOutput output = null;
        try {
            output = new ObjectOutputStream(bOut);
            output.writeObject(o);
        } catch (IOException e) {
            appLog.error(e.toString());
        }
        return bOut.toByteArray();
    }

    protected List<DBObject> putIntoITmap(QueryResultIterator qri, int originalHashcode) {
        long curId = qri.getCursorId();
        int curSize = qri.getCurSize();
        List<DBObject> results = new ArrayList<DBObject>();
        int count = 0;
        while (qri.hasNext()){
            count++;
            if (count <= curSize) {
                DBObject o = qri.next();
                appLog.debug("Next DBObject: {}", o);
                results.add(o);
            } else {
                appLog.debug("Dont get anymore for qri {}, let getmore() happen",qri.getOriginalHashCode());
                break;
            }
        }

        if (curId != 0) {
            cm.getCache(IT_MAP_CACHE).put(new Element("" + originalHashcode, qri));
        }
        return results;
    }

    protected boolean ifRequestIsForAGridFSObject(Object id) {
        if (id != null) {
            Element element = cm.getCache(GRID_FS_INPUT_FILE_MAP_CACHE).get(id.toString());
            ObjectId o = (ObjectId)element.getObjectValue();
            if (o != null) {
                if (o.toString().equals(id.toString())) {
                    return true;
                }
            }
        }
        return false;
    }
}
