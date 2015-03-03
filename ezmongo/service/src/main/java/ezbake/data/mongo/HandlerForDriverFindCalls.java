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
import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.common.TokenUtils;
import ezbake.data.mongo.driver.thrift.EzFindRequest;
import ezbake.data.mongo.driver.thrift.EzMongoDriverException;
import ezbake.data.mongo.driver.thrift.ResultsWrapper;
import ezbake.util.AuditEventType;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

/**
 * Created by jagius on 8/7/14.
 */
public class HandlerForDriverFindCalls {

    private final Logger appLog = LoggerFactory.getLogger(HandlerForDriverFindCalls.class);

    final private MongoDriverHandler parent_handler;
    final private String appName, dbName;
    ConcurrentHashMap<String,QueryResultIterator> itMap = null;
    ConcurrentHashMap<String, ObjectId> gridFSInputFileMap = null;

    protected HandlerForDriverFindCalls(MongoDriverHandler handler){
        this.parent_handler = handler;
        this.appName = parent_handler.handler.appName;
        this.dbName = parent_handler.handler.dbName;
        itMap = new ConcurrentHashMap<String, QueryResultIterator>();
        gridFSInputFileMap = new ConcurrentHashMap<String, ObjectId>();
    }

    private void checkNonSupportedQueries(DBObject query, String collection) {
        if (query.get("$where") != null) {
            throw new MongoException("$where operator not supported at this time since redact could not be enforced");
        }
    }

    public ResultsWrapper find_driver(String collection, EzFindRequest ezFindRequest, EzSecurityToken token) throws TException, EzMongoDriverException {
        if (!collection.equals("$cmd") && !collection.equals("system.indexes") &&
                // !collection.equals("fs.chunks") && !collection.equals("fs.files") &&
                !collection.equals("system.namespaces")) {
            collection = appName + "_" + collection;
        }

        appLog.info("find_driver() from collection: {}", collection);

        TokenUtils.validateSecurityToken(token, parent_handler.handler.getConfigurationProperties());
        ResultsWrapper rw = new ResultsWrapper();
        try {
            DBObject ref = (DBObject) new ObjectInputStream
                    (new ByteArrayInputStream(ezFindRequest.getRef())).readObject();

            checkNonSupportedQueries(ref,collection);

            DBObject fields = (DBObject) new ObjectInputStream
                    (new ByteArrayInputStream(ezFindRequest.getFields())).readObject();

            appLog.info("find_driver() ref: {}", ref);
            appLog.info("find_driver() fields: {}", fields);

            String originalDatabase = null;
            boolean isSysCommand = false;
            if (collection.equals("system.namespaces")){
                isSysCommand = true;
            }
            if (collection.equals("system.indexes")) {
                isSysCommand = true;
                String ns = (String)ref.get("ns");
                if (ns != null) {
                    String[] parts = StringUtils.split(ns, ".", 2);
                    originalDatabase = parts[0];
                    String newNS = dbName + "." + appName +  "_" + parts[1];
                    appLog.info("system.indexes newNS: {}", newNS);
                    ref.put("ns", newNS);
                }
            }

            if (collection.equals("$cmd")) {
                isSysCommand = true;
                String eval = (String)ref.get("$eval");
                if (eval != null) {
                    throw new MongoException("Eval() not supported in ezmongo");
                }

                String geoNear = (String)ref.get("geoNear");
                if (geoNear != null){
                    geoNear = appName + "_" + geoNear;
                    appLog.info("newgeoNear: {}", geoNear);
                    ref.put("geoNear", geoNear);
                }

                String collstats = (String)ref.get("collstats");
                if (collstats != null){
                    collstats = appName + "_" + collstats;
                    appLog.info("newcollstats: {}", collstats);
                    ref.put("collstats", collstats);
                }

                String create = (String)ref.get("create");
                if (create != null){
                    create = appName + "_" + create;
                    appLog.info("newcreate: {}", create);
                    ref.put("create", create);
                }

                String deleteIndexes = (String)ref.get("deleteIndexes");
                if (deleteIndexes != null) {
                    String newDeleteIndexes = appName + "_" + deleteIndexes;
                    appLog.info("newDeleteIndexes: {}", newDeleteIndexes);
                    ref.put("deleteIndexes", newDeleteIndexes);
                }

                String distinct = (String)ref.get("distinct");
                if (distinct != null){
                    String newDistinct = appName + "_" + distinct;
                    appLog.info("newDistinct: {}", newDistinct);
                    ref.put("distinct", newDistinct);
                }

                String count = (String)ref.get("count");
                if (count != null){
                    String newCount = appName + "_" + count;
                    appLog.info("newCount: {}", newCount);
                    ref.put("count", newCount);
                }

                String findandmodify = (String)ref.get("findandmodify");
                if (findandmodify != null){
                    String newFindandmodify = appName + "_" + findandmodify;
                    appLog.info("newFindandmodify: {}", newFindandmodify);
                    ref.put("findandmodify", newFindandmodify);
                }

                String aggregate = (String)ref.get("aggregate");
                if (aggregate != null){
                    aggregate = appName + "_" + aggregate;
                    appLog.info("newaggregate: {}", aggregate);
                    ref.put("aggregate", aggregate);

                    List<DBObject> pipeline = (List)ref.get("pipeline");
                    if (pipeline != null){
                        appLog.info("pipeline: {}", pipeline);
                        for (DBObject o : pipeline) {
                            String out = (String)o.get("$out");
                            if (out != null) {
                                appLog.info("NEED to convert: {}", out);
                                out = appName + "_" + out;
                                o.put("$out",out);
                            }
                        }
                    }
                }

                String mapreduce = (String)ref.get("mapreduce");
                if (mapreduce != null){
                    mapreduce = appName + "_" + mapreduce;
                    appLog.info("newmapreduce: {}", mapreduce);
                    ref.put("mapreduce", mapreduce);

                    DBObject out = (DBObject)ref.get("out");
                    if (out != null){
                        String replace = (String)out.get("replace");
                        if (replace != null) {
                            replace = appName + "_" + replace;
                            out.put("replace", replace);
                            ref.put("out", out);
                        }
                    }

                    appLog.info("newref: " + ref);
                }

                DBObject group = (DBObject)ref.get("group");
                if (group != null){
                    String ns = (String)group.get("ns");
                    if (ns != null) {
                        String newNS = appName + "_" + ns;
                        appLog.info("group newNS: {}", newNS);
                        group.put("ns", newNS);
                        ref.put("group", group);
                    }
                }
            }


            int batchSize = ezFindRequest.getBatchSize();
            int limit = ezFindRequest.getLimit();
            int options = ezFindRequest.getOptions();
            int numToSkip = ezFindRequest.getNumToSkip();

            ReadPreference readPref = (ReadPreference) new ObjectInputStream
                    (new ByteArrayInputStream(ezFindRequest.getReadPref())).readObject();

            DBDecoder decoder = (DBDecoder) new ObjectInputStream
                    (new ByteArrayInputStream(ezFindRequest.getDecoder())).readObject();

            HashMap<String, String> auditParamsMap = new HashMap<>();
            auditParamsMap.put("action", "find_driver");
            auditParamsMap.put("collectionName", collection);
            auditParamsMap.put("ref", parent_handler.handler.printMongoObject(ref));
            auditParamsMap.put("fields", parent_handler.handler.printMongoObject(fields));
            auditParamsMap.put("readPref", parent_handler.handler.printMongoObject(readPref));
            auditParamsMap.put("batchSize", String.valueOf(batchSize));
            auditParamsMap.put("limit", String.valueOf(limit));
            auditParamsMap.put("options", String.valueOf(options));
            auditParamsMap.put("numToSkip", String.valueOf(numToSkip));
            parent_handler.handler.auditLog(token, AuditEventType.FileObjectAccess, auditParamsMap);

            QueryResultIterator qri = null;
            if ((isSysCommand || ref.get("$explain") != null) && ref.get("distinct") == null) {
                if (ezFindRequest.isSetEncoder()) {
                    DBEncoder encoder = (DBEncoder) new ObjectInputStream
                            (new ByteArrayInputStream(ezFindRequest.getEncoder())).readObject();
                    qri = parent_handler.handler.db.getCollection(parent_handler.normalizeCollection(collection)).find
                            (ref, fields, numToSkip, batchSize, limit, options, readPref, decoder, encoder);
                } else {
                    qri = parent_handler.handler.db.getCollection(parent_handler.normalizeCollection(collection))
                            .find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder);
                }
            }  else {
                // must convert to agg pipline in order to perform redact
                qri = convertFindForDriver(parent_handler.normalizeCollection(collection), ref, fields, "",
                        numToSkip, limit, batchSize, readPref, token, EzMongoHandler.READ_OPERATION);
            }

            String mapreduce = (String)ref.get("mapreduce");
            if (mapreduce != null){
                Response r = qri.getResponse();
                List<DBObject> obs = r.get_objects();
                if (obs.size() == 1){
                    DBObject o = obs.get(0);
                    String resultTable = (String)o.get("result");
                    resultTable = StringUtils.replace(resultTable, appName + "_","", 1);
                    appLog.info("find() Replaced map reduce result table name: {}", resultTable);
                    o.put("result",resultTable);
                }
            }

            if (collection.equals("system.namespaces")){
                appLog.info("find() reverting collection names from system.namespaces");
                Response r = qri.getResponse();
                List<DBObject> obs = r.get_objects();
                for (DBObject o : obs){
                    String name = (String)o.get("name");
                    if (!name.equals(dbName + "." + "system.indexes")){
                        String newName = StringUtils.replace(name,appName+"_","",1);
                        o.put("name",newName);
                    }
                }
                appLog.info("reverted obs: {}", obs);
            }

            int originalHashcode = qri.hashCode();

            qri.setOriginalHashCode(originalHashcode);

            parent_handler.handler.setResponseObjectWithCursor(rw, qri);

            appLog.info("QueryResultIterator hashcode: {}", qri.hashCode());

            List<DBObject> results = parent_handler.putIntoITmap(qri, originalHashcode);

//            if (parent_handler.ifRequestIsForAGridFSObject(ref.get("_id"))){
//                GridFS _fs = new GridFS(parent_handler.handler.mongoTemplate.getDb());
//                if (ref.get("_id") != null) {
//                    GridFSDBFile file = _fs.findOne(new BasicDBObject("_id", ref.get("_id")));
//                    rw.setResultSet(parent_handler.ser(file));
//                } else {
//                    List<GridFSDBFile> files = _fs.find(ref, fields);
//                    rw.setResultSet(parent_handler.ser(files));
//                }
//            } else {
                rw.setResultSet(parent_handler.handler.addDBCursorResult(results).toByteArray());
//            }

        } catch (Exception e) {
            e.printStackTrace();
            EzMongoDriverException eme =  new EzMongoDriverException();
            eme.setEx(parent_handler.ser(e));
            throw eme;
        }

        return rw;
    }

    protected QueryResultIterator convertFindForDriver(String collectionName, DBObject jsonQuery, DBObject projection, String jsonSort,
                                                     int skip, int limit, int batchSize, ReadPreference readPref, EzSecurityToken token, String operationType) throws Exception {

        appLog.info("convertFindForDriver() query: " + jsonQuery);

        AggregationOptions opts = null;
        if (batchSize > 0) {
            opts = AggregationOptions.builder().outputMode
                    (AggregationOptions.OutputMode.CURSOR).batchSize(batchSize).build();
        } else {
            opts = AggregationOptions.builder().build();
        }

        Object distinct = jsonQuery.get("distinct");
        Object key = null;
        if (distinct != null) {
            key = jsonQuery.get("key");

            Object q = jsonQuery.get("query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }

        jsonQuery = checkForQueryComment(jsonQuery);
        jsonQuery = checkForshowDiskLoc(jsonQuery);

        Object returnKey = jsonQuery.get("$returnKey");
        if (returnKey != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }

        Object snapshot = jsonQuery.get("$snapshot");
        if (snapshot != null) {
            Object ob = jsonQuery.get("$orderby");
            if (ob != null) {
                throw new MongoException("Do not use $snapshot with cursor.hint() and cursor.sort() methods");
            }
            Object hint = jsonQuery.get("$hint");
            if (hint != null) {
                throw new MongoException("Do not use $snapshot with cursor.hint() and cursor.sort() methods");
            }
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }

        Object explain = jsonQuery.get("$explain");
        if (explain != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }

        Object orderby = jsonQuery.get("$orderby");
        if (orderby != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
            jsonSort = orderby.toString();
        }

        Object maxScan = jsonQuery.get("$maxScan");
        if (maxScan != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
            limit = (Integer)maxScan;
        }

        Object min = jsonQuery.get("$min");
        if (min != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }

        Object max = jsonQuery.get("$max");
        if (max != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }

        QueryResultIterator qri = null;
        DBObject query = null;
        if (jsonQuery != null && jsonQuery.keySet().size() > 0) {
            query = new BasicDBObject("$match", jsonQuery);
        }

        DBObject[] additionalOps =
                parent_handler.handler.getMongoFindHelper().getFindAggregationCommandsArray(skip, limit,
                        (projection != null && projection.keySet().size() > 0) ? projection.toString() : "",
                        jsonSort, token, operationType);

        List<DBObject> pipeline = new ArrayList<DBObject>();
        if (query != null) {
            pipeline.add(query);
        }

        Collections.addAll(pipeline, additionalOps);

        appLog.info("convertFindForDriver() final pipeline query: " + pipeline);

        Cursor cursor = null;
        if (distinct != null) {
            qri = handleDistinctCall(jsonQuery, readPref, token, opts, distinct, key, pipeline);
        } else if (max != null && min != null){
            // TODO can max AND min be possible? investigate...
        } else if (max != null) {
            qri = handleMaxCall(collectionName, max, jsonQuery, readPref, token, opts, pipeline);
        } else if (min != null) {
            qri = handleMinCall(collectionName, min, jsonQuery, readPref, token, opts, pipeline);
        } else {
            cursor = parent_handler.handler.db.getCollection(collectionName).aggregate(pipeline, opts, readPref);
            if (cursor instanceof QueryResultIterator) {
                qri = (QueryResultIterator)cursor;
            } else {
                appLog.info("UNKNOWN CURSOR RETURNED: {}" , cursor.toString());
                throw new Exception("Find converted to Aggregate pipeline did not return a QueryResultIterator: " + cursor.toString());
            }
        }

        return qri;
    }

    // redact doesnt support $comment, but we still need to parse out the query obect
    private DBObject checkForQueryComment(DBObject jsonQuery) {
        if (jsonQuery.get("$comment") != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }
        return jsonQuery;
    }

    // redact doesnt support $showDiskLoc, but we still need to parse out the query obect
    private DBObject checkForshowDiskLoc(DBObject jsonQuery) {
        if (jsonQuery.get("$showDiskLoc") != null) {
            Object q = jsonQuery.get("$query");
            if (q != null) {
                jsonQuery = (DBObject)q;
            }
        }
        return jsonQuery;
    }

    private QueryResultIterator handleMinCall(String collection , Object min, DBObject jsonQuery, ReadPreference readPref,
                                              EzSecurityToken token, AggregationOptions opts,
                                              List<DBObject> pipeline) throws Exception {
        Cursor cursor;
        QueryResultIterator qri;// make a agg call to get a QueryResultIterator instance

        // add $out to pipeline
        String outCollection = "min_out_" + randomAlphanumeric(8);
        pipeline.add(new BasicDBObject("$out",outCollection));
        cursor = parent_handler.handler.db.getCollection(collection).aggregateWithOut(pipeline, opts, readPref);
        if (cursor instanceof QueryResultIterator) {
            qri = (QueryResultIterator)cursor;
        } else {
            appLog.info("UNKNOWN CURSOR RETURNED FOR handleMinCall agg call: {}" , cursor.toString());
            throw new Exception("$min converted to Aggregate pipeline did not return a QueryResultIterator: " + cursor.toString());
        }

        for (String key : ((DBObject) min).keySet()) {
            // create the index for the keys
            parent_handler.handler.db.getCollection(outCollection).createIndex(new BasicDBObject(key, 1));
        }

        cursor = new DBCursor(parent_handler.handler.db.getCollection(outCollection),
                new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                .addSpecial("$min", min);

        List<DBObject> l = new ArrayList<>();
        while (cursor.hasNext()) {
            DBObject o = cursor.next();
            appLog.info("o {}", o);
            l.add(o);
        }

        qri.setIterator(l.iterator());
        qri.setCurSize(l.size());

        // drop the tmp output collection
        parent_handler.handler.db.getCollection(outCollection).drop();
        return qri;
    }

    private QueryResultIterator handleMaxCall(String collection , Object max, DBObject jsonQuery, ReadPreference readPref,
                                                    EzSecurityToken token, AggregationOptions opts,
                                                    List<DBObject> pipeline) throws Exception {
        Cursor cursor;
        QueryResultIterator qri;// make a agg call to get a QueryResultIterator instance

        // add $out to pipeline
        String outCollection = "max_out_" + randomAlphanumeric(8);
        pipeline.add(new BasicDBObject("$out",outCollection));
        cursor = parent_handler.handler.db.getCollection(collection).aggregateWithOut(pipeline, opts, readPref);
        if (cursor instanceof QueryResultIterator) {
            qri = (QueryResultIterator)cursor;
        } else {
            appLog.info("UNKNOWN CURSOR RETURNED FOR handleMaxCall agg call: {}" , cursor.toString());
            throw new Exception("$max converted to Aggregate pipeline did not return a QueryResultIterator: " + cursor.toString());
        }

        for (String key : ((DBObject) max).keySet()) {
            // create the index for the keys
            parent_handler.handler.db.getCollection(outCollection).createIndex(new BasicDBObject(key, 1));
        }

        cursor = new DBCursor(parent_handler.handler.db.getCollection(outCollection),
                new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                .addSpecial("$max", max);

        List<DBObject> l = new ArrayList<>();
        while (cursor.hasNext()) {
            DBObject o = cursor.next();
            l.add(o);
        }

        qri.setIterator(l.iterator());
        qri.setCurSize(l.size());

        // drop the tmp output collection
        parent_handler.handler.db.getCollection(outCollection).drop();
        return qri;
    }

    private QueryResultIterator handleDistinctCall(DBObject jsonQuery, ReadPreference readPref,
                                    EzSecurityToken token, AggregationOptions opts, Object distinct,
                                    Object key, List<DBObject> pipeline) throws Exception {
        Cursor cursor;
        QueryResultIterator qri;// make a agg call to get a QueryResultIterator instance
        cursor = parent_handler.handler.db.getCollection(distinct.toString()).aggregate(pipeline, opts, readPref);
        if (cursor instanceof QueryResultIterator) {
            qri = (QueryResultIterator)cursor;
        } else {
            appLog.info("UNKNOWN CURSOR RETURNED FOR distinct: {}" , cursor.toString());
            throw new Exception("distinct converted to Aggregate pipeline did not return a QueryResultIterator: " + cursor.toString());
        }

        // add $out to pipeline and call it again
        String outCollectionForDistinct = "out_" + randomAlphanumeric(8);
        pipeline.add(new BasicDBObject("$out",outCollectionForDistinct));
        DBCursor c = (DBCursor)parent_handler.handler.db.getCollection(distinct.toString()).aggregate(pipeline, opts, readPref);

        // call distinct on previously redacted result collection
        CommandResult cr = parent_handler.handler.db.getCollection(outCollectionForDistinct).distinct(key.toString(),jsonQuery,true);

        // assemble results as per the driver paradigm
        List<DBObject> l = new ArrayList<>();
        BasicDBObject obj = new BasicDBObject();
        obj.put("values",(cr.get("values")));
        obj.put("stats",(cr.get("stats")));
        obj.put("ok",(cr.get("ok")));

        l.add(obj);
        qri.setIterator(l.iterator());
        qri.setCurSize(1);

        List<Integer> sizes = new ArrayList<>();
        sizes.add(1);
        qri.set_sizes(sizes);

        // drop the tmp output collection
        parent_handler.handler.db.getCollection(outCollectionForDistinct).drop();
        return qri;
    }


}
