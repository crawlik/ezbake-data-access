/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.WriteResult;

/**
 * The tutorial from http://www.mongodb.org/display/DOCS/Java+Tutorial.
 */
public class QuickTour {
    // CHECKSTYLE:OFF
    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes no args
     * @throws UnknownHostException if it cannot connect to a MongoDB instance at localhost:27017
     */
    public static void main(final String[] args) throws UnknownHostException {
        // connect to the local database server
        MongoClient mongoClient = new MongoClient("192.168.50.104");

        // get handle to "mydb"
        DB db = mongoClient.getDB("testapp");

        // Authenticate - optional
        //boolean auth = db.authenticate("foo", "bar".toCharArray());


        // get a list of the collections in this database and print them out
        Set<String> collectionNames = db.getCollectionNames();
        for (final String s : collectionNames) {
            System.out.println("collection name: " + s);
        }

        // get a collection object to work with
        DBCollection testCollection = db.getCollection("testCollection2");

        // drop all the data in it
        testCollection.drop();

        // make a document and insert it
        BasicDBObject doc = new BasicDBObject("name", "MongoDB").append("type", "database")
                                                                .append("count", 1)
                                                                .append("info", new BasicDBObject("x", 203).append("y", 102));

        WriteResult wr = testCollection.insert(doc);
        System.out.println("wr: " + wr);

        // get it (since it's the only one in there since we dropped the rest earlier on)
        DBObject myDoc = testCollection.findOne();
        System.out.println("myDoc: " + myDoc);

        // update the doc
        wr = testCollection.update
                (new BasicDBObject("info.x", 203),
                        new BasicDBObject("y", new BasicDBObject("name", "MongoDB").append("type", "database")
                        .append("count", 1).append("info", new BasicDBObject("x", 203).append("y", 202))));
        System.out.println("update wr: " + wr);

        // get updated doc (since it's the only one in there since we dropped the rest earlier on)
        myDoc = testCollection.findOne();
        System.out.println("updated myDoc: " + myDoc);

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        for (int i = 0; i < 300; i++) {
        	BasicDBObject newDBObj = new BasicDBObject().append("i", i);
        	BasicDBList slOuterList = new BasicDBList();
        	BasicDBList slInnerList = new BasicDBList();
        	
        	if (i % 2 == 0) {
        		slInnerList.add("CANT_READ");
        	} else {
        		slInnerList.add("U");
        	}
        	
        	slOuterList.add(slInnerList);
        	newDBObj.append("sl", slOuterList);
        	
            testCollection.insert(newDBObj);
        }
        System.out.println("total # of documents after inserting 100 small ones (should be 101) " + testCollection.getCount());

        DBCursor cursor = null;
        BasicDBObject query = null;

        // lets get all the documents in the collection and print them out
        cursor = testCollection.find();
        try {
            while (cursor.hasNext()) {
                System.out.println("Next: " + cursor.next());
            }
        } finally {
            cursor.close();
        }

        // now use a query to get 1 document out
        query = new BasicDBObject("i", 71);
        cursor = testCollection.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println("Next from query: " + cursor.next());
            }
        } finally {
            cursor.close();
        }

        // now use a range query to get a larger subset
        query = new BasicDBObject("i", new BasicDBObject("$gt", 50));  // i.e. find all where i > 50
        cursor = testCollection.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println("Next from range(>50) query:  " + cursor.next());
            }
        } finally {
            cursor.close();
        }

        // range query with multiple constraints
        query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte", 30));  // i.e.   20 < i <= 30
        cursor = testCollection.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println("Next from range(>20 <=30) query:  " + cursor.next());
            }
        } finally {
            cursor.close();
        }

        // create an index on the "i" field
        testCollection.createIndex(new BasicDBObject("i", 1));  // create index on "i", ascending

        // list the indexes on the collection
        List<DBObject> list = testCollection.getIndexInfo();
        for (final DBObject o : list) {
            System.out.println("index: " + o);
        }

        // try Aggregation
        // create our pipeline operations, first with the $match
        DBObject match = new BasicDBObject("$match", new BasicDBObject("i", new BasicDBObject("$gt", 90)));

        // build the $projection operation
        DBObject fields = new BasicDBObject("i", 1);
        fields.put("_id", 0);
        DBObject project = new BasicDBObject("$project", fields );

        // run aggregation
        List<DBObject> pipeline = Arrays.asList(match, project);
        AggregationOptions aggregationOptions = AggregationOptions.builder().build();
        ReadPreference readPref = ReadPreference.primary();
        Cursor aggCursor = testCollection.aggregate(pipeline, aggregationOptions, readPref);
        while (aggCursor.hasNext()) {
        	DBObject o = aggCursor.next();
            System.out.println("Aggregation DBObject: " + o);
        }

        // See if the last operation had an error
        System.out.println("Last error : " + db.getLastError());

        // see if any previous operation had an error
        System.out.println("Previous error : " + db.getPreviousError());

        // force an error
        db.forceError();

        // See if the last operation had an error
        System.out.println("Last error after force error : " + db.getLastError());

        db.resetError();

        // release resources
        mongoClient.close();
        
        System.exit(0);
    }
    // CHECKSTYLE:ON
}
