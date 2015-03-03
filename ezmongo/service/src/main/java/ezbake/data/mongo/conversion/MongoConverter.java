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

package ezbake.data.mongo.conversion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * 
 * @author blong
 */
public class MongoConverter {

    private static Logger logger = LoggerFactory.getLogger(MongoConverter.class);

    public final static DBObject toDBObject(final String object) throws JSONException {
        return toDBObject(object, false);
    }
    
    public static <T extends Comparable<? super T>> List<T> asSortedList(Collection<T> c) {
      List<T> list = new ArrayList<T>(c);
      java.util.Collections.sort(list);
      return list;
    }

    /**
     * Convert a string JS/JSON object to a Mongo DBObject.
     * 
     * @param object A JSON/JavaScript object.
     * @param isJson Boolean flag to indicate whether the <code>object</code> passed is known to be valid JSON. If a
     *        client is unsure, or is passing a JavaScript object, we will deserialize the <code>object</code> into a
     *        JavaScript object first and then serializse the constructed JavaScript object to JSON before attempting
     *        the conversion to a Mongo DBObject.
     * @return A Mongo DBObject instance ("A key-value map that can be saved to the database")
     * @throws JSONException If decoding/encoding the JSON fails.
     */
    public final static DBObject toDBObject(final String object, boolean isJson) throws JSONException {
        String jsonString;
        if (isJson) {
            logger.debug("Assuming the String object provided is properly "
                    + "encoded and can be parsed via Mongo's JSON.parse(...) method");
            jsonString = object;
        } else {
            logger.debug("Assuming the String object provided is a JS object "
                    + "and will not parse via Mongo's JSON.parse(...) method");
            logger.debug("Original String object: \n" + object);

            // Convert a javascript object literal to a Java object
            // NOTE: This Type and Constructor are non obvious, but the
            // constructor is parsing a JavaScript object literal, NOT
            // a JSON string.
            final JSONObject objectFromString = new JSONObject(object);
            // Here we convert the java object to a valid JSON String
            jsonString = objectFromString.toString();

            logger.debug("Updated JSON String for parsing: \n" + jsonString);
        }

        // Parse the JSON string into a Mongo DBObject
        final DBObject dbObject = (DBObject) JSON.parse(jsonString);
        return dbObject;
    }
}
