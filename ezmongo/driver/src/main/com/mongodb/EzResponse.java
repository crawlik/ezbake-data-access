package com.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jagius on 6/4/14.
 */
public class EzResponse extends Response {
/*
    EzResponse(List<DBObject> objects){
        super();
        addDBObjects(objects);
    }
*/

    EzResponse(ServerAddress addr, DBCollection collection, InputStream in, DBDecoder decoder) throws IOException {
        super(addr, collection, in, decoder);
    }

    void addDBObjects(List<DBObject> objs){
        for (DBObject o : objs) {
            _objects.add(o);
        }
    }
}
