package com.mongodb;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.data.mongo.driver.thrift.EzMongoDriverException;
import ezbake.data.mongo.driver.thrift.EzMongoDriverService;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import org.apache.thrift.TException;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

/**
 * Created by jagius on 6/17/14.
 */
public class EzDBTCPConnector extends DBTCPConnector {

    protected static ThriftClientPool pool;
    protected static EzbakeSecurityClient securityClient;
    EzConfiguration configuration = null;
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

    public EzDBTCPConnector(Mongo mongo) {
        super(mongo);
        createClient();
    }

    EzSecurityToken getToken() throws Exception {
        String tokenSource = System.getProperty("tokenSource", "userInfo");
        if (tokenSource.equals("userInfo")){
            return securityClient.fetchTokenForProxiedUser();
        } else {
           return securityClient.fetchAppToken();
        }
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     *
     * @return the maximum size, or 0 if not obtained from servers yet.
     */
    @Override
    public int getMaxBsonObjectSize() {

        EzMongoDriverService.Client client = null;
        try {
            client = getThriftClient();
            EzSecurityToken token = getToken();
            return client.getMaxBsonObjectSize_driver(token);
        }  catch (EzMongoDriverException e) {
            Object o = deser(e.getEx());
            if (o == null){
                throw new MongoException("Unknown Exception");
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
    }


    private Object deser(byte[] ex) {
        try {
            return new ObjectInputStream(new ByteArrayInputStream(ex)).readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
