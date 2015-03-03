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

package ezbake.data.common;

import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.base.thrift.EzBakeBaseService;
import ezbake.ezdiscovery.ServiceDiscoveryClient;

public class ThriftClient {

    private static final Logger logger = LoggerFactory.getLogger(ThriftClient.class);
    private static EzBakeBaseService.Client client = null;
    private static String clientClassName = "";
    private static String DATASET = "";

    TTransport transport = null;

    public static void create(String serviceClientClassName, String service) {
        clientClassName = serviceClientClassName;
        DATASET = service;
    }

    private ThriftClient(String zookeeper, String appName) throws Exception {
        logger.info("Initializing Thrift client..." + zookeeper + "," + appName);
        try {
            client = initThrift(zookeeper, appName);
            if (client != null) {
                logger.info("Thrift client ready");
            }
        } catch (final Exception e) {
            client = null;
            logger.error(e.getMessage());
        }
    }

    @SuppressWarnings("unused")
    public static EzBakeBaseService.Client getInstance() throws Exception {
        if (client == null) {
            new ThriftClient(System.getProperty("zookeeper"), System.getProperty("appname"));
        }

        return client;
    }

    // Marked as final to prevent overriden method calls in the constructor
    final EzBakeBaseService.Client initThrift(String zookeeper, String appName) throws Exception {
        List<String> endpoints = null;
        try (ServiceDiscoveryClient serviceDiscoveryClient = new ServiceDiscoveryClient(zookeeper)) {
            logger.info("Connected for service discovery.  Attempting to get endpoints for appName: '" + appName
                    + "', and dataset: '" + DATASET + "'");

            try {
                endpoints = serviceDiscoveryClient.getEndpoints(appName, DATASET);
            } catch (final Exception ex) {
                final String message =
                        "An error occurred during service discovery: serviceDiscoveryClient.getEndpoints(...)";
                logger.error(message, ex);
                throw new Exception(message, ex);
            }
        }

        if (endpoints == null || endpoints.size() < 1) {
            throw new Exception("Service discovery returned no endpoints for " + appName);
        }

        for (final String enpoint : endpoints) {
            final String host = enpoint.split(":")[0];
            final String port = enpoint.split(":")[1];
            logger.info("Trying to connect to " + host + " on " + port);

            if (transport != null) {
                transport.close();
                transport = null;
            }

            transport = new TSocket(host, Integer.parseInt(port));
            try {
                transport.open();
            } catch (final TTransportException ex) {
                final String message = "An error occurred opening the TSocket to host: " + enpoint;
                logger.error(message, ex);
                transport = null;
                continue;
            }

            if (transport != null) {
                break;
            }

        }

        if (transport != null) {
            try {
                TBinaryProtocol tbp = null;
                final Class<?> cl1 = Class.forName("org.apache.thrift.protocol.TBinaryProtocol");

                if (cl1 != null) {
                    final Constructor<?> cons1 = cl1.getConstructor(TTransport.class);
                    if (cons1 != null) {
                        tbp = (TBinaryProtocol) cons1.newInstance(transport);
                    }
                }

                if (tbp != null) {
                    final Class<?> cl = Class.forName(clientClassName);
                    if (cl1 != null) {
                        final Constructor<?> cons = cl.getConstructor(TProtocol.class);
                        if (cons != null) {
                            client = (EzBakeBaseService.Client) cons.newInstance(tbp);
                        }
                    }
                }

            } catch (final ClassNotFoundException e) {
                logger.error("Class Not Found Exception with Message : " + e.getMessage());
            }
        }

        return client;

    }

    @Override
    public String toString() {
        return "ThriftClient{" + "client=" + client + '}';
    }

    public static void close() {
        if (client != null) {
            client = null;
        }

    }

}
