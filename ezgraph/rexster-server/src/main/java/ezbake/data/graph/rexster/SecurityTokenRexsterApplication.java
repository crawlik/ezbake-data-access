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

package ezbake.data.graph.rexster;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.NotFoundException;
import com.tinkerpop.rexster.RexsterApplicationGraph;
import com.tinkerpop.rexster.server.AbstractMapRexsterApplication;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.graph.rexster.graphstore.GraphStore;
import ezbake.data.graph.rexster.graphstore.ManagedGraphStore;

/**
 * RexsterApplication that attempts to get a security token provided by the {@link ezbake.data.graph.rexster
 * .SecurityTokenSecurityFilter}. This security token is passed along to 'security token-wrapped' variants of the
 * standard Rexster classes and ultimately to a Blueprints graph wrapper that uses the security token to perform secure
 * operations. The security token is stored in a ThreadLocal in SecurityTokenSecurityFilter and assigned a value based
 * on the headers of incoming HTTP requests or the username value of RexPro requests.
 * <p/>
 * A provider of graphs can be specified by identifying a class implementing GraphStore in the config property {@link
 * ezbake.data.graph.rexster.SecurityTokenRexsterApplication#GRAPH_STORE_KEY}.  This graph store will be responsible for
 * retrieving graphs requesting by this RexsterApplication and identifying the names of available graphs for a given
 * request.
 */
public class SecurityTokenRexsterApplication extends AbstractMapRexsterApplication {

    /**
     * Specify the runtime class of {@link #graphStore}
     */
    public static final String GRAPH_STORE_KEY = "graph.store.class";

    private static final Logger logger = LoggerFactory.getLogger(SecurityTokenRexsterApplication.class);

    /**
     * Interface from which we will retrieve graphs. The runtime class can be specified via the {@link #GRAPH_STORE_KEY}
     * property.
     */
    private final GraphStore graphStore;

    /**
     * Constructs a new SecurityTokenRexsterApplication.  EzConfiguration will be used to search for properties to
     * configure this class with. The {@link #GRAPH_STORE_KEY} property will determine which GraphStore this class will
     * use.
     */
    public SecurityTokenRexsterApplication() {
        this(null);
    }

    /**
     * Constructs a new SecurityTokenRexsterApplication with the given properties or using EzConfiguration to find
     * properties if null. The {@link #GRAPH_STORE_KEY} property will be used to determine what GraphStore this class
     * should use.
     *
     * @param properties properties with which to configure this or null if EzConfiugration should be used to search
     * for properties.
     */
    @VisibleForTesting
    SecurityTokenRexsterApplication(Properties properties) {
        if (properties == null) {
            try {
                properties = new EzProperties(new EzConfiguration().getProperties(), true);
            } catch (final EzConfigurationLoaderException e) {
                final String errMsg = "Unable to get EzProperties, aborting startup.";
                logger.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        }

        final String storeName =
                properties.getProperty(GRAPH_STORE_KEY, ManagedGraphStore.class.getName());
        final Class clazz;
        try {
            logger.info("Attempting to initalize GraphStore from class {}", storeName);
            clazz = Class.forName(storeName);
        } catch (final ClassNotFoundException e) {
            final String errMsg = String.format("Unable to find class: %s.  Aborting startup.", storeName);
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }

        if (GraphStore.class.isAssignableFrom(clazz)) {
            try {
                graphStore = (GraphStore) clazz.newInstance();
            } catch (final Exception e) {
                final String errMsg = String.format("Error instantiating instance of GraphStore: %s!", storeName);
                logger.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        } else {
            final String errMsg = String.format(
                    "Property value for %s must refer to a class that implements GraphStore.", GRAPH_STORE_KEY);

            logger.error(errMsg);
            throw new RuntimeException(errMsg);
        }

        graphStore.initialize(properties);
    }

    /**
     * Gets a 'security token-wrapped' RexsterApplicationGraph that can provide accesses to a standard
     * RexsterApplicationGraph in a way secured using the security token.
     *
     * @param graphName The name of the graph to interact with.
     * @return The 'security token-wrapped' RexsterApplicationGraph specified by the graphName param.
     */
    @Override
    public RexsterApplicationGraph getApplicationGraph(final String graphName) {
        final EzSecurityToken token = SecurityTokenSecurityFilter.getEzBakeSecurityToken();

        if (token == null) {
            final String errMsg = String.format(
                    "Unable to get security token. Access to graph denied. Thread: %s, GraphName: %s",
                    Thread.currentThread().getName(), graphName);

            logger.error(errMsg);
            throw new SecurityException(errMsg);
        }

        final RexsterApplicationGraph rexsterGraph = graphStore.getApplicationGraph(graphName, token);

        if (rexsterGraph == null) {
            final String errMsg = String.format(
                    "Could not find graph '%s'. Must pass in name of already existing graph. ", graphName);

            logger.error(errMsg);
            throw new NotFoundException(errMsg);
        }

        return new SecurityTokenRexsterApplicationGraphWrapper(rexsterGraph, token);
    }

    @Override
    public Set<String> getGraphNames() {
        final EzSecurityToken token = SecurityTokenSecurityFilter.getEzBakeSecurityToken();

        //TODO: this currently does not work with RexPro.  Every script request calls getGraphNames and script requests
        // are not guaranteed to have a token in their context.
        //        if (token == null) {
        //            final String errMsg = "Unable to get security token, cannot list available graphs!";
        //            logger.error(errMsg);
        //            throw new SecurityException(errMsg);
        //        }

        return graphStore.getGraphNames(token);
    }
}
