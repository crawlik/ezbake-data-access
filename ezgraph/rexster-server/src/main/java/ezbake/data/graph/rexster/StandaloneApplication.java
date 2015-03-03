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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.log4j.Logger;

import com.tinkerpop.rexster.protocol.EngineConfiguration;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.server.HttpRexsterServer;
import com.tinkerpop.rexster.server.RexProRexsterServer;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterProperties;
import com.tinkerpop.rexster.server.RexsterServer;
import com.tinkerpop.rexster.server.RexsterSettings;
import com.tinkerpop.rexster.server.ShutdownManager;

import ezbake.configuration.EzConfiguration;

// Copy-pasted from com.tinkerpop.rexster.Application. We should copy-paste the rest of it too and just change the
// parts of ours that we need to be more EzBake-like.

public class StandaloneApplication {
    private static final Logger logger = Logger.getLogger(StandaloneApplication.class);

    private final RexsterServer httpServer;
    private final RexsterServer rexproServer;
    private final RexsterApplication rexsterApplication;
    private final RexsterProperties properties;

    public StandaloneApplication(RexsterProperties properties) {
        this.properties = properties;
        this.httpServer = new HttpRexsterServer(properties);
        this.rexproServer = new RexProRexsterServer(properties, true);
        this.rexsterApplication = new SecurityTokenRexsterApplication();
    }

    public void start() throws Exception {
        final List<EngineConfiguration> configuredScriptEngines = new ArrayList<EngineConfiguration>();
        final List<HierarchicalConfiguration> configs = this.properties.getScriptEngines();
        for(HierarchicalConfiguration config : configs) {
            configuredScriptEngines.add(new EngineConfiguration(config));
        }

        EngineController.configure(configuredScriptEngines);

        this.httpServer.start(this.rexsterApplication);
        this.rexproServer.start(this.rexsterApplication);

        startShutdownManager(properties);
    }

    public void stop() {
        try {
            this.httpServer.stop();
        } catch (Exception ex) {
        }

        try {
            this.rexproServer.stop();
        } catch (Exception ex) {
        }

        try {
            this.rexsterApplication.stop();
        } catch (Exception ex) {
        }
    }

    private void startShutdownManager(final RexsterProperties properties) throws Exception {
        final ShutdownManager shutdownManager = new ShutdownManager(properties);

        //Register a shutdown hook
        shutdownManager.registerShutdownListener(new ShutdownManager.ShutdownListener() {
            public void shutdown() {
                // shutdown grizzly/graphs
                stop();
            }
        });

        //Start the shutdown listener
        shutdownManager.start();

        //Wait for a shutdown request and all shutdown listeners to complete
        shutdownManager.waitForShutdown();
    }

    public static void main(String args[]) throws Exception {
        RexsterProperties rexsterProperties = new RexsterSettings(args).getProperties();
        Properties ezBakeProperties = new EzConfiguration().getProperties();
        RexsterProperties combinedProperties = RexsterPropertiesUtils.combineRexsterEzBakeProperties(rexsterProperties,
                ezBakeProperties);

        new StandaloneApplication(combinedProperties).start();
    }
}
