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

package ezbake.data.graph.rexster.graphstore;

import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.rexster.RexsterApplicationGraph;
import com.tinkerpop.rexster.server.RexsterApplication;
import com.tinkerpop.rexster.server.RexsterProperties;
import com.tinkerpop.rexster.server.XmlRexsterApplication;

import ezbake.base.thrift.EzSecurityToken;

/**
 * Wrapper around the XmlRexsterApplication
 */
public class RexsterXmlConfigurationGraphStore implements GraphStore {
    private static final Logger logger = LoggerFactory.getLogger(RexsterXmlConfigurationGraphStore.class);

    /**
     * Standard RexsterApplication built from xml configuration.
     */
    private RexsterApplication wrappedRexsterApplication;

    @Override
    public RexsterApplicationGraph getApplicationGraph(String graphName, EzSecurityToken token) {

        logger.debug("Graph '{}' requested by {}", graphName, token.getTokenPrincipal().getName());
        return wrappedRexsterApplication.getApplicationGraph(graphName);
    }

    @Override
    public void initialize(Properties props) {
        // TODO: find better way to get config: read location from props file, or default to src/main/resources
        RexsterProperties properties = new RexsterProperties("config/rexster.xml");
        wrappedRexsterApplication = new XmlRexsterApplication(properties);
    }

    @Override
    public Set<String> getGraphNames(EzSecurityToken token) {
        return wrappedRexsterApplication.getGraphNames();
    }
}
