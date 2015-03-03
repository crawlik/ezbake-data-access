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

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.rexster.RexsterApplicationGraph;
import com.tinkerpop.rexster.extension.ExtensionSegmentSet;
import com.tinkerpop.rexster.gremlin.GremlinExtension;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.graph.blueprints.visibility.DefaultPermissionContext;
import ezbake.data.graph.blueprints.visibility.PermissionContext;
import ezbake.data.graph.blueprints.visibility.VisibilityFilterGraph;

/**
 * Wrapper around a RexsterApplicationGraph that overrides {@link com.tinkerpop.rexster
 * .RexsterApplicationGraph#getGraph()} to return {@link VisibilityFilterGraph} wrapped Blueprints graphs instead of a
 * 'standard' (non-wrapped) Blueprints graph.
 */
public class SecurityTokenRexsterApplicationGraphWrapper extends RexsterApplicationGraph {
    /**
     * EzSecurityToken which can be used to regulate read and write operations.
     */
    private final EzSecurityToken token;

    /**
     * Constructor creates a graph wrapper ready to hand out Blueprints graphs wrapped with the {@link
     * VisibilityFilterGraph}, which has access to the EzSecurityToken.
     *
     * @param graph The graph to return wrapped in a VisibilityFilterGraphWrapper.
     * @param token The token associated with the graph returned by getGraph.
     */
    public SecurityTokenRexsterApplicationGraphWrapper(RexsterApplicationGraph graph, EzSecurityToken token) {
        super(graph.getGraphName(), graph.getGraph());
        this.token = token;
    }

    @Override
    public Graph getGraph() {
        PermissionContext permissionContext = new DefaultPermissionContext(token);
        return new VisibilityFilterGraph(super.getGraph(), permissionContext);
    }

    //TODO: Determine if we need to delegate any more RexsterApplicationGraph methods to the wrapped graph.

    @Override
    public boolean isExtensionAllowed(final ExtensionSegmentSet extensionSegmentSet) {
        if (extensionSegmentSet.getExtension().equals(GremlinExtension.EXTENSION_NAME) && extensionSegmentSet
                .getNamespace().equals(GremlinExtension.EXTENSION_NAMESPACE)) {
            return true;
        }

        return false;
    }
}
