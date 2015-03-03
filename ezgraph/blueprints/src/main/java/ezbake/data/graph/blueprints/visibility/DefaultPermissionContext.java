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

package ezbake.data.graph.blueprints.visibility;

import java.util.Set;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.security.permissions.PermissionEvaluator;

/**
 * A permission context that evaluates visibilities against the authorizations
 * contained in a security token.
 */
public class DefaultPermissionContext extends PermissionContext {

    /**
     * Evaluates permissions.
     */
    private final PermissionEvaluator evaluator;

    /**
     * Deserializer for visibility objects. Shared across all instances of
     * context because it has no state.
     */
    private static final VisibilityDeserializer visibilityDeserializer =
            new CachingVisibilityDeserializer(new DefaultVisibilityDeserializer());

    /**
     * Construct a permission context that evaluates visibilities against the
     * authorizations contained in a security token.
     *
     * @param token token containing authorizations to use when evaluating
     *              visibilities.
     */
    public DefaultPermissionContext(EzSecurityToken token) {
        this.evaluator = new PermissionEvaluator(token.getAuthorizations());
    }

    @Override
    public Set<Permission> getPermissions(Visibility visibility) {
        return evaluator.getPermissions(visibility);
    }

    @Override
    public VisibilityDeserializer getElementVisibilityDeserializer() {
        return visibilityDeserializer;
    }

    @Override
    public VisibilityDeserializer getPropertyVisibilityDeserializer() {
        return visibilityDeserializer;
    }
}
