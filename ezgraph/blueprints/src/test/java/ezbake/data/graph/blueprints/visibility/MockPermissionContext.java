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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import ezbake.base.thrift.Permission;
import ezbake.base.thrift.Visibility;
import ezbake.security.permissions.PermissionUtils;

public class MockPermissionContext extends PermissionContext {
    private Set<Permission> defaultPermissions;
    private Set<Permission> defaultElementPermissions;
    private Map<Visibility, Set<Permission>> visibilityMap;

    private Map<Object, Set<Permission>> edgeMap;
    private Map<Object, Set<Permission>> vertexMap;

    public MockPermissionContext() {
        defaultPermissions = PermissionUtils.NO_PERMS;
        defaultElementPermissions = PermissionUtils.NO_PERMS;
        visibilityMap = new HashMap<>();

        edgeMap = new HashMap<>();
        vertexMap = new HashMap<>();
    }

    public void setDefaultPermissions(Permission... permissions) {
        defaultPermissions = Sets.newHashSet(permissions);
    }

    public void setDefaultPermissions(Set<Permission> permissions) {
        defaultPermissions = new HashSet<>(permissions);
    }

    public void setDefaultElementPermissions(Permission... permissions) {
        defaultElementPermissions = Sets.newHashSet(permissions);
    }

    public void setDefaultElementPermissions(Set<Permission> permissions) {
        defaultElementPermissions = new HashSet<>(permissions);
    }

    @Override
    public Set<Permission> getPermissions(Visibility visibility) {
        Set<Permission> p = visibilityMap.get(visibility);
        if (p == null) {
            return defaultPermissions;
        } else {
            return p;
        }
    }

    @Override
    public VisibilityDeserializer getElementVisibilityDeserializer() {
        return new DefaultVisibilityDeserializer();
    }

    @Override
    public VisibilityDeserializer getPropertyVisibilityDeserializer() {
        return new DefaultVisibilityDeserializer();
    }

    public void setPermissions(Visibility visibility, Permission... permissions) {
        visibilityMap.put(visibility, Sets.newHashSet(permissions));
    }

    public void setPermissions(Visibility visibility, Set<Permission> permissions) {
        visibilityMap.put(visibility, new HashSet<>(permissions));
    }

    public void setElementPermissions(Edge edge, Permission... permissions) {
        updateElementPermissions(edgeMap, edge, permissions);
    }

    public void setElementPermissions(Edge edge, Set<Permission> permissions) {
        updateElementPermissions(edgeMap, edge, permissions);
    }

    public void setElementPermissions(Vertex vertex, Permission... permissions) {
        updateElementPermissions(vertexMap, vertex, permissions);
    }

    public void setElementPermissions(Vertex vertex, Set<Permission> permissions) {
        updateElementPermissions(vertexMap, vertex, permissions);
    }

    private static void updateElementPermissions(Map<Object, Set<Permission>> map, Element element, Permission... permissions) {
        updateElementPermissions(map, element, Sets.newHashSet(permissions));
    }

    private static void updateElementPermissions(Map<Object, Set<Permission>> map, Element element, Set<Permission> permissions) {
        if (element instanceof VisibilityFilterElement) {
            updateElementPermissions(map, ((VisibilityFilterElement) element).getBaseElement(), permissions);
        }

        map.put(element.getId(), new HashSet<>(permissions));
    }

    public ElementFilter getElementFilter() {
        return new MockElementFilter(this);
    }

    private class MockElementFilter extends ElementFilter {

        public MockElementFilter(PermissionContext ctx) {
            super(ctx);
        }

        @Override
        public Set<Permission> getPermissions(Element element) {
            Set<Permission> p = null;
            if (element instanceof Edge) {
                p = edgeMap.get(element.getId());
            } else if (element instanceof Vertex) {
                p = vertexMap.get(element.getId());
            }

            if (p == null) {
                p = defaultElementPermissions;
            }

            return p;
        }

        @Override
        public boolean hasAnyPermissions(Element element, Permission... permissions) {
            Set<Permission> ps = getPermissions(element);
            for (Permission p : permissions) {
                if (ps.contains(p)) {
                    return true;
                }
            }

            return false;
        }
    }
}
