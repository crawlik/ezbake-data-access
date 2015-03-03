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

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Permission;
import ezbake.base.thrift.PlatformObjectVisibilities;
import ezbake.base.thrift.Visibility;
import ezbake.security.permissions.PermissionUtils;
import ezbake.thrift.ThriftTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultPermissionContextTest {

    private DefaultPermissionContext ctx;

    @Before
    public void setUp() {
        ctx = new DefaultPermissionContext(ThriftTestUtils.generateTestSecurityToken("A", "B"));
    }

    @Test
    public void testNullAdvancedMarkings() {
        Visibility vis = new Visibility();
        vis.setFormalVisibility("A");

        assertEquals(PermissionUtils.ALL_PERMS, ctx.getPermissions(vis));
    }

    @Test
    public void testEmptyAdvancedMarkings() {
        Visibility vis = new Visibility();
        vis.setFormalVisibility("A");
        vis.setAdvancedMarkings(new AdvancedMarkings());

        assertEquals(PermissionUtils.ALL_PERMS, ctx.getPermissions(vis));
    }

    @Test
    public void testNonEmptyAdvancedMarkings() {
        Authorizations aut = new Authorizations();
        aut.setPlatformObjectAuthorizations(Collections.singleton(1L));
        aut.setFormalAuthorizations(Collections.singleton("A"));

        EzSecurityToken tok = ThriftTestUtils.generateTestSecurityToken("A");
        tok.setAuthorizations(aut);

        PlatformObjectVisibilities pov = new PlatformObjectVisibilities();
        pov.setPlatformObjectReadVisibility(Collections.singleton(1L));
        pov.setPlatformObjectWriteVisibility(Collections.singleton(2L));

        AdvancedMarkings adv = new AdvancedMarkings();
        adv.setPlatformObjectVisibility(pov);

        Visibility vis = new Visibility();
        vis.setFormalVisibility("A");
        vis.setAdvancedMarkings(adv);

        ctx = new DefaultPermissionContext(tok);

        // Explicitly granted POV
        assertTrue(ctx.hasAnyPermission(vis, Permission.READ));

        // POV is set and we're not in the vector
        assertFalse(ctx.hasAnyPermission(vis, Permission.WRITE));

        // POV is not set, defaults to true
        assertTrue(ctx.hasAnyPermission(vis, Permission.DISCOVER));
    }

    @Test
    public void testEmptyVisibility() {
        Visibility vis = new Visibility();

        assertEquals(PermissionUtils.ALL_PERMS, ctx.getPermissions(vis));
    }

    @Test
    public void testFormalVisibilityPermissions() {
        Visibility vis = new Visibility();

        vis.setFormalVisibility("A");
        assertEquals(PermissionUtils.ALL_PERMS, ctx.getPermissions(vis));

        vis.setFormalVisibility("C");
        assertEquals(PermissionUtils.NO_PERMS, ctx.getPermissions(vis));
    }
}
