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

package ezbake.data.elastic.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Sets;

import ezbake.base.thrift.Authorizations;

/**
 * Created by jpercivall on 1/26/15.
 */
public class ElasticUtilsTest {

    @Test
    public void testVerifyAuthorizations() throws Exception {
        Authorizations authorizationsToCheckAgainst = new Authorizations();
        Authorizations authorizationsToCheck = new Authorizations();

        // Formal Against null
        authorizationsToCheck.setFormalAuthorizations(Sets.newHashSet("U"));
        assertFalse(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Formal To Check null
        authorizationsToCheck.setFormalAuthorizations(null);
        authorizationsToCheckAgainst.setFormalAuthorizations(Sets.newHashSet("U"));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Formal Equal
        authorizationsToCheck.setFormalAuthorizations(Sets.newHashSet("U"));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Formal To Check Higher
        authorizationsToCheck.setFormalAuthorizations(Sets.newHashSet("U", "S", "AB", "CD", "EF", "USA"));
        assertFalse(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Formal To Check Lower
        authorizationsToCheck.setFormalAuthorizations(Sets.newHashSet("U"));
        authorizationsToCheckAgainst.setFormalAuthorizations(Sets.newHashSet("U", "S", "AB", "CD", "EF", "USA"));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        /* ----------------------------- */

        // External Against null
        authorizationsToCheck.setExternalCommunityAuthorizations(Sets.newHashSet("U"));
        assertFalse(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // External To Check null
        authorizationsToCheck.setExternalCommunityAuthorizations(null);
        authorizationsToCheckAgainst.setExternalCommunityAuthorizations(Sets.newHashSet("U"));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // External Equal
        authorizationsToCheck.setExternalCommunityAuthorizations(Sets.newHashSet("U"));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // External To Check Higher
        authorizationsToCheck.setExternalCommunityAuthorizations(Sets.newHashSet("U", "S", "AB", "CD", "EF", "USA"));
        assertFalse(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // External To Check Lower
        authorizationsToCheck.setExternalCommunityAuthorizations(Sets.newHashSet("U"));
        authorizationsToCheckAgainst.setExternalCommunityAuthorizations(
                Sets.newHashSet("U", "S", "AB", "CD", "EF", "USA"));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        /* ----------------------------- */

        // Platform Against null
        authorizationsToCheck.setPlatformObjectAuthorizations(Sets.newHashSet(new Long(1)));
        assertFalse(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Platform To Check null
        authorizationsToCheck.setPlatformObjectAuthorizations(null);
        authorizationsToCheckAgainst.setPlatformObjectAuthorizations(Sets.newHashSet(new Long(1)));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Platform Equal
        authorizationsToCheck.setPlatformObjectAuthorizations(Sets.newHashSet(new Long(1)));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Platform To Check Higher
        authorizationsToCheck.setPlatformObjectAuthorizations(Sets.newHashSet(new Long(1),new Long(2)));
        assertFalse(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));

        // Platform To Check Lower
        authorizationsToCheck.setPlatformObjectAuthorizations(Sets.newHashSet(new Long(1)));
        authorizationsToCheckAgainst.setPlatformObjectAuthorizations(Sets.newHashSet(new Long(1),new Long(2)));
        assertTrue(ElasticUtils.verifyAuthorizations(authorizationsToCheck, authorizationsToCheckAgainst));
    }
}
