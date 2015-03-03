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

package ezbake.data.test;

import static org.junit.Assert.assertNotNull;

import static ezbake.data.test.TestUtils.MOCK_APP_SEC_ID;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

/**
 *
 * @author blong
 */
public class TestUtilsTest {
    /**
     * Test of createTSUser method, of class TestUtils.
     */
    @Test
    public void testCreateTSUser() {
        assertNotNull(TestUtils.createTSUser());
    }

    /**
     * Test of createTS_S_B_User method, of class TestUtils.
     */
    @Test
    public void testCreateTS_S_B_User() {
        assertNotNull(TestUtils.createTS_S_B_User());
    }

    /**
     * Test of createTestToken method, of class TestUtils.
     */
    @Test
    public void testCreateTestToken() {
        final String sid = "SOME_TEST_SID";
        final Set<String> auths = Sets.newHashSet("SOME_CLEARANCE", "S", "D");
        assertNotNull(TestUtils.createTestToken(sid, auths, MOCK_APP_SEC_ID));
    }
}
