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

package ezbake.data.postgres.functional;

import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * These tests are not designed to document the intended or future behavior.
 * They only demonstrate behavior as it exists today.
 */
public class RoleTest extends AbstractEzPostgresIntegrationTest {

    /**
     * We can change roles to super user.
     * @throws SQLException
     */
    @Test // XXX: this probably shouldn't be allowed
    public void testServicePreventsSetRole() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("SET ROLE deployerdba;");
        statement.close();
    }
}
