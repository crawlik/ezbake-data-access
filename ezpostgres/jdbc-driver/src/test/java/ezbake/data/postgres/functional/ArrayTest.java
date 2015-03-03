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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArrayTest extends AbstractEzPostgresIntegrationTest {

    public static final String uuid1 = "00000000-0000-0000-0000-000000000001";
    public static final String uuid2 = "00000000-0000-0000-0000-000000000002";
    public static final String uuid3 = "00000000-0000-0000-0000-000000000003";

    @Test
    public void testCreateArray() throws SQLException {
        connection.createArrayOf("uuid", new String[] { uuid1, uuid2 });
    }

    @Test
    public void testSetArray() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists foo cascade;");
        statement.execute("create table foo (id uuid primary key, s varchar(16), visibility varchar default E'CwABAAAAAVUA');");

        PreparedStatement insertPS = connection.prepareStatement("insert into foo values (?, ?);");
        insertPS.setObject(1, UUID.fromString(uuid1));
        insertPS.setString(2, "a");
        insertPS.addBatch();
        insertPS.setObject(1, UUID.fromString(uuid2));
        insertPS.setString(2, "b");
        insertPS.addBatch();
        insertPS.setObject(1, UUID.fromString(uuid3));
        insertPS.setString(2, "c");
        insertPS.addBatch();
        insertPS.executeBatch();
        insertPS.close();

        PreparedStatement selectPS = connection.prepareStatement("select id, s from foo where id = any(?) order by id;");
        selectPS.setArray(1, connection.createArrayOf("uuid", new String[] { uuid1, uuid2 }));

        ResultSet rs = selectPS.executeQuery();
        assertTrue(rs.next());
        assertEquals("a", rs.getString("s"));
        assertTrue(rs.next());
        assertEquals("b", rs.getString("s"));
        rs.close();

        statement.execute("drop table foo;");
        statement.close();
    }
}
