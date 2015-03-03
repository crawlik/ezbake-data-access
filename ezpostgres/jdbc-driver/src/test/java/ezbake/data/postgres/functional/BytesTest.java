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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BytesTest extends AbstractEzPostgresIntegrationTest {

    @Test
    public void testInsertEscapedBytes() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists foo;");
        statement.execute("create table foo (b bytea, visibility varchar default E'CwABAAAAAVUA');");
        statement.execute("insert into foo values (E'\\\\x457a'::bytea);");

        ResultSet rs = statement.executeQuery("select count(*) from foo;");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("count"));

        statement.execute("drop table foo;");
        statement.close();
    }

    @Test
    public void testGetResultBytes() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists foo;");
        statement.execute("create table foo (b bytea, visibility varchar default E'CwABAAAAAVUA');");
        statement.execute("insert into foo values (E'\\\\x457a'::bytea);");

        ResultSet rs = statement.executeQuery("select b from foo;");
        assertTrue(rs.next());

        byte[] b = rs.getBytes("b");
        assertEquals(2, b.length);
        assertEquals(0x45, b[0]);
        assertEquals(0x7a, b[1]);

        statement.execute("drop table foo;");
        statement.close();
    }

    @Test
    public void testSetStatementBytes() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists foo;");
        statement.execute("create table foo (b bytea, visibility varchar default E'CwABAAAAAVUA');");

        PreparedStatement ps = connection.prepareStatement("insert into foo values (?);");
        ps.setBytes(1, new byte[] { 0x45, 0x7a });
        ps.execute();

        ResultSet rs = statement.executeQuery("select b from foo;");
        assertTrue(rs.next());

        byte[] b = rs.getBytes("b");
        assertEquals(2, b.length);
        assertEquals(0x45, b[0]);
        assertEquals(0x7a, b[1]);

        statement.execute("drop table foo;");
        statement.close();
    }
}
