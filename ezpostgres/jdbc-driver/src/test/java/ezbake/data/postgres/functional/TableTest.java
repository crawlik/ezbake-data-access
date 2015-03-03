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

import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * These tests are not designed to document the intended or future behavior.
 * They only demonstrate behavior as it exists today.
 */
public class TableTest extends AbstractEzPostgresIntegrationTest {

    @Test
    public void testTableCanBeDropped() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists foo cascade;");
        s.execute("create table foo (x int, visibility varchar default E'CwABAAAAAVUA');");
        s.execute("drop table foo;");
        s.close();
    }

    @Test
    public void testTableCanBeAltered() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists foo cascade;");
        s.execute("create table foo (x int, visibility varchar default E'CwABAAAAAVUA');");
        s.execute("alter table foo add column y int;");
        s.execute("drop table foo;");
        s.close();
    }

    @Test
    public void testTableCanBeIndexed() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists foo cascade;");
        s.execute("create table foo (x int, visibility varchar default E'CwABAAAAAVUA');");
        s.execute("create index foo_x_index on foo(x);");
        s.execute("drop index foo_x_index;");
        s.execute("drop table foo;");
        s.close();
    }

    @Test
    public void testTableCanBeViewed() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists foo cascade;");
        s.execute("create table foo (x int, visibility varchar default E'CwABAAAAAVUA');");
        s.execute("create view foo_view as select * from foo;");
        s.execute("drop view foo_view;");
        s.execute("drop table foo;");
        s.close();
    }

    @Test
    public void testTableCanInheritVisibility() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists foo cascade;");
        s.execute("drop table if exists bar cascade;");
        s.execute("create table foo (x int, visibility varchar default E'CwABAAAAAVUA');");
        s.execute("create table bar (y int) inherits (foo)");

        s.execute("drop table foo cascade;");
        s.close();
    }

    @Test
    @Ignore // We haven't written the event triggers yet
    public void testVisibilityColumnIsProtected() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists foo cascade;");
        s.execute("create table foo (x int, visibility varchar default E'CwABAAAAAVUA');");
        s.execute("insert into foo values (1);");

        ResultSet rs = s.executeQuery("select count(*) from foo;");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("count"));

        SQLException ex = null;
        try {
            s.execute("alter table foo drop column visibility;");
        } catch (SQLException e) {
            ex = e;
        } finally {
            s.execute("drop table foo;");
            s.close();
        }

        assertNotNull(ex);
    }

    @Test
    public void testJoinTableCanBeDropped() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists bar cascade;");
        s.execute("create table bar (x int, y int);");
        s.execute("drop table bar;");
        s.close();
    }

    /**
     * This may be a problem where we can circumvent the security rules by
     * creating a two column table then altering it. Since the "unsecured"
     * keyword used to exist though, I don't know how much we care.
     */
    @Test
    public void testJoinTableCanBeAltered() throws SQLException {
        Statement s = connection.createStatement();
        s.execute("drop table if exists bar cascade;");
        s.execute("create table bar (x int, y int);");
        s.execute("alter table bar add column z varchar;");
        s.execute("drop table bar;");
        s.close();
    }
}
