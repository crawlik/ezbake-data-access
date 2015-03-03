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

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.thrift.ThriftTestUtils;
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VisibilityFilterTest {

    private Properties properties;
    private Connection connection;

    @Before
    public void setUp() throws EzConfigurationLoaderException, SQLException {
        properties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        connection = DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%s/%s",
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties.getProperty("postgres.username"),
                properties.getProperty("postgres.password"));
    }

    @After
    public void tearDown() throws SQLException {
        connection.close();
    }

    @Test
    public void testSelectFilters() throws SQLException, TException {
        Statement st = connection.createStatement();

        st.execute("drop table if exists foo cascade;");
        st.execute("create table foo (i int8 primary key, visibility varchar(32768));");

        PreparedStatement ips = connection.prepareStatement("insert into foo values (?, ?);");

        // U marked row
        ips.setInt(1, 1);
        ips.setString(2, "CwABAAAAAVUA");
        ips.addBatch();

        // TS marked row
        ips.setInt(1, 2);
        ips.setString(2, "CwABAAAAAlRTAA==");
        ips.addBatch();

        ips.executeBatch();

        EzSecurityToken token = ThriftTestUtils.generateTestSecurityToken("id", "id", Arrays.asList("U"));
        st.execute(String.format("set ezbake.token = '%s';", ThriftUtils.serializeToBase64(token)));
        ResultSet rs = st.executeQuery("select count(*) as c from foo;");

        assertTrue(rs.next());
        assertEquals(1, rs.getInt("c"));
        rs.close();

        token = ThriftTestUtils.generateTestSecurityToken("id", "id", Arrays.asList("U", "TS"));
        st.execute(String.format("set ezbake.token = '%s';", ThriftUtils.serializeToBase64(token)));
        rs = st.executeQuery("select count(*) as c from foo;");

        assertTrue(rs.next());
        assertEquals(2, rs.getInt("c"));
        rs.close();

        PreparedStatement sps = connection.prepareStatement("select count(*) as c from foo where i = ?;");
        sps.setInt(1, 1);
        rs = sps.executeQuery();

        assertTrue(rs.next());
        assertEquals(1, rs.getInt("c"));
        rs.close();

        st.execute("drop table foo;");

        st.close();
    }
}
