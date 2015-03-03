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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityClient;
import ezbake.thrift.ThriftTestUtils;
import ezbake.thrift.ThriftUtils;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DriverTest {

    private Properties properties;
    private EzSecurityClient securityClient;
    private Connection connection;

    private final static String JDBC_EZBAKE_POSTGRESQL_SCHEME = "jdbc:ezbake:postgresql";

    @Before
    public void setUp() throws EzConfigurationLoaderException {
        properties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        properties.setProperty("user", properties.getProperty("postgres.username"));
        properties.setProperty("password", properties.getProperty("postgres.password"));

        securityClient = new EzbakeSecurityClient(properties);

        connection = null;
    }

    @After
    public void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    @Test
    public void testSetContextTokenByDefault() throws SQLException, TException {
        connection = DriverManager.getConnection(
                String.format("%s://%s:%s/%s",
                        JDBC_EZBAKE_POSTGRESQL_SCHEME,
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties);

        assertCurrentTokenSettingEquals(securityClient.fetchTokenForProxiedUser());
    }

    @Test
    public void testSetContextToken() throws SQLException, TException {
        properties.setProperty("ezbakeTokenProvider", "ezbake.data.postgres.SpringSecurityContextTokenProvider");

        connection = DriverManager.getConnection(
                String.format("%s://%s:%s/%s",
                        JDBC_EZBAKE_POSTGRESQL_SCHEME,
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties);

        assertCurrentTokenSettingEquals(securityClient.fetchTokenForProxiedUser());
    }

    @Test
    @Ignore // There's a bug where we can't generate an app token in mock mode
    public void testSetApplicationToken() throws SQLException, TException {
        properties.setProperty("ezbakeTokenProvider", "ezbake.data.postgres.ApplicationTokenProvider");

        connection = DriverManager.getConnection(
                String.format("%s://%s:%s/%s",
                        JDBC_EZBAKE_POSTGRESQL_SCHEME,
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties);

        assertCurrentTokenSettingEquals(securityClient.fetchAppToken());
    }

    @Test
    public void testSetExplicitToken() throws SQLException, TException {
        String securityId = new EzBakeApplicationConfigurationHelper(properties).getSecurityID();
        EzSecurityToken explicitToken = ThriftTestUtils.generateTestSecurityToken(securityId, securityId, Arrays.asList("U", "S", "TS"));

        properties.setProperty("ezbakeTokenProvider", "ezbake.data.postgres.ExplicitTokenProvider");
        properties.setProperty("ezbakeToken", ThriftUtils.serializeToBase64(explicitToken));

        connection = DriverManager.getConnection(
                String.format("%s://%s:%s/%s",
                        JDBC_EZBAKE_POSTGRESQL_SCHEME,
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties);

        assertCurrentTokenSettingEquals(explicitToken);
    }

    @Test
    public void testSetContextTokenURL() throws SQLException, TException {
        connection = DriverManager.getConnection(
                String.format("%s://%s:%s/%s?ezbakeTokenProvider=ezbake.data.postgres.SpringSecurityContextTokenProvider",
                        JDBC_EZBAKE_POSTGRESQL_SCHEME,
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties);

        assertCurrentTokenSettingEquals(securityClient.fetchTokenForProxiedUser());
    }

    @Test
    @Ignore // There's a bug where we can't generate an app token in mock mode
    public void testSetApplicationTokenURL() throws SQLException, TException {
        connection = DriverManager.getConnection(
                String.format("%s://%s:%s/%s?ezbakeTokenProvider=ezbake.data.postgres.ApplicationTokenProvider",
                        JDBC_EZBAKE_POSTGRESQL_SCHEME,
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties);

        assertCurrentTokenSettingEquals(securityClient.fetchAppToken());
    }

    @Test
    public void testSetExplicitTokenURL() throws SQLException, TException, UnsupportedEncodingException {
        String securityId = new EzBakeApplicationConfigurationHelper(properties).getSecurityID();
        EzSecurityToken explicitToken = ThriftTestUtils.generateTestSecurityToken(securityId, securityId, Arrays.asList("U", "S", "TS"));

        connection = DriverManager.getConnection(
                String.format("%s://%s:%s/%s?ezbakeTokenProvider=ezbake.data.postgres.ExplicitTokenProvider&ezbakeToken=%s",
                        JDBC_EZBAKE_POSTGRESQL_SCHEME,
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db"),
                        URLEncoder.encode(ThriftUtils.serializeToBase64(explicitToken), "UTF-8")),
                properties);

        assertCurrentTokenSettingEquals(explicitToken);
    }

    @Test
    public void testAcceptsURL() throws SQLException {
        String url = String.format("%s://%s:%s/%s",
                JDBC_EZBAKE_POSTGRESQL_SCHEME,
                properties.getProperty("postgres.host"),
                properties.getProperty("postgres.port"),
                properties.getProperty("postgres.db"));
        Driver driver = DriverManager.getDriver(url);

        assertTrue(driver.acceptsURL(url));
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        String url = String.format("%s://%s:%s/%s",
                JDBC_EZBAKE_POSTGRESQL_SCHEME,
                properties.getProperty("postgres.host"),
                properties.getProperty("postgres.port"),
                properties.getProperty("postgres.db"));
        Driver driver = DriverManager.getDriver(url);
        driver.getPropertyInfo(url, properties);
    }

    private void assertCurrentTokenSettingEquals(EzSecurityToken expected) throws SQLException, TException {
        Statement st = null;
        ResultSet rs = null;

        try {
            st = connection.createStatement();
            rs = st.executeQuery("select current_setting('ezbake.token');");

            assertTrue(rs.next());
            String base64EncodedToken = rs.getString("current_setting");
            rs.close();
            st.close();

            EzSecurityToken actual = ThriftUtils.deserializeFromBase64(EzSecurityToken.class, base64EncodedToken);
            assertEquals(expected, actual);
        } finally {
            if (rs != null) {
                rs.close();
            }

            if (st != null) {
                st.close();
            }
        }
    }
}
