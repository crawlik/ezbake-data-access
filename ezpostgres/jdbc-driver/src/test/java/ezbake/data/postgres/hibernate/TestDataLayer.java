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

package ezbake.data.postgres.hibernate;

import com.google.common.collect.Lists;
import ezbake.data.postgres.hibernate.HibernateUtil.SessionType;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.thrift.ThriftTestUtils;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import ezbakehelpers.ezconfigurationhelpers.postgres.PostgresConfigurationHelper;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by klilly on 10/3/14.
 */
public class TestDataLayer {

	private static EzSecurityToken securityToken;

	private static final SessionType SESSION_TYPE = SessionType.EZBAKE;

	@BeforeClass
	public static void init() throws Exception {
		EzConfiguration configuration = new EzConfiguration(new ClasspathConfigurationLoader());

		EzBakeApplicationConfigurationHelper appConfig =
				new EzBakeApplicationConfigurationHelper(configuration.getProperties());
		String appName = appConfig.getApplicationName();
        securityToken = ThriftTestUtils.generateTestSecurityToken(appConfig.getSecurityID(),
                appConfig.getSecurityID(), Lists.newArrayList("U"));
	}

	@Before
	public void createClient() throws TException, SQLException {
		// Wipe out existing data
		//cleanDatabase(configuration.getProperties(), securityToken);
	}

	//--- FEATURE TESTS ---//

	@Test
	public void testHibernate() throws Exception {
		DataLayer dl = new DataLayer();
		dl.doSomething(securityToken, SESSION_TYPE);
	}

	private void cleanDatabase(Properties ezConfigurationProperties, EzSecurityToken ezSecurityToken) throws
	SQLException, TException {

		if (SESSION_TYPE.equals(SessionType.EZBAKE)) {
			String command = "DELETE FROM magic";

			try (Connection conn = getConnection(ezConfigurationProperties, ezSecurityToken)) {
				conn.setAutoCommit(false);
				try (PreparedStatement ps = conn.prepareStatement(command)) {
					ps.executeUpdate();
				}
				conn.commit();
			}
		}
	}

	private Connection getConnection(Properties properties, EzSecurityToken ezSecurityToken) throws SQLException, TException {
		PostgresConfigurationHelper helper = new PostgresConfigurationHelper(properties);
		Connection connection = helper.getEzPostgresConnection(ezSecurityToken);
		connection.setClientInfo("EzSecurityToken", String.valueOf(new TSerializer().serialize(ezSecurityToken)));
		return connection;
	}
}
