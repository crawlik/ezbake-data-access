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

import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public abstract class AbstractEzPostgresIntegrationTest {

    protected Properties properties;
    protected Connection connection;

    @Before
    public void setUp() throws Exception {
        properties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
        properties.setProperty("user", properties.getProperty("postgres.username"));
        properties.setProperty("password", properties.getProperty("postgres.password"));

        connection = DriverManager.getConnection(
                String.format("jdbc:ezbake:postgresql://%s:%s/%s",
                        properties.getProperty("postgres.host"),
                        properties.getProperty("postgres.port"),
                        properties.getProperty("postgres.db")),
                properties);
    }

    @After
    public void tearDown() throws SQLException, InterruptedException {
        connection.close();
    }
}
