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

import com.google.common.base.Charsets;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.constants.EzBakePropertyConstants;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.TSerializer;
import org.hibernate.SessionFactory;

import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;

import java.util.Map;
import java.util.Properties;

public class HibernateUtil {

	public static enum SessionType { EZBAKE, POSTGRES };

	private static SessionFactory buildSessionFactory(Properties ezConfig, EzSecurityToken token, SessionType sessionType) {
		try {
			String appName = ezConfig.getProperty(EzBakePropertyConstants.EZBAKE_APPLICATION_NAME);
			switch (sessionType) {
			case EZBAKE : {

				// Create the SessionFactory from hibernate.cfg.xml
                Configuration configuration = new Configuration();
                for (Map.Entry<Object, Object> entry : ezConfig.entrySet()) {
                    String key = (String) entry.getKey();
                    String value = (String) entry.getValue();

                    configuration.setProperty("hibernate.connection." + key, value);
                }

				return configuration
				.setProperty("hibernate.connection.driver_class", "ezbake.data.postgres.EzPostgresDriver")
				.setProperty("hibernate.connection.username", ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_USERNAME))
				.setProperty("hibernate.connection.password", ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_PASSWORD))
				.setProperty("hibernate.connection.url",
                        String.format("jdbc:ezbake:postgresql://%s:%s/%s",
                                ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_HOST),
                                ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_PORT),
                                appName))
				.setProperty("hibernate.default_schema", "public")
				.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQL9Dialect")
				.setProperty("EzSecurityToken", new String(Base64.encodeBase64(new TSerializer().serialize(token)),
                        Charsets.US_ASCII))
						.setProperty("hibernate.hbm2ddl.auto", "update")
						.configure().buildSessionFactory();
			}
			case POSTGRES : {
				return new Configuration()
				.setProperty("hibernate.connection.driver_class", "org.postgresql.Driver")
				.setProperty("hibernate.connection.username", ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_USERNAME))
				.setProperty("hibernate.connection.password", ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_PASSWORD))
				.setProperty("hibernate.connection.url",
                        String.format("jdbc:postgresql://%s:%s/%s",
                                ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_HOST),
                                ezConfig.getProperty(EzBakePropertyConstants.POSTGRES_PORT),
                                appName))
				.setProperty("hibernate.default_schema", "public")
				.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQL9Dialect")
				.setProperty("hibernate.show_sql", "true")
				.setProperty("hibernate.hbm2ddl.auto", "update")
				.configure().buildSessionFactory();
			}
			default : {
				throw new Exception("Invalid session type [" + sessionType + "].");
			}
			}
		} catch (Throwable ex) {
			// Make sure you log the exception, as it might be swallowed
			System.err.println("Initial SessionFactory creation failed." + ex);
			throw new ExceptionInInitializerError(ex);
		}
	}

	public static SessionFactory getSessionFactory(Properties ezConfig, EzSecurityToken userToken, SessionType sessionType) {
		return buildSessionFactory(ezConfig, userToken, sessionType);
	}


}
