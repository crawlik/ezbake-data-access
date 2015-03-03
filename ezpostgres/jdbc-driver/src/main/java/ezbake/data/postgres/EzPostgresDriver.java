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

package ezbake.data.postgres;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.security.common.core.EzSecurityClient;
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

/**
 * Wraps a real Driver with one that returns Connections decorated with TokenPropertyConnectionDecorator.
 * <p />
 * Accepts any URL that a Driver is registered for, as long as the scheme is prefixed with "ezbake". For example,
 * jdbc:ezbake:postgresql://... wraps a Driver that can accept jdbc:postgresql://.
 * <p />
 * The token passed to the database is configurable by the properties <code>ezbake.token.provider</code> (the fully
 * qualified name of a provider; known options are: <code>ezbake.data.postgres.SpringSecurityContextTokenProvider</code>
 * - default, <code>ezbake.data.postgres.ApplicationTokenProvider</code>, and
 * <code>ezbake.data.postgres.ExplicitTokenProvider</code>). If <code>ExplicitTokenProvider</code> is used, then the
 * property <code>ezbake.token</code> must contain a base64 encoded EzSecurityToken.
 * <p />
 * The query parameters <code>ezbakeTokenProvider</code> and <code>ezbakeToken</code> may also be passed to the
 * connection URL. If <code>ezbakeToken</code> is passed, it must be URL-escaped. Unfortunately, different database
 * implementations pass properties through the connection URL differently (for example, Derby and H2 both pass pairs
 * after a ';' instead of '?'), so URL property passing is known to work only with PostgreSQL.
 * <p />
 * This driver manages EzBake security clients that can be used across different connections. The properties passed to
 * the client are taken from the Properties passed to connect(). If not Properties are given, then they are read using
 * EzConfiguration.
 */
public class EzPostgresDriver implements Driver {

    /** Prefix for the scheme portion of the JDBC connection URL */
    public static final String JDBC_URL_PREFIX = "jdbc:";

    /** Prefix for the scheme portion of the JDBC connection URL for this EzBake-wrapped driver */
    public static final String JDBC_EZBAKE_URL_PREFIX = "jdbc:ezbake:";

    /** Property name for token provider class */
    public static final String EZBAKE_TOKEN_PROVIDER_PROPERTY = "ezbakeTokenProvider";

    /** Property name for explicit token */
    public static final String EZBAKE_TOKEN_PROPERTY = "ezbakeToken";

    /** Encoding used to URL-encoded query parameters */
    public static final String URL_ENCODING = "UTF-8";

    private static final String DEFAULT_TOKEN_PROVIDER = SpringSecurityContextTokenProvider.class.getCanonicalName();

    private static final Logger logger = LoggerFactory.getLogger(EzPostgresDriver.class);

    private EzSecurityClient securityClient;

    static {
        try {
            DriverManager.registerDriver(new EzPostgresDriver());
        } catch (SQLException e) {
            logger.error("Failed to register EzPostgresDriver with DriverManager", e);
        }
    }

    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        if (!url.startsWith(JDBC_EZBAKE_URL_PREFIX)) {
            throw new SQLException(String.format("JDBC URL %s doesn't start with jdbc:ezbake:", url));
        }

        Properties queryProperties = getURLQueryParameterProperties(url);
        Properties cloneProperties = (Properties) properties.clone();
        cloneProperties.putAll(queryProperties);

        Provider<EzSecurityToken> tokenProvider;
        try {
            tokenProvider = getTokenProvider(cloneProperties);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        } catch (NoSuchMethodException e) {
            throw new SQLException(e);
        } catch (IllegalAccessException e) {
            throw new SQLException(e);
        } catch (InvocationTargetException e) {
            throw new SQLException(e);
        } catch (InstantiationException e) {
            throw new SQLException(e);
        } catch (TException e) {
            throw new SQLException(e);
        } catch (EzConfigurationLoaderException e) {
            throw new SQLException(e);
        }

        // Can't use the driver manager with an uberjar
        Driver realDriver = new org.postgresql.Driver();
        Connection realConnection = realDriver.connect(unwrapURL(url), cloneProperties);

        return new ConnectionDecorator(realConnection, tokenProvider);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url.startsWith(JDBC_EZBAKE_URL_PREFIX)) {
            try {
                unwrapDriver(url);

                return true;
            } catch (SQLException e) {
                return false;
            }
        }

        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
        return unwrapDriver(url).getPropertyInfo(unwrapURL(url), properties);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Remove the "ezbake" componnent from the scheme of a JDBC connection URL
     * @param url JDBC connection URL
     * @return URL without "ezbake" component in scheme
     */
    protected String unwrapURL(String url) {
        if (url.startsWith(JDBC_EZBAKE_URL_PREFIX)) {
            return JDBC_URL_PREFIX + url.substring(JDBC_EZBAKE_URL_PREFIX.length());
        } else {
            return url;
        }
    }

    /**
     * Return Properties extracted from URL query parameters.
     *
     * @param url a URL
     * @return Properties extracted from query parameters
     */
    protected Properties getURLQueryParameterProperties(String url) {
        // Oh Oracle, why is java.net.URL so broken...?
        Properties properties = new Properties();

        int queryInd = url.indexOf('?');
        if (queryInd < 0) {
            return properties;
        }

        String queryPart = url.substring(queryInd + 1);
        String[] parameters = queryPart.split("&");
        for (String p : parameters) {
            int ind = p.indexOf('=');
            if (ind >= 0) {
                String k = p.substring(0, ind);

                if (k.equals(EZBAKE_TOKEN_PROVIDER_PROPERTY) || k.equals(EZBAKE_TOKEN_PROPERTY)) {
                    String v = null;
                    try {
                        v = URLDecoder.decode(p.substring(ind + 1), URL_ENCODING);
                    } catch (UnsupportedEncodingException e) {
                        logger.error("Unsupported encoding while decoding query parameters", e);
                    }
                    properties.setProperty(k, v);
                }
            }
        }

        return properties;
    }

    /**
     * Get the Driver wrapped by this decorating driver.
     *
     * @param url JDBC connection URL
     * @return the wrapped Driver
     * @throws SQLException if the DriverManager cannot get the wrapped driver
     */
    protected Driver unwrapDriver(String url) throws SQLException {
        return DriverManager.getDriver(unwrapURL(url));
    }

    private synchronized EzSecurityClient getSecurityClient(Properties properties) throws
            EzConfigurationLoaderException {
        if (securityClient == null) {
            if (properties == null) {
                properties = new EzConfiguration().getProperties();
            }
            securityClient = new EzbakeSecurityClient(properties);
        }

        return securityClient;
    }

    /**
     * Returns the requested token provider based on JDBC connection properties. The properties are a combination of
     * properties passed to the DriverManager and are possibly overridden by query parameters in the JDBC connection
     * URL.
     *
     * @param properties JDBC connection properties
     * @return a token provider
     * @throws ClassNotFoundException if the token provider class is not found
     * @throws EzConfigurationLoaderException if the EzBake configuration cannot be loaded
     * @throws NoSuchMethodException if the token provider cannot be loaded
     * @throws IllegalAccessException if the token provider cannot be loaded
     * @throws InvocationTargetException if the token provider cannot be loaded
     * @throws InstantiationException if the token provider cannot be loaded
     * @throws TException if the explicit token cannot be deserialized
     */
    @SuppressWarnings("unchecked")
    private Provider<EzSecurityToken> getTokenProvider(Properties properties) throws ClassNotFoundException,
            EzConfigurationLoaderException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException, TException {
        String tokenProviderProperty = properties.getProperty(EZBAKE_TOKEN_PROVIDER_PROPERTY, DEFAULT_TOKEN_PROVIDER);
        Provider<EzSecurityToken> tokenProvider;
        if (tokenProviderProperty.equals(ExplicitTokenProvider.class.getName())) {
            String tokenValue = properties.getProperty(EZBAKE_TOKEN_PROPERTY);
            if (tokenValue == null) {
                throw new IllegalArgumentException(String.format(
                        "%s requires %s to be set", ExplicitTokenProvider.class.getName(), EZBAKE_TOKEN_PROPERTY));
            }

            tokenProvider = new ExplicitTokenProvider(getSecurityClient(properties),
                    ThriftUtils.deserializeFromBase64(EzSecurityToken.class, tokenValue));
        } else {
            tokenProvider = (Provider<EzSecurityToken>) Class.forName(tokenProviderProperty)
                    .getConstructor(EzSecurityClient.class)
                    .newInstance(getSecurityClient(properties));
        }

        return tokenProvider;
    }
}
