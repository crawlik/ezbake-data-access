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
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TException;

import javax.inject.Provider;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * A statement that sets an EzBake security token as a database property before executing any statements. Wraps a real
 * JDBC statement.
 */
class StatementDecorator implements Statement {

    /** Database property to set token to */
    public static final String SECURITY_TOKEN_PROPERTY_NAME = "ezbake.token";

    private Statement statement;
    private Provider<EzSecurityToken> tokenProvider;

    /**
     * Wrap an existing JDBC statement.
     *
     * @param statement statement to wrap
     * @param tokenProvider token provider
     */
    public StatementDecorator(Statement statement, Provider<EzSecurityToken> tokenProvider) {
        this.statement = statement;
        this.tokenProvider = tokenProvider;
    }

    /**
     * Set security token to database property. The property is set to the base64 encoded EzSecurityToken object
     * provided by the token provider.
     *
     * @throws SQLException if the property-setting statement could not be executed
     */
    protected void executeSetTokenProperty() throws SQLException {
        EzSecurityToken token = tokenProvider.get();
        if (token == null) {
            throw new SQLException("Could not get security token from token provider");
        }

        String serializedToken;
        try {
            serializedToken = ThriftUtils.serializeToBase64(token);
        } catch (TException e) {
            throw new SQLException(e);
        }

        // In case the wrapped statement is a PreparedStatement or a CallableStatement, in which case it can't take a
        // query string on its own. The connection we're getting is from the wrapped (true) implementation.
        Statement propertyStatement = getConnection().createStatement();
        propertyStatement.execute(String.format("set %s = '%s';", SECURITY_TOKEN_PROPERTY_NAME, serializedToken));
        propertyStatement.close();
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        executeSetTokenProperty();

        return statement.executeQuery(s);
    }

    @Override
    public int executeUpdate(String s) throws SQLException {
        executeSetTokenProperty();

        return statement.executeUpdate(s);
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {
        statement.setMaxFieldSize(i);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int i) throws SQLException {
        statement.setMaxRows(i);
    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {
        statement.setEscapeProcessing(b);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {
        statement.setQueryTimeout(i);
    }

    @Override
    public void cancel() throws SQLException {
        statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        statement.clearWarnings();
    }

    @Override
    public void setCursorName(String s) throws SQLException {
        statement.setCursorName(s);
    }

    @Override
    public boolean execute(String s) throws SQLException {
        executeSetTokenProperty();

        return statement.execute(s);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return statement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {
        statement.setFetchDirection(i);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
        statement.setFetchSize(i);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    public void addBatch(String s) throws SQLException {
        statement.addBatch(s);
    }

    @Override
    public void clearBatch() throws SQLException {
        statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        executeSetTokenProperty();

        return statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return statement.getConnection();
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        return statement.getMoreResults(i);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return statement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String s, int i) throws SQLException {
        executeSetTokenProperty();

        return statement.executeUpdate(s, i);
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        executeSetTokenProperty();

        return statement.executeUpdate(s, ints);
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        executeSetTokenProperty();

        return statement.executeUpdate(s, strings);
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        executeSetTokenProperty();

        return statement.execute(s, i);
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        executeSetTokenProperty();

        return statement.execute(s, ints);
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        executeSetTokenProperty();

        return statement.execute(s, strings);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return statement.isClosed();
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {
        statement.setPoolable(b);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return statement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        statement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return statement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> tClass) throws SQLException {
        return statement.unwrap(tClass);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return statement.isWrapperFor(aClass);
    }

    @Override
    public String toString() {
        return statement.toString();
    }
}
