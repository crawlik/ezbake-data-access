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

import javax.inject.Provider;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

class ConnectionDecorator implements Connection {

    private Connection connection;
    private Provider<EzSecurityToken> tokenProvider;

    public ConnectionDecorator(Connection connection, Provider<EzSecurityToken> tokenProvider) {
        this.connection = connection;
        this.tokenProvider = tokenProvider;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new StatementDecorator(connection.createStatement(), tokenProvider);
    }

    @Override
    public PreparedStatement prepareStatement(String s) throws SQLException {
        return new PreparedStatementDecorator(connection.prepareStatement(s), tokenProvider);
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        return new CallableStatementDecorator(connection.prepareCall(s), tokenProvider);
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        return connection.nativeSQL(s);
    }

    @Override
    public void setAutoCommit(boolean b) throws SQLException {
        connection.setAutoCommit(b);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return connection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        connection.rollback();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {
        connection.setReadOnly(b);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public void setCatalog(String s) throws SQLException {
        connection.setCatalog(s);
    }

    @Override
    public String getCatalog() throws SQLException {
        return connection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {
        connection.setTransactionIsolation(i);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        connection.clearWarnings();
    }

    @Override
    public Statement createStatement(int i, int i2) throws SQLException {
        return new StatementDecorator(connection.createStatement(i, i2), tokenProvider);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i2) throws SQLException {
        return new PreparedStatementDecorator(connection.prepareStatement(s, i, i2), tokenProvider);
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i2) throws SQLException {
        return new CallableStatementDecorator(connection.prepareCall(s, i, i2), tokenProvider);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return connection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> typeMap) throws SQLException {
        connection.setTypeMap(typeMap);
    }

    @Override
    public void setHoldability(int i) throws SQLException {
        connection.setHoldability(i);
    }

    @Override
    public int getHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return connection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        return connection.setSavepoint(s);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        connection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        connection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int i, int i2, int i3) throws SQLException {
        return new StatementDecorator(connection.createStatement(i, i2, i3), tokenProvider);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i2, int i3) throws SQLException {
        return new PreparedStatementDecorator(connection.prepareStatement(s, i, i2, i3), tokenProvider);
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i2, int i3) throws SQLException {
        return new CallableStatementDecorator(connection.prepareCall(s, i, i2, i3), tokenProvider);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i) throws SQLException {
        return new PreparedStatementDecorator(connection.prepareStatement(s, i), tokenProvider);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
        return new PreparedStatementDecorator(connection.prepareStatement(s, ints), tokenProvider);
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        return new PreparedStatementDecorator(connection.prepareStatement(s, strings), tokenProvider);
    }

    @Override
    public Clob createClob() throws SQLException {
        return connection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return connection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return connection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return connection.createSQLXML();
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        return connection.isValid(i);
    }

    @Override
    public void setClientInfo(String s, String s2) throws SQLClientInfoException {
        connection.setClientInfo(s, s2);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        connection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        return connection.getClientInfo(s);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return connection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        return connection.createArrayOf(s, objects);
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        return connection.createStruct(s, objects);
    }

    @Override
    public void setSchema(String s) throws SQLException {
        connection.setSchema(s);
    }

    @Override
    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        connection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int i) throws SQLException {
        connection.setNetworkTimeout(executor, i);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return connection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> tClass) throws SQLException {
        return connection.unwrap(tClass);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return connection.isWrapperFor(aClass);
    }
}
