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
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/** Wrapper for prepared statement that sets a security token as a database property before statement execution. */
class PreparedStatementDecorator extends StatementDecorator implements PreparedStatement {
    private PreparedStatement preparedStatement;

    /**
     * Wrap an existing prepared statement.
     *
     * @param preparedStatement statement to wrap
     * @param provider token provider
     */
    public PreparedStatementDecorator(PreparedStatement preparedStatement, Provider<EzSecurityToken> provider) {
        super(preparedStatement, provider);

        this.preparedStatement = preparedStatement;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        executeSetTokenProperty();

        return preparedStatement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        executeSetTokenProperty();

        return preparedStatement.executeUpdate();
    }

    @Override
    public void setNull(int i, int i2) throws SQLException {
        preparedStatement.setNull(i, i2);
    }

    @Override
    public void setBoolean(int i, boolean b) throws SQLException {
        preparedStatement.setBoolean(i, b);
    }

    @Override
    public void setByte(int i, byte b) throws SQLException {
        preparedStatement.setByte(i, b);
    }

    @Override
    public void setShort(int i, short i2) throws SQLException {
        preparedStatement.setShort(i, i2);
    }

    @Override
    public void setInt(int i, int i2) throws SQLException {
        preparedStatement.setInt(i, i2);
    }

    @Override
    public void setLong(int i, long l) throws SQLException {
        preparedStatement.setLong(i, l);
    }

    @Override
    public void setFloat(int i, float v) throws SQLException {
        preparedStatement.setFloat(i, v);
    }

    @Override
    public void setDouble(int i, double v) throws SQLException {
        preparedStatement.setDouble(i, v);
    }

    @Override
    public void setBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
        preparedStatement.setBigDecimal(i, bigDecimal);
    }

    @Override
    public void setString(int i, String s) throws SQLException {
        preparedStatement.setString(i, s);
    }

    @Override
    public void setBytes(int i, byte[] bytes) throws SQLException {
        preparedStatement.setBytes(i, bytes);
    }

    @Override
    public void setDate(int i, Date date) throws SQLException {
        preparedStatement.setDate(i, date);
    }

    @Override
    public void setTime(int i, Time time) throws SQLException {
        preparedStatement.setTime(i, time);
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) throws SQLException {
        preparedStatement.setTimestamp(i, timestamp);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, int i2) throws SQLException {
        preparedStatement.setAsciiStream(i, inputStream, i2);
    }

    @Override @SuppressWarnings("deprecation")
    public void setUnicodeStream(int i, InputStream inputStream, int i2) throws SQLException {
        preparedStatement.setUnicodeStream(i, inputStream, i2);
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, int i2) throws SQLException {
        preparedStatement.setBinaryStream(i, inputStream, i2);
    }

    @Override
    public void clearParameters() throws SQLException {
        preparedStatement.clearParameters();
    }

    @Override
    public void setObject(int i, Object o, int i2) throws SQLException {
        preparedStatement.setObject(i, o, i2);
    }

    @Override
    public void setObject(int i, Object o) throws SQLException {
        preparedStatement.setObject(i, o);
    }

    @Override
    public boolean execute() throws SQLException {
        executeSetTokenProperty();

        return preparedStatement.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        preparedStatement.addBatch();
    }

    @Override
    public void setCharacterStream(int i, Reader reader, int i2) throws SQLException {
        preparedStatement.setCharacterStream(i, reader, i2);
    }

    @Override
    public void setRef(int i, Ref ref) throws SQLException {
        preparedStatement.setRef(i, ref);
    }

    @Override
    public void setBlob(int i, Blob blob) throws SQLException {
        preparedStatement.setBlob(i, blob);
    }

    @Override
    public void setClob(int i, Clob clob) throws SQLException {
        preparedStatement.setClob(i, clob);
    }

    @Override
    public void setArray(int i, Array array) throws SQLException {
        preparedStatement.setArray(i, array);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return preparedStatement.getMetaData();
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar) throws SQLException {
        preparedStatement.setDate(i, date, calendar);
    }

    @Override
    public void setTime(int i, Time time, Calendar calendar) throws SQLException {
        preparedStatement.setTime(i, time, calendar);
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar) throws SQLException {
        preparedStatement.setTimestamp(i, timestamp, calendar);
    }

    @Override
    public void setNull(int i, int i2, String s) throws SQLException {
        preparedStatement.setNull(i, i2, s);
    }

    @Override
    public void setURL(int i, URL url) throws SQLException {
        preparedStatement.setURL(i, url);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return preparedStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int i, RowId rowId) throws SQLException {
        preparedStatement.setRowId(i, rowId);
    }

    @Override
    public void setNString(int i, String s) throws SQLException {
        preparedStatement.setNString(i, s);
    }

    @Override
    public void setNCharacterStream(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setNCharacterStream(i, reader, l);
    }

    @Override
    public void setNClob(int i, NClob nClob) throws SQLException {
        preparedStatement.setNClob(i, nClob);
    }

    @Override
    public void setClob(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setClob(i, reader, l);
    }

    @Override
    public void setBlob(int i, InputStream inputStream, long l) throws SQLException {
        preparedStatement.setBlob(i, inputStream, l);
    }

    @Override
    public void setNClob(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setNClob(i, reader, l);
    }

    @Override
    public void setSQLXML(int i, SQLXML sqlxml) throws SQLException {
        preparedStatement.setSQLXML(i, sqlxml);
    }

    @Override
    public void setObject(int i, Object o, int i2, int i3) throws SQLException {
        preparedStatement.setObject(i, o, i2, i3);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
        preparedStatement.setAsciiStream(i, inputStream, l);
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
        preparedStatement.setBinaryStream(i, inputStream, l);
    }

    @Override
    public void setCharacterStream(int i, Reader reader, long l) throws SQLException {
        preparedStatement.setCharacterStream(i, reader, l);
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream) throws SQLException {
        preparedStatement.setAsciiStream(i, inputStream);
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream) throws SQLException {
        preparedStatement.setBinaryStream(i, inputStream);
    }

    @Override
    public void setCharacterStream(int i, Reader reader) throws SQLException {
        preparedStatement.setCharacterStream(i, reader);
    }

    @Override
    public void setNCharacterStream(int i, Reader reader) throws SQLException {
        preparedStatement.setNCharacterStream(i, reader);
    }

    @Override
    public void setClob(int i, Reader reader) throws SQLException {
        preparedStatement.setClob(i, reader);
    }

    @Override
    public void setBlob(int i, InputStream inputStream) throws SQLException {
        preparedStatement.setBlob(i, inputStream);
    }

    @Override
    public void setNClob(int i, Reader reader) throws SQLException {
        preparedStatement.setNClob(i, reader);
    }
}
