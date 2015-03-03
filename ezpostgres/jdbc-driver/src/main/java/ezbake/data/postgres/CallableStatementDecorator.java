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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/**
 * Wrapper for callable statement that sets a security token as a database property before statement execution.
 */
class CallableStatementDecorator extends PreparedStatementDecorator implements CallableStatement {

    private CallableStatement callableStatement;

    /**
     * Wrap an existing callable statement.
     *
     * @param callableStatement statement to wrap
     * @param tokenProvider token provider
     */
    public CallableStatementDecorator(CallableStatement callableStatement, Provider<EzSecurityToken> tokenProvider) {
        super(callableStatement, tokenProvider);

        this.callableStatement = callableStatement;
    }

    @Override
    public void registerOutParameter(int i, int i2) throws SQLException {
        callableStatement.registerOutParameter(i, i2);
    }

    @Override
    public void registerOutParameter(int i, int i2, int i3) throws SQLException {
        callableStatement.registerOutParameter(i, i2, i3);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return callableStatement.wasNull();
    }

    @Override
    public String getString(int i) throws SQLException {
        return callableStatement.getString(i);
    }

    @Override
    public boolean getBoolean(int i) throws SQLException {
        return callableStatement.getBoolean(i);
    }

    @Override
    public byte getByte(int i) throws SQLException {
        return callableStatement.getByte(i);
    }

    @Override
    public short getShort(int i) throws SQLException {
        return callableStatement.getShort(i);
    }

    @Override
    public int getInt(int i) throws SQLException {
        return callableStatement.getInt(i);
    }

    @Override
    public long getLong(int i) throws SQLException {
        return callableStatement.getLong(i);
    }

    @Override
    public float getFloat(int i) throws SQLException {
        return callableStatement.getFloat(i);
    }

    @Override
    public double getDouble(int i) throws SQLException {
        return callableStatement.getDouble(i);
    }

    @Override @SuppressWarnings("deprecation")
    public BigDecimal getBigDecimal(int i, int i2) throws SQLException {
        return callableStatement.getBigDecimal(i, i2);
    }

    @Override
    public byte[] getBytes(int i) throws SQLException {
        return callableStatement.getBytes(i);
    }

    @Override
    public Date getDate(int i) throws SQLException {
        return callableStatement.getDate(i);
    }

    @Override
    public Time getTime(int i) throws SQLException {
        return callableStatement.getTime(i);
    }

    @Override
    public Timestamp getTimestamp(int i) throws SQLException {
        return callableStatement.getTimestamp(i);
    }

    @Override
    public Object getObject(int i) throws SQLException {
        return callableStatement.getObject(i);
    }

    @Override
    public BigDecimal getBigDecimal(int i) throws SQLException {
        return callableStatement.getBigDecimal(i);
    }

    @Override
    public Object getObject(int i, Map<String, Class<?>> typeMap) throws SQLException {
        return callableStatement.getObject(i, typeMap);
    }

    @Override
    public Ref getRef(int i) throws SQLException {
        return callableStatement.getRef(i);
    }

    @Override
    public Blob getBlob(int i) throws SQLException {
        return callableStatement.getBlob(i);
    }

    @Override
    public Clob getClob(int i) throws SQLException {
        return callableStatement.getClob(i);
    }

    @Override
    public Array getArray(int i) throws SQLException {
        return callableStatement.getArray(i);
    }

    @Override
    public Date getDate(int i, Calendar calendar) throws SQLException {
        return callableStatement.getDate(i, calendar);
    }

    @Override
    public Time getTime(int i, Calendar calendar) throws SQLException {
        return callableStatement.getTime(i, calendar);
    }

    @Override
    public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
        return callableStatement.getTimestamp(i, calendar);
    }

    @Override
    public void registerOutParameter(int i, int i2, String s) throws SQLException {
        callableStatement.registerOutParameter(i, i2, s);
    }

    @Override
    public void registerOutParameter(String s, int i) throws SQLException {
        callableStatement.registerOutParameter(s, i);
    }

    @Override
    public void registerOutParameter(String s, int i, int i2) throws SQLException {
        callableStatement.registerOutParameter(s, i, i2);
    }

    @Override
    public void registerOutParameter(String s, int i, String s2) throws SQLException {
        callableStatement.registerOutParameter(s, i, s2);
    }

    @Override
    public URL getURL(int i) throws SQLException {
        return callableStatement.getURL(i);
    }

    @Override
    public void setURL(String s, URL url) throws SQLException {
        callableStatement.setURL(s, url);
    }

    @Override
    public void setNull(String s, int i) throws SQLException {
        callableStatement.setNull(s, i);
    }

    @Override
    public void setBoolean(String s, boolean b) throws SQLException {
        callableStatement.setBoolean(s, b);
    }

    @Override
    public void setByte(String s, byte b) throws SQLException {
        callableStatement.setByte(s, b);
    }

    @Override
    public void setShort(String s, short i) throws SQLException {
        callableStatement.setShort(s, i);
    }

    @Override
    public void setInt(String s, int i) throws SQLException {
        callableStatement.setInt(s, i);
    }

    @Override
    public void setLong(String s, long l) throws SQLException {
        callableStatement.setLong(s, l);
    }

    @Override
    public void setFloat(String s, float v) throws SQLException {
        callableStatement.setFloat(s, v);
    }

    @Override
    public void setDouble(String s, double v) throws SQLException {
        callableStatement.setDouble(s, v);
    }

    @Override
    public void setBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
        callableStatement.setBigDecimal(s, bigDecimal);
    }

    @Override
    public void setString(String s, String s2) throws SQLException {
        callableStatement.setString(s, s2);
    }

    @Override
    public void setBytes(String s, byte[] bytes) throws SQLException {
        callableStatement.setBytes(s, bytes);
    }

    @Override
    public void setDate(String s, Date date) throws SQLException {
        callableStatement.setDate(s, date);
    }

    @Override
    public void setTime(String s, Time time) throws SQLException {
        callableStatement.setTime(s, time);
    }

    @Override
    public void setTimestamp(String s, Timestamp timestamp) throws SQLException {
        callableStatement.setTimestamp(s, timestamp);
    }

    @Override
    public void setAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
        callableStatement.setAsciiStream(s, inputStream, i);
    }

    @Override
    public void setBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
        callableStatement.setAsciiStream(s, inputStream, i);
    }

    @Override
    public void setObject(String s, Object o, int i, int i2) throws SQLException {
        callableStatement.setObject(s, o, i, i2);
    }

    @Override
    public void setObject(String s, Object o, int i) throws SQLException {
        callableStatement.setObject(s, o, i);
    }

    @Override
    public void setObject(String s, Object o) throws SQLException {
        callableStatement.setObject(s, o);
    }

    @Override
    public void setCharacterStream(String s, Reader reader, int i) throws SQLException {
        callableStatement.setCharacterStream(s, reader, i);
    }

    @Override
    public void setDate(String s, Date date, Calendar calendar) throws SQLException {
        callableStatement.setDate(s, date, calendar);
    }

    @Override
    public void setTime(String s, Time time, Calendar calendar) throws SQLException {
        callableStatement.setTime(s, time, calendar);
    }

    @Override
    public void setTimestamp(String s, Timestamp timestamp, Calendar calendar) throws SQLException {
        callableStatement.setTimestamp(s, timestamp, calendar);
    }

    @Override
    public void setNull(String s, int i, String s2) throws SQLException {
        callableStatement.setNull(s, i, s2);
    }

    @Override
    public String getString(String s) throws SQLException {
        return callableStatement.getString(s);
    }

    @Override
    public boolean getBoolean(String s) throws SQLException {
        return callableStatement.getBoolean(s);
    }

    @Override
    public byte getByte(String s) throws SQLException {
        return callableStatement.getByte(s);
    }

    @Override
    public short getShort(String s) throws SQLException {
        return callableStatement.getShort(s);
    }

    @Override
    public int getInt(String s) throws SQLException {
        return callableStatement.getInt(s);
    }

    @Override
    public long getLong(String s) throws SQLException {
        return callableStatement.getLong(s);
    }

    @Override
    public float getFloat(String s) throws SQLException {
        return callableStatement.getFloat(s);
    }

    @Override
    public double getDouble(String s) throws SQLException {
        return callableStatement.getDouble(s);
    }

    @Override
    public byte[] getBytes(String s) throws SQLException {
        return callableStatement.getBytes(s);
    }

    @Override
    public Date getDate(String s) throws SQLException {
        return callableStatement.getDate(s);
    }

    @Override
    public Time getTime(String s) throws SQLException {
        return callableStatement.getTime(s);
    }

    @Override
    public Timestamp getTimestamp(String s) throws SQLException {
        return callableStatement.getTimestamp(s);
    }

    @Override
    public Object getObject(String s) throws SQLException {
        return callableStatement.getObject(s);
    }

    @Override
    public BigDecimal getBigDecimal(String s) throws SQLException {
        return callableStatement.getBigDecimal(s);
    }

    @Override
    public Object getObject(String s, Map<String, Class<?>> typeMap) throws SQLException {
        return callableStatement.getObject(s, typeMap);
    }

    @Override
    public Ref getRef(String s) throws SQLException {
        return callableStatement.getRef(s);
    }

    @Override
    public Blob getBlob(String s) throws SQLException {
        return callableStatement.getBlob(s);
    }

    @Override
    public Clob getClob(String s) throws SQLException {
        return callableStatement.getClob(s);
    }

    @Override
    public Array getArray(String s) throws SQLException {
        return callableStatement.getArray(s);
    }

    @Override
    public Date getDate(String s, Calendar calendar) throws SQLException {
        return callableStatement.getDate(s, calendar);
    }

    @Override
    public Time getTime(String s, Calendar calendar) throws SQLException {
        return callableStatement.getTime(s, calendar);
    }

    @Override
    public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
        return callableStatement.getTimestamp(s, calendar);
    }

    @Override
    public URL getURL(String s) throws SQLException {
        return callableStatement.getURL(s);
    }

    @Override
    public RowId getRowId(int i) throws SQLException {
        return callableStatement.getRowId(i);
    }

    @Override
    public RowId getRowId(String s) throws SQLException {
        return callableStatement.getRowId(s);
    }

    @Override
    public void setRowId(String s, RowId rowId) throws SQLException {
        callableStatement.setRowId(s, rowId);
    }

    @Override
    public void setNString(String s, String s2) throws SQLException {
        callableStatement.setNString(s, s2);
    }

    @Override
    public void setNCharacterStream(String s, Reader reader, long l) throws SQLException {
        callableStatement.setNCharacterStream(s, reader, l);
    }

    @Override
    public void setNClob(String s, NClob nClob) throws SQLException {
        callableStatement.setNClob(s, nClob);
    }

    @Override
    public void setClob(String s, Reader reader, long l) throws SQLException {
        callableStatement.setClob(s, reader, l);
    }

    @Override
    public void setBlob(String s, InputStream inputStream, long l) throws SQLException {
        callableStatement.setBlob(s, inputStream, l);
    }

    @Override
    public void setNClob(String s, Reader reader, long l) throws SQLException {
        callableStatement.setNClob(s, reader, l);
    }

    @Override
    public NClob getNClob(int i) throws SQLException {
        return callableStatement.getNClob(i);
    }

    @Override
    public NClob getNClob(String s) throws SQLException {
        return callableStatement.getNClob(s);
    }

    @Override
    public void setSQLXML(String s, SQLXML sqlxml) throws SQLException {
        callableStatement.setSQLXML(s, sqlxml);
    }

    @Override
    public SQLXML getSQLXML(int i) throws SQLException {
        return callableStatement.getSQLXML(i);
    }

    @Override
    public SQLXML getSQLXML(String s) throws SQLException {
        return callableStatement.getSQLXML(s);
    }

    @Override
    public String getNString(int i) throws SQLException {
        return callableStatement.getNString(i);
    }

    @Override
    public String getNString(String s) throws SQLException {
        return callableStatement.getNString(s);
    }

    @Override
    public Reader getNCharacterStream(int i) throws SQLException {
        return callableStatement.getNCharacterStream(i);
    }

    @Override
    public Reader getNCharacterStream(String s) throws SQLException {
        return callableStatement.getNCharacterStream(s);
    }

    @Override
    public Reader getCharacterStream(int i) throws SQLException {
        return callableStatement.getCharacterStream(i);
    }

    @Override
    public Reader getCharacterStream(String s) throws SQLException {
        return callableStatement.getCharacterStream(s);
    }

    @Override
    public void setBlob(String s, Blob blob) throws SQLException {
        callableStatement.setBlob(s, blob);
    }

    @Override
    public void setClob(String s, Clob clob) throws SQLException {
        callableStatement.setClob(s, clob);
    }

    @Override
    public void setAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
        callableStatement.setAsciiStream(s, inputStream, l);
    }

    @Override
    public void setBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
        callableStatement.setBinaryStream(s, inputStream, l);
    }

    @Override
    public void setCharacterStream(String s, Reader reader, long l) throws SQLException {
        callableStatement.setCharacterStream(s, reader, l);
    }

    @Override
    public void setAsciiStream(String s, InputStream inputStream) throws SQLException {
        callableStatement.setAsciiStream(s, inputStream);
    }

    @Override
    public void setBinaryStream(String s, InputStream inputStream) throws SQLException {
        callableStatement.setBinaryStream(s, inputStream);
    }

    @Override
    public void setCharacterStream(String s, Reader reader) throws SQLException {
        callableStatement.setCharacterStream(s, reader);
    }

    @Override
    public void setNCharacterStream(String s, Reader reader) throws SQLException {
        callableStatement.setNCharacterStream(s, reader);
    }

    @Override
    public void setClob(String s, Reader reader) throws SQLException {
        callableStatement.setClob(s, reader);
    }

    @Override
    public void setBlob(String s, InputStream inputStream) throws SQLException {
        callableStatement.setBlob(s, inputStream);
    }

    @Override
    public void setNClob(String s, Reader reader) throws SQLException {
        callableStatement.setNClob(s, reader);
    }

    @Override
    public <T> T getObject(int i, Class<T> tClass) throws SQLException {
        return callableStatement.getObject(i, tClass);
    }

    @Override
    public <T> T getObject(String s, Class<T> tClass) throws SQLException {
        return callableStatement.getObject(s, tClass);
    }
}
