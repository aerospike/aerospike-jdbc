package com.aerospike.jdbc.sql.type;

import java.io.*;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.Objects;

import static java.lang.String.format;

public class StringClob implements NClob {

    private String data;

    public StringClob() {
        this("");
    }

    public StringClob(String data) {
        this.data = data;
    }

    @Override
    public long length() {
        return data.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        validatePosition(pos);
        int from = (int) pos - 1;
        return data.substring(from, from + length);
    }

    @Override
    public Reader getCharacterStream() {
        return new StringReader(data);
    }

    @Override
    public InputStream getAsciiStream() {
        return new ByteArrayInputStream(data.getBytes());
    }

    @Override
    public long position(String searchStr, long start) throws SQLException {
        validatePosition(start);
        int from = (int) start - 1;
        int foundIndex = data.indexOf(searchStr, from);
        return foundIndex < 0 ? foundIndex : foundIndex + 1;
    }

    @Override
    public long position(Clob searchStr, long start) throws SQLException {
        return position(((StringClob) searchStr).data, start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return setString(pos, str, 0, str.length());
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        validatePosition(pos);
        if (offset < 0) {
            throw new SQLException(format("Offset cannot be negative but was %d", offset));
        }
        int till = (int) pos;
        data = (data.length() >= till ? data.substring(0, till) : data) + str.substring(offset, offset + len);
        return len - offset;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        validatePosition(pos);
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                try {
                    setString(pos, new String(toByteArray()));
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public Writer setCharacterStream(long pos) {
        return new StringWriter() {
            @Override
            public void close() throws IOException {
                super.close();
                try {
                    setString(pos, getBuffer().toString());
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public void truncate(long len) {
        data = data.substring(0, (int) len);
    }

    @Override
    public void free() {
        data = "";
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(getSubString(pos, (int) length));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Objects.equals(data, ((StringClob) o).data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    private void validatePosition(long pos) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
    }
}
