package com.aerospike.jdbc.sql.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;

import static java.lang.String.format;

public class ByteArrayBlob implements Blob {

    private static final byte[] EMPTY = new byte[0];
    private volatile byte[] data;

    public ByteArrayBlob() {
        this(EMPTY);
    }

    public ByteArrayBlob(byte[] data) {
        this.data = data;
    }

    // Source: https://stackoverflow.com/questions/21341027/find-indexof-a-byte-array-within-another-byte-array
    private static int indexOf(byte[] source, int sourceOffset, int sourceCount, byte[] target,
                               int targetOffset, int targetCount, int fromIndex) {
        if (fromIndex >= sourceCount) {
            return (targetCount == 0 ? sourceCount : -1);
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        if (targetCount == 0) {
            return fromIndex;
        }

        byte first = target[targetOffset];
        int max = sourceOffset + (sourceCount - targetCount);

        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            /* Look for first character. */
            if (source[i] != first) {
                while (++i <= max && source[i] != first) ;
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && source[j] == target[k]; j++, k++) ;

                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

    @Override
    public long length() {
        return data.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
        if (length < 0) {
            throw new SQLException(format("Length must be >= 0 but was %d", length));
        }
        int from = (int) pos - 1;
        return Arrays.copyOfRange(data, from, from + length);
    }

    @Override
    public InputStream getBinaryStream() {
        return new ByteArrayInputStream(data);
    }

    @Override
    public long position(byte[] pattern, long start) {
        if (start > length()) {
            return -1;
        }
        int index = indexOf(data, (int) start - 1, data.length, pattern, 0, pattern.length, 0);
        return index >= 0 ? index + 1 : index;
    }

    @Override
    public long position(Blob pattern, long start) {
        return position(((ByteArrayBlob) pattern).data, start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
        return setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
        if (offset < 0) {
            throw new SQLException(format("Offset cannot be negative but was %d", offset));
        }
        int blobOffset = (int) pos - 1;
        int n = blobOffset + bytes.length - offset;
        byte[] newData = new byte[n];
        System.arraycopy(data, 0, newData, 0, blobOffset);
        System.arraycopy(bytes, offset, newData, blobOffset, bytes.length - offset);
        data = newData;
        return n;
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                try {
                    setBytes(pos, toByteArray());
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public void truncate(long len) throws SQLException {
        if (len > Integer.MAX_VALUE || len < 1) {
            throw new SQLException(format("Length must be between 0 and %d but was %d", Integer.MAX_VALUE, len));
        }
        @SuppressWarnings("UnnecessaryLocalVariable") // otherwise the assignment is not atomic.
        byte[] newData = Arrays.copyOf(data, (int) len);
        data = newData;
    }

    @Override
    public void free() {
        data = EMPTY;
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(getBytes(pos, (int) length));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Arrays.equals(data, ((ByteArrayBlob) o).data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
