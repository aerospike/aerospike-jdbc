package com.aerospike.jdbc.util;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import static java.sql.Types.NULL;
import static java.sql.Types.OTHER;
import static java.util.stream.Collectors.toMap;

public class SqlLiterals {

    public static final Map<Class<?>, Integer> sqlTypes = new HashMap<>();

    static {
        sqlTypes.put(Short.class, Types.SMALLINT);
        sqlTypes.put(Integer.class, Types.INTEGER);
        sqlTypes.put(Long.class, Types.BIGINT);
        sqlTypes.put(Boolean.class, Types.BOOLEAN);
        sqlTypes.put(Float.class, Types.FLOAT);
        sqlTypes.put(Double.class, Types.DOUBLE);
        sqlTypes.put(short.class, Types.SMALLINT);
        sqlTypes.put(int.class, Types.INTEGER);
        sqlTypes.put(long.class, Types.BIGINT);
        sqlTypes.put(boolean.class, Types.BOOLEAN);
        sqlTypes.put(float.class, Types.FLOAT);
        sqlTypes.put(double.class, Types.DOUBLE);
        sqlTypes.put(String.class, Types.VARCHAR);
        sqlTypes.put(byte[].class, Types.BLOB);
        sqlTypes.put(java.sql.Date.class, Types.DATE);
        sqlTypes.put(java.sql.Time.class, Types.TIME);
        sqlTypes.put(java.sql.Timestamp.class, Types.TIMESTAMP);
        sqlTypes.put(ArrayList.class, Types.ARRAY);
        sqlTypes.put(null, NULL);
        sqlTypes.put(Object.class, OTHER);
    }

    public static final Map<Integer, String> sqlTypeNames = new HashMap<>();

    static {
        sqlTypeNames.put(Types.SMALLINT, "short");
        sqlTypeNames.put(Types.INTEGER, "integer");
        sqlTypeNames.put(Types.BIGINT, "long");
        sqlTypeNames.put(Types.BOOLEAN, "boolean");
        sqlTypeNames.put(Types.FLOAT, "float");
        sqlTypeNames.put(Types.DOUBLE, "double");
        sqlTypeNames.put(Types.VARCHAR, "varchar");
        sqlTypeNames.put(Types.BLOB, "blob");
        sqlTypeNames.put(Types.DATE, "date");
        sqlTypeNames.put(Types.TIME, "time");
        sqlTypeNames.put(Types.TIMESTAMP, "timestamp");
        sqlTypeNames.put(Types.ARRAY, "list");
    }

    public static final Map<String, Integer> sqlTypeByName;

    static {
        sqlTypeByName = sqlTypeNames.entrySet().stream()
                .filter(name -> name.getKey() != Types.ARRAY)
                .collect(toMap(Entry::getValue, Entry::getKey, (v1, v2) -> v1, TreeMap::new));

        sqlTypeByName.put("bigint", Types.BIGINT);
        sqlTypeByName.put("smallint", Types.SMALLINT);
    }

    public static final Map<Integer, Class<?>> sqlToJavaTypes = new HashMap<>();

    static {
        sqlToJavaTypes.put(Types.SMALLINT, Short.class);
        sqlToJavaTypes.put(Types.INTEGER, Integer.class);
        sqlToJavaTypes.put(Types.BIGINT, Long.class);
        sqlToJavaTypes.put(Types.BOOLEAN, Boolean.class);
        sqlToJavaTypes.put(Types.FLOAT, Float.class);
        sqlToJavaTypes.put(Types.DOUBLE, Double.class);
        sqlToJavaTypes.put(Types.VARCHAR, String.class);
        sqlToJavaTypes.put(Types.LONGVARCHAR, String.class);
        sqlToJavaTypes.put(Types.BLOB, byte[].class);
        sqlToJavaTypes.put(Types.BINARY, byte[].class);
        sqlToJavaTypes.put(Types.VARBINARY, byte[].class);
        sqlToJavaTypes.put(Types.LONGVARBINARY, byte[].class);
        sqlToJavaTypes.put(Types.DATE, java.sql.Date.class);
        sqlToJavaTypes.put(Types.TIME, java.sql.Time.class);
        sqlToJavaTypes.put(Types.TIMESTAMP, java.sql.Timestamp.class);
    }

    public static int getSqlType(Object value) {
        return value == null ? NULL : getSqlType(value.getClass());
    }

    public static int getSqlType(Class<?> clazz) {
        return clazz == null ? NULL : sqlTypes.getOrDefault(clazz, OTHER);
    }
}
