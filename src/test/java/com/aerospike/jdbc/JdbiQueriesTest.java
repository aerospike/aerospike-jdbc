package com.aerospike.jdbc;

import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static com.aerospike.jdbc.util.TestConfig.TABLE_NAME;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * JDBI on top of this driver. Uses the same {@link com.aerospike.jdbc.util.TestConfig#TABLE_NAME} schema as other integration tests.
 */
public class JdbiQueriesTest extends JdbcBaseTest {

    @Test
    public void testJdbiInsertStringLiteral() {
        String id = UUID.randomUUID().toString();
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            try {
                String query = format(
                        "INSERT INTO %s (%s, bool1, int1, int2, str1) VALUES ('%s', false, 1, 2, 'literal')",
                        TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, id);
                int rowsInserted = handle.createUpdate(query).execute();

                assertEquals(rowsInserted, 1);

                query = format("SELECT * FROM %s WHERE %s='%s'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, id);
                Map<String, Object> result = handle.createQuery(query)
                        .mapToMap()
                        .first();

                assertEquals(result.get(PRIMARY_KEY_COLUMN_NAME).toString(), id);
                assertFalse((Boolean) result.get("bool1"));
                assertEquals(((Number) result.get("int1")).intValue(), 1);
                assertEquals(((Number) result.get("int2")).intValue(), 2);
                assertEquals(result.get("str1"), "literal");
            } finally {
                handle.createUpdate(format("DELETE FROM %s WHERE %s='%s'", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME, id))
                        .execute();
            }
            return true;
        });
    }

    @Test
    public void testJdbiBindVariablesPositions() {
        String id = UUID.randomUUID().toString();
        boolean bool1 = false;
        int int1 = 11;
        int int2 = 22;
        String str1 = "jdbi-pos";
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            try {
                String query = format("INSERT INTO %s (%s, bool1, int1, int2, str1) VALUES (?, ?, ?, ?, ?)",
                        TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
                int rowsInserted = handle.createUpdate(query)
                        .bind(0, id)
                        .bind(1, bool1)
                        .bind(2, int1)
                        .bind(3, int2)
                        .bind(4, str1)
                        .execute();

                assertEquals(rowsInserted, 1);

                query = format("SELECT * FROM %s WHERE %s=?",
                        TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
                Map<String, Object> result = handle.createQuery(query)
                        .bind(0, id)
                        .mapToMap()
                        .first();

                assertEquals(result.get(PRIMARY_KEY_COLUMN_NAME).toString(), id);
                assertEquals(result.get("bool1"), bool1);
                assertEquals(((Number) result.get("int1")).intValue(), int1);
                assertEquals(((Number) result.get("int2")).intValue(), int2);
                assertEquals(result.get("str1"), str1);
            } finally {
                handle.createUpdate(format("DELETE FROM %s WHERE %s=?", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME))
                        .bind(0, id)
                        .execute();
            }
            return true;
        });
    }

    @Test
    public void testJdbiBindVariablesNames() {
        String id = UUID.randomUUID().toString();
        boolean bool1 = true;
        int int1 = 33;
        int int2 = 44;
        String str1 = "jdbi-named";
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            try {
                String query = format(
                        "INSERT INTO %s (%s, bool1, int1, int2, str1) VALUES (:pk, :bool1, :int1, :int2, :str1)",
                        TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
                int rowsInserted = handle.createUpdate(query)
                        .bind("pk", id)
                        .bind("bool1", bool1)
                        .bind("int1", int1)
                        .bind("int2", int2)
                        .bind("str1", str1)
                        .execute();

                assertEquals(rowsInserted, 1);

                query = format("SELECT * FROM %s WHERE %s=:pk",
                        TABLE_NAME, PRIMARY_KEY_COLUMN_NAME);
                Map<String, Object> result = handle.createQuery(query)
                        .bind("pk", id)
                        .mapToMap()
                        .first();

                assertEquals(result.get(PRIMARY_KEY_COLUMN_NAME).toString(), id);
                assertEquals(result.get("bool1"), bool1);
                assertEquals(((Number) result.get("int1")).intValue(), int1);
                assertEquals(((Number) result.get("int2")).intValue(), int2);
                assertEquals(result.get("str1"), str1);
            } finally {
                handle.createUpdate(format("DELETE FROM %s WHERE %s=:pk", TABLE_NAME, PRIMARY_KEY_COLUMN_NAME))
                        .bind("pk", id)
                        .execute();
            }
            return true;
        });
    }
}
