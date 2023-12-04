package com.aerospike.jdbc;

import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

import static com.aerospike.jdbc.util.Constants.PRIMARY_KEY_COLUMN_NAME;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class JdbiQueriesTest extends JdbcBaseTest {

    @Test
    @Ignore
    public void testJdbiInsertStringLiteral() {
        String id = UUID.randomUUID().toString();
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            String query = format("INSERT INTO test.foo (%s, k1, k2, k3, k4) VALUES ('%s', 'i1', 'i2', 'i3', 'i4')",
                    PRIMARY_KEY_COLUMN_NAME, id);
            int rowsInserted = handle.createUpdate(query)
                    .execute();

            assertEquals(rowsInserted, 1);

            query = format("SELECT * FROM test.foo WHERE %s='%s'", PRIMARY_KEY_COLUMN_NAME, id);
            Map<String, Object> result = handle.createQuery(query)
                    .mapToMap()
                    .first();

            assertEquals(result.get(PRIMARY_KEY_COLUMN_NAME).toString(), id);
            assertEquals(result.get("k1"), "i1");
            assertEquals(result.get("k2"), "i2");
            assertEquals(result.get("k3"), "i3");
            assertEquals(result.get("k4"), "i4");

            return true;
        });
    }

    @Test
    @Ignore
    public void testJdbiBindVariablesPositions() {
        String id = UUID.randomUUID().toString();
        String v1 = "v1";
        String v2 = "v2";
        String v3 = "v3";
        String v4 = "v4";
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            String query = format("INSERT INTO test.foo (%s, k1, k2, k3, k4) VALUES (?, ?, ?, ?, ?)",
                    PRIMARY_KEY_COLUMN_NAME);
            int rowsInserted = handle.createUpdate(query)
                    .bind(0, id)
                    .bind(1, v1)
                    .bind(2, v2)
                    .bind(3, v3)
                    .bind(4, v4)
                    .execute();

            assertEquals(rowsInserted, 1);

            query = format("SELECT * FROM test.foo WHERE %s=?",
                    PRIMARY_KEY_COLUMN_NAME);
            Map<String, Object> result = handle.createQuery(query)
                    .bind(0, id)
                    .mapToMap()
                    .first();

            assertEquals(result.get(PRIMARY_KEY_COLUMN_NAME).toString(), id);
            assertEquals(result.get("k1"), v1);
            assertEquals(result.get("k2"), v2);
            assertEquals(result.get("k3"), v3);
            assertEquals(result.get("k4"), v4);

            return true;
        });
    }

    @Test
    @Ignore
    public void testJdbiBindVariablesNames() {
        String id = UUID.randomUUID().toString();
        String v1 = "v1";
        String v2 = "v2";
        String v3 = "v3";
        String v4 = "v4";
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            String query = format("INSERT INTO test.foo (%s, k1, k2, k3, k4) VALUES (:id, :v1, :v2, :v3, :v4)",
                    PRIMARY_KEY_COLUMN_NAME);
            int rowsInserted = handle.createUpdate(query)
                    .bind("id", id)
                    .bind("v1", v1)
                    .bind("v2", v2)
                    .bind("v3", v3)
                    .bind("v4", v4)
                    .execute();

            assertEquals(rowsInserted, 1);

            query = format("SELECT * FROM test.foo WHERE %s=:id",
                    PRIMARY_KEY_COLUMN_NAME);
            Map<String, Object> result = handle.createQuery(query)
                    .bind("id", id)
                    .mapToMap()
                    .first();

            assertEquals(result.get(PRIMARY_KEY_COLUMN_NAME).toString(), id);
            assertEquals(result.get("k1"), v1);
            assertEquals(result.get("k2"), v2);
            assertEquals(result.get("k3"), v3);
            assertEquals(result.get("k4"), v4);

            return true;
        });
    }
}
