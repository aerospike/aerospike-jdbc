package com.aerospike.jdbc;

import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class JdbiQueriesTest extends JdbcBaseTest {

    @Test
    public void testJdbiInsertStringLiteral() {
        String id = UUID.randomUUID().toString();
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            int rowsInserted = handle.createUpdate(String.format("INSERT INTO test.foo (PK, k1, k2, k3, k4) VALUES ('%s', i1, i2, i3, i4)", id))
                    .execute();

            assertEquals(rowsInserted, 1);

            Map<String, Object> result = handle.createQuery(String.format("SELECT * FROM test.foo WHERE PK='%s'", id))
                    .mapToMap()
                    .first();

            assertEquals(result.get("pk"), id);
            assertEquals(result.get("k1"), "i1");
            assertEquals(result.get("k2"), "i2");
            assertEquals(result.get("k3"), "i3");
            assertEquals(result.get("k4"), "i4");

            return true;
        });
    }

    @Test
    public void testJdbiBindVariablesPositions() {
        String id = UUID.randomUUID().toString();
        String v1 = "v1";
        String v2 = "v2";
        String v3 = "v3";
        String v4 = "v4";
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            int rowsInserted = handle.createUpdate("INSERT INTO test.foo (PK, k1, k2, k3, k4) VALUES (?, ?, ?, ?, ?)")
                    .bind(0, id)
                    .bind(1, v1)
                    .bind(2, v2)
                    .bind(3, v3)
                    .bind(4, v4)
                    .execute();

            assertEquals(rowsInserted, 1);

            Map<String, Object> result = handle.createQuery("SELECT * FROM test.foo WHERE PK=?")
                    .bind(0, id)
                    .mapToMap()
                    .first();

            assertEquals(result.get("pk"), id);
            assertEquals(result.get("k1"), v1);
            assertEquals(result.get("k2"), v2);
            assertEquals(result.get("k3"), v3);
            assertEquals(result.get("k4"), v4);

            return true;
        });
    }

    @Test
    public void testJdbiBindVariablesNames() {
        String id = UUID.randomUUID().toString();
        String v1 = "v1";
        String v2 = "v2";
        String v3 = "v3";
        String v4 = "v4";
        Jdbi jdbi = Jdbi.create(connection);
        jdbi.withHandle(handle -> {
            int rowsInserted = handle.createUpdate("INSERT INTO test.foo (PK, k1, k2, k3, k4) VALUES (:id, :v1, :v2, :v3, :v4)")
                    .bind("id", id)
                    .bind("v1", v1)
                    .bind("v2", v2)
                    .bind("v3", v3)
                    .bind("v4", v4)
                    .execute();

            assertEquals(rowsInserted, 1);

            Map<String, Object> result = handle.createQuery("SELECT * FROM test.foo WHERE PK=:id")
                    .bind("id", id)
                    .mapToMap()
                    .first();

            assertEquals(result.get("pk"), id);
            assertEquals(result.get("k1"), v1);
            assertEquals(result.get("k2"), v2);
            assertEquals(result.get("k3"), v3);
            assertEquals(result.get("k4"), v4);

            return true;
        });
    }
}
