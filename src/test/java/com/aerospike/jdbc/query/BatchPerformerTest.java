package com.aerospike.jdbc.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.model.AerospikeQuery;
import com.aerospike.jdbc.model.DriverPolicy;
import com.aerospike.jdbc.util.AerospikeVersion;
import org.apache.calcite.sql.parser.SqlParseException;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.sql.Statement.EXECUTE_FAILED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class BatchPerformerTest {

    private static final String INSERT_1 = "INSERT INTO jdbc (__key, int1) VALUES ('k1', 1)";
    private static final String INSERT_2 = "INSERT INTO jdbc (__key, int1) VALUES ('k2', 2)";

    @Test(timeOut = 10000)
    public void testBatchOperateRuntimeException() throws SqlParseException {
        IllegalStateException failure = new IllegalStateException("operate failed");
        IAerospikeClient client = StubClient.create((method, args) -> {
            if (isBatchOperate(method)) {
                throw failure;
            }
            throw new UnsupportedOperationException(method.toString());
        });

        BatchUpdateException e = expectThrows(BatchUpdateException.class,
                () -> performer(client, true).execute(commands(INSERT_1, INSERT_2)));
        assertEquals(e.getUpdateCounts(), new int[]{EXECUTE_FAILED, EXECUTE_FAILED});
        assertEquals(e.getErrorCode(), ResultCode.CLIENT_ERROR);
        assertTrue(causeChainContains(e, failure), "cause chain must contain the original exception");
    }

    @Test(timeOut = 10000)
    public void testEmulatedBatchRuntimeException() throws SqlParseException {
        IllegalStateException failure = new IllegalStateException("operate failed");
        AtomicInteger calls = new AtomicInteger();
        IAerospikeClient client = StubClient.create((method, args) -> {
            if (isSingleOperate(method)) {
                if (calls.getAndIncrement() == 0) {
                    throw failure;
                }
                ((RecordListener) args[1]).onSuccess((Key) args[3], null);
                return null;
            }
            throw new UnsupportedOperationException(method.toString());
        });

        BatchUpdateException e = expectThrows(BatchUpdateException.class,
                () -> performer(client, false).execute(commands(INSERT_1, INSERT_2)));
        assertEquals(e.getUpdateCounts(), new int[]{EXECUTE_FAILED, 1});
        assertEquals(e.getErrorCode(), ResultCode.CLIENT_ERROR);
        assertEquals(calls.get(), 2);
    }

    @Test(timeOut = 10000)
    public void testEmulatedBatchFilteredOut() throws SqlParseException, SQLException {
        List<WritePolicy> updatePolicies = new ArrayList<>();
        List<WritePolicy> deletePolicies = new ArrayList<>();
        IAerospikeClient client = StubClient.create((method, args) -> {
            if (isSingleOperate(method)) {
                updatePolicies.add((WritePolicy) args[2]);
                ((RecordListener) args[1]).onFailure(new AerospikeException(ResultCode.FILTERED_OUT));
                return null;
            }
            if (StubClient.is(method, "delete", EventLoop.class, DeleteListener.class, WritePolicy.class,
                    Key.class)) {
                deletePolicies.add((WritePolicy) args[2]);
                ((DeleteListener) args[1]).onFailure(new AerospikeException(ResultCode.FILTERED_OUT));
                return null;
            }
            throw new UnsupportedOperationException(method.toString());
        });

        int[] counts = performer(client, false).execute(commands(
                "UPDATE jdbc SET int2=1 WHERE __key='k' AND int1=5",
                "DELETE FROM jdbc WHERE __key='k2' AND int1=5"));
        assertEquals(counts, new int[]{0, 0});
        assertEquals(updatePolicies.size(), 1);
        assertEquals(deletePolicies.size(), 1);
        assertTrue(updatePolicies.get(0).failOnFilteredOut);
        assertTrue(deletePolicies.get(0).failOnFilteredOut);
        assertNotNull(deletePolicies.get(0).filterExp);
    }

    private static boolean isBatchOperate(Method method) {
        return StubClient.is(method, "operate", EventLoop.class, BatchOperateListListener.class,
                BatchPolicy.class, List.class);
    }

    private static boolean isSingleOperate(Method method) {
        return StubClient.is(method, "operate", EventLoop.class, RecordListener.class, WritePolicy.class,
                Key.class, Operation[].class);
    }

    private static BatchPerformer performer(IAerospikeClient client, boolean batchOpsSupported) {
        AerospikeVersion version = new AerospikeVersion(null) {
            @Override
            public boolean isBatchOpsSupported() {
                return batchOpsSupported;
            }
        };
        return new BatchPerformer(client, null, new DriverPolicy(new Properties()), version);
    }

    private static List<BatchCommand> commands(String... sqls) throws SqlParseException {
        List<BatchCommand> commands = new ArrayList<>();
        for (String sql : Arrays.asList(sqls)) {
            AerospikeQuery query = AerospikeQuery.parse(sql, null);
            query.setCatalog("test");
            commands.add(BatchCommand.of(query));
        }
        return commands;
    }

    private static boolean causeChainContains(Throwable t, Throwable expected) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c == expected) {
                return true;
            }
        }
        return false;
    }
}
