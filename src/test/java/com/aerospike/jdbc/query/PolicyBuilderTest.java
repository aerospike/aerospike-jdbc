package com.aerospike.jdbc.query;

import com.aerospike.client.BatchDelete;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.model.AerospikeQuery;
import org.apache.calcite.sql.parser.SqlParseException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class PolicyBuilderTest {

    private final PolicyBuilder policyBuilder = new PolicyBuilder(StubClient.create());

    @Test
    public void testEmulatedBatchPolicyFailsOnFilteredOut() throws SqlParseException {
        AerospikeQuery update = parse("UPDATE jdbc SET int2=1 WHERE __key='k' AND int1=5");
        AerospikeQuery delete = parse("DELETE FROM jdbc WHERE __key='k' AND int1=5");
        Key key = new Key("test", "jdbc", "k");

        BatchWrite write = new BatchWrite(policyBuilder.buildBatchUpdateOnlyPolicy(), key,
                new Operation[]{Operation.put(new Bin("int2", 1))});
        WritePolicy writePolicy = policyBuilder.buildWritePolicy(write, update);
        assertTrue(writePolicy.failOnFilteredOut);
        assertEquals(writePolicy.filterExp, write.policy.filterExp);
        assertEquals(writePolicy.recordExistsAction, RecordExistsAction.UPDATE_ONLY);

        BatchDelete batchDelete = new BatchDelete(policyBuilder.buildBatchDeletePolicy(delete), key);
        writePolicy = policyBuilder.buildWritePolicy(batchDelete, delete);
        assertTrue(writePolicy.failOnFilteredOut);
        assertNotNull(writePolicy.filterExp);
        assertEquals(writePolicy.filterExp, batchDelete.policy.filterExp);
    }

    private static AerospikeQuery parse(String sql) throws SqlParseException {
        AerospikeQuery query = AerospikeQuery.parse(sql, null);
        query.setCatalog("test");
        return query;
    }
}
