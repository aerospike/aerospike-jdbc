package com.aerospike.jdbc.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * A no-server {@link IAerospikeClient} stub: default policies are built in, every other call is
 * delegated to a handler.
 */
final class StubClient {

    interface Handler {
        Object invoke(Method method, Object[] args) throws Throwable;
    }

    private StubClient() {
    }

    static IAerospikeClient create() {
        return create((method, args) -> {
            throw new UnsupportedOperationException(method.toString());
        });
    }

    static IAerospikeClient create(Handler handler) {
        WritePolicy writePolicy = new WritePolicy();
        BatchPolicy batchPolicy = new BatchPolicy();
        ScanPolicy scanPolicy = new ScanPolicy();
        QueryPolicy queryPolicy = new QueryPolicy();
        return (IAerospikeClient) Proxy.newProxyInstance(IAerospikeClient.class.getClassLoader(),
                new Class<?>[]{IAerospikeClient.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getWritePolicyDefault":
                            return writePolicy;
                        case "getBatchPolicyDefault":
                            return batchPolicy;
                        case "getScanPolicyDefault":
                            return scanPolicy;
                        case "getQueryPolicyDefault":
                            return queryPolicy;
                        case "toString":
                            return "StubClient";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            return handler.invoke(method, args);
                    }
                });
    }

    static boolean is(Method method, String name, Class<?>... parameterTypes) {
        if (!method.getName().equals(name) || method.getParameterCount() != parameterTypes.length) {
            return false;
        }
        Class<?>[] actual = method.getParameterTypes();
        for (int i = 0; i < actual.length; i++) {
            if (actual[i] != parameterTypes[i]) {
                return false;
            }
        }
        return true;
    }
}
