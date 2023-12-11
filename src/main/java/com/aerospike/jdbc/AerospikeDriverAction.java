package com.aerospike.jdbc;

import com.aerospike.jdbc.async.EventLoopProvider;

import java.sql.DriverAction;
import java.util.logging.Logger;

public class AerospikeDriverAction implements DriverAction {

    private static final Logger logger = Logger.getLogger(AerospikeDriverAction.class.getName());

    @Override
    public void deregister() {
        logger.info("Deregister AerospikeDriver");
        EventLoopProvider.close();
    }
}
