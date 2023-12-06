package com.aerospike.jdbc.util;

import com.aerospike.client.Log;

import java.util.logging.Logger;

import static java.lang.String.format;

public class AerospikeClientLogger implements Log.Callback {

    private static final Logger logger = Logger.getLogger(AerospikeClientLogger.class.getName());

    @Override
    public void log(Log.Level level, String message) {
        switch (level) {
            case DEBUG:
                logger.fine(message);
                break;

            case INFO:
                logger.info(message);
                break;

            case WARN:
                logger.warning(message);
                break;

            case ERROR:
                logger.severe(message);
                break;

            default:
                logger.warning(() -> format("Unexpected Aerospike client log level %s. Message: %s",
                        level, message));
        }
    }
}
