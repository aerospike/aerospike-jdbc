package com.aerospike.jdbc.util;

import org.testng.Assert;

public class Asserts {

    private Asserts() {
    }

    public static <T extends Throwable> T expectThrowsCause(Class<T> throwableClass, Assert.ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable t) {
            Throwable cause = t.getCause();
            if (cause == null) {
                throw new AssertionError("Throwable cause is null");
            }

            if (throwableClass.isInstance(cause)) {
                return throwableClass.cast(cause);
            }

            String mismatchMessage = String.format("Expected %s to be thrown, but %s was thrown",
                    throwableClass.getSimpleName(), cause.getClass().getSimpleName());
            throw new AssertionError(mismatchMessage, cause);
        }

        String message = String.format("Expected %s to be thrown, but nothing was thrown",
                throwableClass.getSimpleName());
        throw new AssertionError(message);
    }
}
