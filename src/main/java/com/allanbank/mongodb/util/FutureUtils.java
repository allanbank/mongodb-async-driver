/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.MongoDbException;

/**
 * FutureUtils provides helper methods for dealing with {@link Future}s.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class FutureUtils {

    /**
     * Unwraps the contents of the Future.
     *
     * @param <T>
     *            The type of the future and response.
     * @param future
     *            The future value to get.
     * @return The response from the Future.
     * @throws MongoDbException
     *             On an error from the Future.
     */
    public static <T> T unwrap(final Future<T> future) {
        try {
            return future.get();
        }
        catch (final InterruptedException e) {
            e.fillInStackTrace();
            throw new MongoDbException(e);
        }
        catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            cause.fillInStackTrace();
            if (cause instanceof MongoDbException) {
                throw (MongoDbException) cause;
            }
            throw new MongoDbException(cause);
        }
    }

    /**
     * Stop creation of a new FutureUtils.
     */
    private FutureUtils() {
        // Nothing.
    }
}