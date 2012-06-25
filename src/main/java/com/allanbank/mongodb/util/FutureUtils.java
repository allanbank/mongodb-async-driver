/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.MongoDbException;

/**
 * FutureUtils provides helper methods for dealing with {@link Future}s.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
            throw new MongoDbException(e);
        }
        catch (final ExecutionException e) {
            if (e.getCause() instanceof MongoDbException) {
                throw (MongoDbException) e.getCause();
            }
            throw new MongoDbException(e.getCause());
        }
    }

    /**
     * Stop creation of a new FutureUtils.
     */
    private FutureUtils() {
        // Nothing.
    }

}