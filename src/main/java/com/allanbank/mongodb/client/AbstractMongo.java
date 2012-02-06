/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.MongoDbException;

/**
 * AbstractMongo provides common methods for all MongoDB client classes.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AbstractMongo {

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
    protected <T> T unwrap(final Future<T> future) {
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

}