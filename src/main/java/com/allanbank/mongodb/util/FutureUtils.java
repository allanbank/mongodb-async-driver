/*
 * #%L
 * FutureUtils.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.MongoDbException;

/**
 * FutureUtils provides helper methods for dealing with {@link Future}s.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
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