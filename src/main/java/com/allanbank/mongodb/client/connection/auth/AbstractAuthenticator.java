/*
 * #%L
 * AbstractAuthenticator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.auth;

import java.util.concurrent.ExecutionException;

import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * AbstractAuthenticator provides common functionality for all authenticators.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractAuthenticator {

    /** The result of the Authentication attempt. */
    protected FutureCallback<Boolean> myResults;

    /**
     * Creates a new AbstractAuthenticator.
     */
    public AbstractAuthenticator() {
        myResults = new FutureCallback<Boolean>();
    }

    /**
     * Returns true if the authenticator has completed, false otherwise.
     *
     * @return True if the authenticator has completed.
     */
    public boolean finished() {

        // Clear and restore the threads interrupted state for reconnect cases.
        final boolean interrupted = Thread.interrupted();
        try {
            return myResults.isDone();
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Returns the results of the authentication attempt.
     * <p>
     * Overridden to returns the results of the authentication, once complete.
     * </p>
     *
     * @return True if the user is successfully authenticated on the connection,
     *         false if the authentication fails.
     * @throws MongoDbAuthenticationException
     *             On a failure in the protocol to authenticate the user on the
     *             connection.
     */
    public boolean result() throws MongoDbAuthenticationException {

        // Clear and restore the threads interrupted state for reconnect cases.
        final boolean interrupted = Thread.interrupted();
        try {
            return myResults.get().booleanValue();
        }
        catch (final InterruptedException e) {
            throw new MongoDbAuthenticationException(e);
        }
        catch (final ExecutionException e) {
            throw new MongoDbAuthenticationException(e);
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}