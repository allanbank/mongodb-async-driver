/*
 * #%L
 * AbstractValidatingReplyCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.callback;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.CursorNotFoundException;
import com.allanbank.mongodb.error.QueryFailedException;
import com.allanbank.mongodb.error.ReplyException;
import com.allanbank.mongodb.error.ShardConfigStaleException;

/**
 * Helper class for constructing callbacks that convert a {@link Reply} message
 * into a different type of result.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractValidatingReplyCallback extends ReplyErrorHandler
        implements ReplyCallback {

    /**
     * Creates a new AbstractValidatingReplyCallback.
     */
    public AbstractValidatingReplyCallback() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to {@link #verify(Reply) verify} the reply and then
     * {@link #handle(Reply) handle} it.
     * </p>
     *
     * @see Callback#callback
     */
    @Override
    public void callback(final Reply result) {

        try {
            verify(result);
            handle(result);
        }
        catch (final MongoDbException error) {
            exception(error);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void exception(final Throwable thrown);

    /**
     * Called once the {@link Reply} has been validated.
     *
     * @param reply
     *            The {@link Reply} to be handled.
     */
    protected abstract void handle(Reply reply);

    /**
     * Checks the reply for an error message.
     *
     * @param reply
     *            The Reply to verify is successful.
     * @throws MongoDbException
     *             On a failure message in the reply.
     */
    protected void verify(final Reply reply) throws MongoDbException {
        if (reply.isCursorNotFound()) {
            throw new CursorNotFoundException(reply, asError(reply, true));
        }
        else if (reply.isQueryFailed()) {
            final MongoDbException error = asError(reply, true);
            if ((error == null) || (error.getClass() == ReplyException.class)) {
                throw new QueryFailedException(reply, error);
            }

            throw error;
        }
        else if (reply.isShardConfigStale()) {
            throw new ShardConfigStaleException(reply, asError(reply, true));
        }
        else {
            checkForError(reply);
        }
    }

}