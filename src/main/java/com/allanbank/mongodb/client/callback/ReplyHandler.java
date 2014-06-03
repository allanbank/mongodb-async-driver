/*
 * #%L
 * ReplyHandler.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.allanbank.mongodb.client.message.Reply;

/**
 * ReplyHandler provides the capability to properly handle the replies to a
 * callback.
 * 
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyHandler implements Runnable {

    /** The socket that we are receiving for. */
    private static final ThreadLocal<Receiver> ourReceiver = new ThreadLocal<Receiver>();

    /**
     * Raise an error on the callback, if any. Will execute the request on a
     * background thread if provided.
     * 
     * @param exception
     *            The thrown exception.
     * @param replyCallback
     *            The callback for the reply to the message.
     * @param executor
     *            The executor to use for the back-grounding the reply handling.
     */
    public static void raiseError(final Throwable exception,
            final ReplyCallback replyCallback, final Executor executor) {
        if (replyCallback != null) {
            if (executor != null) {
                try {
                    executor.execute(new ReplyHandler(replyCallback, exception));
                }
                catch (final RejectedExecutionException rej) {
                    // Run on this thread.
                    replyCallback.exception(exception);
                }
            }
            else {
                replyCallback.exception(exception);
            }
        }
    }

    /**
     * Updates to set the reply for the callback, if any.
     * 
     * @param receiver
     *            The socket receiving the message.
     * @param reply
     *            The reply.
     * @param replyCallback
     *            The callback for the reply to the message.
     * @param executor
     *            The executor to use for the back-grounding the reply handling.
     */
    public static void reply(final Receiver receiver, final Reply reply,
            final ReplyCallback replyCallback, final Executor executor) {
        if (replyCallback != null) {
            // We know the FutureCallback will not block or take long to process
            // so just use this thread in that case.
            final boolean lightWeight = replyCallback.isLightWeight();
            if (!lightWeight && (executor != null)) {
                try {
                    executor.execute(new ReplyHandler(replyCallback, reply));
                }
                catch (final RejectedExecutionException rej) {
                    // Run on this thread.
                    run(receiver, reply, replyCallback);
                }
            }
            else {
                run(receiver, reply, replyCallback);
            }
        }
    }

    /**
     * If there is a pending reply tries to process that reply.
     */
    public static void tryReceive() {
        final Receiver receiver = ourReceiver.get();

        if (receiver != null) {
            receiver.tryReceive();
        }
    }

    /**
     * Runs the callback on the current thread.
     * 
     * @param receiver
     *            The receiver to be run.
     * @param reply
     *            The reply to the provide to the callback.
     * @param replyCallback
     *            The reply callback.
     */
    private static void run(final Receiver receiver, final Reply reply,
            final ReplyCallback replyCallback) {
        final Receiver before = ourReceiver.get();
        try {
            ourReceiver.set(receiver);
            replyCallback.callback(reply);
        }
        finally {
            ourReceiver.set(before);
        }
    }

    /** The exception raised from processing the message. */
    private final Throwable myError;

    /** The reply to the message. */
    private final Reply myReply;

    /** The callback for the reply to the message. */
    private final ReplyCallback myReplyCallback;

    /**
     * Creates a new ReplyHandler.
     * 
     * @param replyCallback
     *            The callback for the message.
     * @param reply
     *            The reply.
     */
    public ReplyHandler(final ReplyCallback replyCallback, final Reply reply) {
        super();
        myReplyCallback = replyCallback;
        myReply = reply;
        myError = null;
    }

    /**
     * Creates a new ReplyHandler.
     * 
     * @param replyCallback
     *            The callback for the message.
     * @param exception
     *            The thrown exception.
     */
    public ReplyHandler(final ReplyCallback replyCallback,
            final Throwable exception) {
        super();
        myReplyCallback = replyCallback;
        myError = exception;
        myReply = null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to process the callback response.
     * </p>
     */
    @Override
    public void run() {
        if (myReply != null) {
            reply(null, myReply, myReplyCallback, null);
        }
        else if (myError != null) {
            raiseError(myError, myReplyCallback, null);
        }
    }
}
