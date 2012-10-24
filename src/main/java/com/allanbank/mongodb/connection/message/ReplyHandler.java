/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.allanbank.mongodb.Callback;

/**
 * ReplyHandler provides the capability to properly handle the replies to a
 * callback.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyHandler implements Runnable {

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
            final Callback<Reply> replyCallback, final Executor executor) {
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
     * @param reply
     *            The reply.
     * @param replyCallback
     *            The callback for the reply to the message.
     * @param executor
     *            The executor to use for the back-grounding the reply handling.
     */
    public static void reply(final Reply reply,
            final Callback<Reply> replyCallback, final Executor executor) {
        if (replyCallback != null) {
            if (executor != null) {
                try {
                    executor.execute(new ReplyHandler(replyCallback, reply));
                }
                catch (final RejectedExecutionException rej) {
                    // Run on this thread.
                    replyCallback.callback(reply);
                }
            }
            else {
                replyCallback.callback(reply);
            }
        }
    }

    /** The exception raised from processing the message. */
    private Throwable myError;

    /** The reply to the message. */
    private Reply myReply;

    /** The callback for the reply to the message. */
    private final Callback<Reply> myReplyCallback;

    /**
     * Creates a new ReplyHandler.
     * 
     * @param replyCallback
     *            The callback for the message.
     * @param reply
     *            The reply.
     */
    public ReplyHandler(final Callback<Reply> replyCallback, final Reply reply) {
        super();
        myReplyCallback = replyCallback;
        myReply = reply;
    }

    /**
     * Creates a new ReplyHandler.
     * 
     * @param replyCallback
     *            The callback for the message.
     * @param exception
     *            The thrown exception.
     */
    public ReplyHandler(final Callback<Reply> replyCallback,
            final Throwable exception) {
        super();
        myReplyCallback = replyCallback;
        myError = exception;
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
            reply(myReply, myReplyCallback, null);
        }
        else if (myError != null) {
            raiseError(myError, myReplyCallback, null);
        }
    }
}
