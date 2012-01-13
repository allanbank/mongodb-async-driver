/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.messsage.Reply;

/**
 * Container for a pending message. Before the message is sent the message id
 * will be zero. After it will contain the assigned message id for the
 * connection.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PendingMessage {

    /** The message sent. */
    private final Message myMessage;

    /** The message id assigned to the sent message. */
    private int myMessageId;

    /** The callback for the reply to the message. */
    private final Callback<Reply> myReplyCallback;

    /**
     * Create a new PendingMessage.
     * 
     * @param messageId
     *            The id assigned to the message.
     * @param message
     *            The sent message.
     */
    public PendingMessage(final int messageId, final Message message) {
        this(messageId, message, null);
    }

    /**
     * Create a new PendingMessage.
     * 
     * @param messageId
     *            The id assigned to the message.
     * @param message
     *            The sent message.
     * @param replyCallback
     *            The callback for the reply to the message.
     * 
     */
    public PendingMessage(final int messageId, final Message message,
            final Callback<Reply> replyCallback) {
        myMessageId = messageId;
        myMessage = message;
        myReplyCallback = replyCallback;
    }

    /**
     * Returns the sent message.
     * 
     * @return The sent message.
     */
    public Message getMessage() {
        return myMessage;
    }

    /**
     * Returns the message id assigned to the sent message.
     * 
     * @return The message id assigned to the sent message.
     */
    public int getMessageId() {
        return myMessageId;
    }

    /**
     * Returns the callback for the reply to the message.
     * 
     * @return The callback for the reply to the message.
     */
    public Callback<Reply> getReplyCallback() {
        return myReplyCallback;
    }

    /**
     * Raises an error in the callback, if any.
     * 
     * @param exception
     *            The thrown exception.
     */
    public void raiseError(final Throwable exception) {
        if (myReplyCallback != null) {
            myReplyCallback.exception(exception);
        }
    }

    /**
     * Sets the reply for the callback, if any.
     * 
     * @param reply
     *            The reply.
     */
    public void reply(final Reply reply) {
        if (myReplyCallback != null) {
            myReplyCallback.callback(reply);
        }
    }

    /**
     * Sets the message id assigned to the sent message to the new value.
     * 
     * @param messageId
     *            The message id assigned to the sent message.
     */
    public void setMessageId(final int messageId) {
        myMessageId = messageId;
    }

}
