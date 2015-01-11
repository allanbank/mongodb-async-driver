/*
 * #%L
 * PendingMessage.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.message;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;

/**
 * Container for a pending message. Before the message is sent the message id
 * will be zero. After it will contain the assigned message id for the
 * connection.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PendingMessage {

    /** The message sent. */
    private Message myMessage;

    /** The message id assigned to the sent message. */
    private int myMessageId;

    /** The callback for the reply to the message. */
    private ReplyCallback myReplyCallback;

    /** The timestamp of the message. */
    private long myTimestamp;

    /**
     * Create a new PendingMessage.
     */
    public PendingMessage() {
        this(0, null, null);
    }

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
            final ReplyCallback replyCallback) {
        myMessageId = messageId;
        myMessage = message;
        myReplyCallback = replyCallback;
    }

    /**
     * Clears the state of the message allowing the referenced objects to be
     * garbage collected.
     */
    public void clear() {
        myTimestamp = 0;
        myMessageId = 0;
        myMessage = null;
        myReplyCallback = null;
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
    public ReplyCallback getReplyCallback() {
        return myReplyCallback;
    }

    /**
     * Determines the latency of the message in nano-seconds. If the message
     * does not have a time stamp then zero is returned.
     *
     * @return The current latency for the message.
     */
    public long latency() {
        final long timestamp = myTimestamp;

        if (timestamp == 0) {
            return 0;
        }

        return System.nanoTime() - timestamp;
    }

    /**
     * Sets the state of the pending message.
     *
     * @param messageId
     *            The id of the sent message.
     * @param message
     *            The sent message.
     * @param replyCallback
     *            The callback for the message. May be null.
     */
    public void set(final int messageId, final Message message,
            final ReplyCallback replyCallback) {
        myMessageId = messageId;
        myMessage = message;
        myReplyCallback = replyCallback;
        myTimestamp = System.nanoTime();
    }

    /**
     * Sets the state of the pending message.
     *
     * @param other
     *            The pending message to copy from.
     */
    public void set(final PendingMessage other) {
        myMessageId = other.getMessageId();
        myMessage = other.getMessage();
        myReplyCallback = other.getReplyCallback();
        myTimestamp = other.myTimestamp;
    }

    /**
     * Sets the time stamp of the message to now.
     */
    public void timestampNow() {
        myTimestamp = System.nanoTime();
    }
}
