/*
 * #%L
 * MongoMessageListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.metrics;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.message.Reply;

/**
 * MongoMessageListener provides the interface for notification that a message
 * has been sent/received and for users to be notified that messages have been
 * sent and received.
 * <p>
 * <em>Note</em>: This interface receives all of the messages at the core of the
 * driver and is extremely performance sensitive.
 * </p>
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface MongoMessageListener {

    /**
     * Notification that a message has been received from the MongoDB cluster.
     *
     * @param serverName
     *            The name of the server that the message was received from.
     * @param messageId
     *            The id of the message sent.
     * @param sent
     *            The message that was sent.
     * @param reply
     *            The message that was received.
     * @param latencyNanos
     *            The amount of time that expired from when the message was sent
     *            that requested the reply to when this reply was received.
     */
    public void receive(String serverName, long messageId, Message sent,
            Reply reply, long latencyNanos);

    /**
     * Notification that a message has been sent to the MongoDB cluster.
     *
     * @param serverName
     *            The name of the server that the message was sent to.
     * @param messageId
     *            The id of the message sent.
     * @param sent
     *            The message that was sent.
     */
    public void sent(String serverName, long messageId, Message sent);
}
