/*
 * #%L
 * TwoThreadOutputBuffer.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport.bio.two;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.message.PendingMessage;
import com.allanbank.mongodb.client.transport.TransportOutputBuffer;

/**
 * TwoThreadOutputBuffer provides the ability to carry messages to the send
 * thread.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class TwoThreadOutputBuffer implements TransportOutputBuffer {

    /** The messages to be sent via the buffer. */
    private final List<PendingMessage> myMessages;

    /**
     * Creates a new TwoThreadOutputBuffer.
     */
    public TwoThreadOutputBuffer() {
        myMessages = new ArrayList<PendingMessage>(2);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to copy the message into a pending message to be sent.
     * </p>
     */
    @Override
    public void write(int messageId, Message message, ReplyCallback callback) {
        myMessages.add(new PendingMessage(messageId, message, callback));
    }

    /**
     * Returns the messages to be sent via the buffer.
     * 
     * @return The messages to be sent via the buffer.
     */
    public List<PendingMessage> getMessages() {
        return Collections.unmodifiableList(myMessages);
    }

    /**
     * Clears the pending messages.
     */
    public void clear() {
        myMessages.clear();
    }
}
