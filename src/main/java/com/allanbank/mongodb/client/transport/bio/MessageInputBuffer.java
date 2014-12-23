/*
 * #%L
 * MessageInputBuffer.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport.bio;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.transport.TransportInputBuffer;

/**
 * MessageInputBuffer provides a very simple transport buffer that directly
 * holds the message that has been read.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MessageInputBuffer implements TransportInputBuffer {

    /** The message that has been read. */
    private final Message myMessage;

    /**
     * Creates a new TwoThreadInputBuffer.
     * 
     * @param message
     *            The message that has been read.
     */
    public MessageInputBuffer(Message message) {
        myMessage = message;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the message that was read.
     * </p>
     */
    @Override
    public Message read() {
        return myMessage;
    }
}
