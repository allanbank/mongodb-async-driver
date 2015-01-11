/*
 * #%L
 * OneThreadOutputBuffer.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport.bio.one;

import java.io.IOException;
import java.io.OutputStream;

import com.allanbank.mongodb.bson.io.BufferingBsonOutputStream;
import com.allanbank.mongodb.bson.io.RandomAccessOutputStream;
import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.transport.TransportOutputBuffer;

/**
 * OneThreadOutputBuffer provides a output buffer that serializes all of the
 * messages to a {@link BufferingBsonOutputStream}.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */class OneThreadOutputBuffer
        implements TransportOutputBuffer {
    /** The buffer backing the stream. */
    private final RandomAccessOutputStream myBuffer;

    /** The stream to write to. */
    private final BufferingBsonOutputStream myOutputStream;

    /**
     * Creates a new BinaryOutputBuffer.
     *
     * @param stringCache
     *            The cache for strings.
     */
    public OneThreadOutputBuffer(final StringEncoderCache stringCache) {
        myBuffer = new RandomAccessOutputStream(stringCache);
        myOutputStream = new BufferingBsonOutputStream(myBuffer);
    }

    /**
     * Clears the message buffer.
     */
    public void clear() {
        myBuffer.reset();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to write the message to the internal buffer.
     * </p>
     *
     * @see TransportOutputBuffer#write(int, Message, ReplyCallback)
     */
    @Override
    public void write(final int messageId, final Message message,
            final ReplyCallback callback) throws IOException {
        message.write(messageId, myOutputStream);
    }

    /**
     * Writes the contents of the buffer to the provided stream.
     *
     * @param out
     *            The stream to write to.
     * @throws IOException
     *             On a failure writing to the stream.
     */
    public void writeTo(final OutputStream out) throws IOException {
        myBuffer.writeTo(out);
    }
}
