/*
 * #%L
 * Transport.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.transport;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Transport provides the low level interface for sending and receiving messages
 * to a MongoDB server.
 * 
 * @param <OUT>
 *            The type for the send buffer used by the transport.
 * 
 * @api.internal This interface is part of the driver's internal API. Users of
 *               this API should advertise the explicit version of the driver
 *               they are compatible with. Public and protected members may be
 *               modified between non-bugfix releases (version numbers are
 *               &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;).
* @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Transport<OUT extends TransportOutputBuffer> extends
        Closeable, Flushable {

    /**
     * Request for a new buffer to write a message to. The buffer will later be
     * used to send a message.
     * 
     * @param size
     *            The expected size of the message to be written. The message
     *            will not be any larger than this size but may be smaller. The
     *            {@link Transport} is free to use this size in selecting a
     *            buffer or to ignore it completely.
     * @return The buffer that can be used to write the message.
     */
    public OUT createSendBuffer(int size);

    /**
     * Sends the buffer via the transport. The driver will no longer use the
     * buffer and the {@link Transport} is free to reuse the buffer.
     * 
     * @param buffer
     *            The buffer to send.
     * @throws IOException
     *             If the send operation fails to write to the stream.
     * @throws InterruptedIOException
     *             If the send operation is interrupted.
     */
    public void send(OUT buffer) throws IOException, InterruptedIOException;

    /**
     * Notification that the transport can start any internal threads.
     */
    public void start();
}
