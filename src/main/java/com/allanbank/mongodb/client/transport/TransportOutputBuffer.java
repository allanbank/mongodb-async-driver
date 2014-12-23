/*
 * #%L
 * TransportOutputBuffer.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.io.IOException;

import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;

/**
 * TransportOutputBuffer provides the ability to write a message to the
 * transport's native buffer.
 * 
 * @api.internal This interface is part of the driver's internal API. Users of
 *               this API should advertise the explicit version of the driver
 *               they are compatible with. Public and protected members may be
 *               modified between non-bugfix releases (version numbers are
 *               &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;).
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface TransportOutputBuffer {
    /**
     * Writes the provided message to the buffer.
     * 
     * @param messageId
     *            The message id for the message.
     * @param message
     *            The message to write.
     * @param callback
     *            The callback for the message. This may be null.
     * @throws IOException
     *             On a failure writing the message.
     */
    public void write(int messageId, Message message, ReplyCallback callback)
            throws IOException;
}
