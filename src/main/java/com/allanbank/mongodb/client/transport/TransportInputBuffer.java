/*
 * #%L
 * TransportInputBuffer.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

/**
 * TransportInputBuffer provides the ability to read a message to the
 * transport's native buffer.
 *
 * @api.internal This interface is part of the driver's internal API. Users of
 *               this API should advertise the explicit version of the driver
 *               they are compatible with. Public and protected members may be
 *               modified between non-bugfix releases (version numbers are
 *               &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;).
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface TransportInputBuffer {
    /**
     * Reads the message from the buffer.
     *
     * @return The message in the buffer.
     * @throws IOException
     *             On a failure writing the message.
     */
    public Message read() throws IOException;
}
