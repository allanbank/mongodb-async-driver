/*
 * #%L
 * TransportResponseListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.MongoDbException;

/**
 * TransportResponseListener provides the low level interface for transports to
 * notify the driver that a message has been received.
 * 
 * @api.internal This interface is part of the driver's internal API. Users of
 *               this API should advertise the explicit version of the driver
 *               they are compatible with. Public and protected members may be
 *               modified between non-bugfix releases (version numbers are
 *               &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;).
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface TransportResponseListener {

    /**
     * Notification that a response has been received.
     * 
     * @param buffer
     *            The buffer with the received response.
     */
    public void response(TransportInputBuffer buffer);

    /**
     * Notification that the transport is closed.
     * 
     * @param error
     *            A {@link MongoDbException} with information on why the
     *            connection was closed.
     */
    public void closed(MongoDbException error);
}
