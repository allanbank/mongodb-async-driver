/*
 * #%L
 * SslEngineFactory.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2015 Allanbank Consulting, Inc.
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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * SslEngineFactory provides an interface for a {@link SSLSocketFactory} to
 * create a {@link SSLEngine} instead of a {@link SSLSocket}. This is provided
 * for transports that do not directly use {@code SSLSockets} and instead
 * require direct access to the {@code SSLEngine}.
 *
 * @api.internal This interface is part of the driver's internal API. Users of
 *               this API should advertise the explicit version of the driver
 *               they are compatible with. Public and protected members may be
 *               modified between non-bugfix releases (version numbers are
 *               &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;).
 * @copyright 2015, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface SslEngineFactory {

    /**
     * Creates a new {@link SSLEngine}.
     *
     * @return The {@link SSLEngine}.
     */
    public SSLEngine createSSLEngine();
}
