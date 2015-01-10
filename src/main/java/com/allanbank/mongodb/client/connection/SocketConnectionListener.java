/*
 * #%L
 * SocketConnectionListener.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client.connection;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import javax.net.SocketFactory;
import javax.net.ssl.SSLEngine;

/**
 * SocketConnectionListener provides a callback interface for
 * {@link SocketFactory} instances to implement that wish to be notified of
 * connection completion. This will mainly be for security issues.
 * 
 * @api.no This interface is <b>NOT</b> part of the drivers API. This class may
 *         be mutated in incompatible ways between any two releases of the
 *         driver.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface SocketConnectionListener {

	/**
	 * Notification that the socket is now connected to the specified
	 * InetSocketAddress.
	 * 
	 * @param connectedTo
	 *            The address the socket has been connected to.
	 * @param connection
	 *            The socket connection.
	 * @throws SocketException
	 *             On a failure configuring the socket.
	 */
	public void connected(InetSocketAddress connectedTo, Socket connection)
			throws SocketException;

	/**
	 * Notification that the socket is now connected to the specified
	 * InetSocketAddress.
	 * 
	 * @param connectedTo
	 *            The address the socket has been connected to.
	 * @param connection
	 *            The SSL Engine for the connection.
	 * @throws SocketException
	 *             On a failure configuring the socket.
	 */
	public void connected(InetSocketAddress connectedTo, SSLEngine connection)
			throws SocketException;
}
