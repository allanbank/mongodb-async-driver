/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import javax.net.SocketFactory;

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
}
