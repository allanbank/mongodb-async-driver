/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import java.net.InetSocketAddress;

/**
 * ServerNameUtils provides the ability to generate a normalized name for a
 * server.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerNameUtils {
    /** The default MongoDB port. */
    public static final int DEFAULT_PORT = 27017;

    /**
     * Creates a normalized form of the {@link InetSocketAddress} in the form
     * '[server]:[port]'.
     * 
     * @param address
     *            The address to generate a normaized form for.
     * @return The normalized address.
     */
    public static String normalize(final InetSocketAddress address) {
        final StringBuilder b = new StringBuilder();

        if (address.isUnresolved()) {
            b.append(address.getHostString());
        }
        else {
            b.append(address.getAddress().getHostName());
        }
        b.append(':');
        b.append(address.getPort());

        return b.toString();
    }

    /**
     * Normalizes the name into a '[server]:[port]' string. If a port component
     * is not provided then port 27017 is assumed.
     * 
     * @param server
     *            The server[:port] string.
     * @return The normailzed server string.
     */
    public static String normalize(final String server) {
        final String name = server;
        int port = DEFAULT_PORT;

        final int colonIndex = server.lastIndexOf(':');
        if (colonIndex > 0) {
            final String portString = server.substring(colonIndex + 1);
            try {
                port = Integer.parseInt(portString);

                // Its a good name no need to create another string.
                return server;
            }
            catch (final NumberFormatException nfe) {
                // Not a port after the colon. Move on.
                port = DEFAULT_PORT;

            }
        }

        return name + ':' + port;
    }

    /**
     * Parse the name into a {@link InetSocketAddress}. If a port component is
     * not provided then port 27017 is assumed.
     * 
     * @param server
     *            The server[:port] string.
     * @return The {@link InetSocketAddress} parsed from the server string.
     */
    public static InetSocketAddress parse(final String server) {
        String name = server;
        int port = DEFAULT_PORT;

        final int colonIndex = server.lastIndexOf(':');
        if (colonIndex > 0) {
            final String portString = server.substring(colonIndex + 1);
            try {
                port = Integer.parseInt(portString);
                name = server.substring(0, colonIndex);
            }
            catch (final NumberFormatException nfe) {
                // Not a port after the colon. Move on.
                port = DEFAULT_PORT;

            }
        }

        return new InetSocketAddress(name, port);
    }

    /**
     * Creates a new ServerNameUtils.
     */
    private ServerNameUtils() {
        // nothing
    }

}
