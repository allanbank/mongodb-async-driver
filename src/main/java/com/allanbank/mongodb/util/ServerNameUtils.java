/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

/**
 * ServerNameUtils provides the ability to generate a normalized name for a
 * server.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerNameUtils {
    /** The default MongoDB port. */
    public static final int DEFAULT_PORT = 27017;

    /** The length of an IPv6 address in bytes. */
    public static final int IPV6_LENGTH = 16;

    /**
     * Pattern to match a literal IPv6 address with an elipse; e.g.,
     * fe80::250:56ff:fec0:1
     */
    private static final Pattern IPV6_ELLIPSED_PATTERN;

    /** Pattern to match a fully specified IPv6. */
    private static final Pattern IPV6_PATTERN;

    static {
        final String hexWord = "[0-9A-Fa-f]{1,4}";
        IPV6_ELLIPSED_PATTERN = Pattern.compile("^((?:" + hexWord + "(?::"
                + hexWord + ")*)?)::((?:" + hexWord + "(?::" + hexWord
                + ")*)?)$");
        IPV6_PATTERN = Pattern.compile("^(?:" + hexWord + ":){7}" + hexWord
                + "$");
    }

    /**
     * Creates a normalized form of the {@link InetSocketAddress} in the form
     * '[server]:[port]'.
     *
     * @param address
     *            The address to generate a normalized form for.
     * @return The normalized address.
     */
    public static String normalize(final InetSocketAddress address) {
        final StringBuilder b = new StringBuilder();

        String name;
        if (address.isUnresolved()) {
            name = address.getHostName();
        }
        else {
            name = address.getAddress().getHostName();
        }

        // Check for a raw IPv6 address and wrap in RFC 2732 format.
        if (IPV6_ELLIPSED_PATTERN.matcher(name).matches()
                || IPV6_PATTERN.matcher(name).matches()) {
            b.append('[');
            b.append(name);
            b.append(']');
        }
        else {
            b.append(name);
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

        final int firstBracket = server.indexOf('[');
        final int lastBracket = server.lastIndexOf(']');
        final int colonIndex = server.lastIndexOf(':');

        // Check for RFC 2732.
        if ((firstBracket == 0) && (lastBracket > 0)) {
            if (colonIndex == (lastBracket + 1)) {
                final String portString = server.substring(colonIndex + 1);
                try {
                    Integer.parseInt(portString);

                    // Its a good name no need to create another string.
                    return server;
                }
                catch (final NumberFormatException nfe) {
                    // Not a port after the colon. Move on.
                    port = DEFAULT_PORT;
                }
            }

            // No port. Add it.
            final String namePart = server.substring(0, lastBracket + 1);
            return namePart + ":" + port;
        }

        if (0 <= colonIndex) {
            final int previousColon = server.lastIndexOf(':', colonIndex - 1);
            if (0 <= previousColon) {
                // Colon in the host name. Might be an IPv6 address. Try to
                // parse the whole thing as an address and if it works then
                // assume no port.
                try {
                    final InetAddress addr = InetAddress.getByName(server);
                    final byte[] bytes = addr.getAddress();

                    // Is it an IPv6 address?
                    if (bytes.length == IPV6_LENGTH) {
                        // Yep - add the default port and wrap in RFC 2732
                        return "[" + server + "]:" + DEFAULT_PORT;
                    }
                }
                catch (final UnknownHostException uhe) {
                    // OK - fall through to being a port.
                    final String addrString = server.substring(0, colonIndex);
                    final String portString = server.substring(colonIndex + 1);
                    try {
                        final InetAddress addr = InetAddress
                                .getByName(addrString);
                        final byte[] bytes = addr.getAddress();

                        // Is it an IPv6 address?
                        if (bytes.length == IPV6_LENGTH) {
                            port = Integer.parseInt(portString);

                            return "[" + addrString + "]:" + port;
                        }
                    }
                    catch (final NumberFormatException nfe) {
                        // Not a port after the colon. Move on.
                        port = DEFAULT_PORT;
                    }
                    catch (final UnknownHostException uhe2) {
                        // OK - fall through to being a port.
                        uhe2.hashCode(); // Shhh - PMD.
                    }
                }
            }

            final String portString = server.substring(colonIndex + 1);
            try {
                Integer.parseInt(portString);

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

        final int firstBracket = server.indexOf('[');
        final int lastBracket = server.lastIndexOf(']');
        final int colonIndex = server.lastIndexOf(':');
        // Check for RFC 2732.
        if ((firstBracket == 0) && (lastBracket > 0)) {
            if ((lastBracket + 1) == colonIndex) {
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
        }
        else if (colonIndex > 0) {
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
