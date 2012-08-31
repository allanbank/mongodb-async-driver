/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * IOUtils provides helper methods for dealing with I/O operations.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class IOUtils {

    /** Hex encoding characters. */
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /** The logger for the {@link IOUtils}. */
    private static final Logger LOG = Logger.getLogger(IOUtils.class
            .getCanonicalName());

    /**
     * Closes the {@link Closeable} and logs any error.
     * 
     * @param closeable
     *            The connection to close.
     */
    public static void close(final Closeable closeable) {
        if (closeable != null) {
            close(closeable, Level.FINE, "I/O Exception closing: "
                    + closeable.getClass().getSimpleName());
        }
    }

    /**
     * Closes the {@link Closeable} and logs any error.
     * 
     * @param closeable
     *            The connection to close.
     * @param level
     *            The level to log on a failure.
     * @param message
     *            The message to log on a failure.
     */
    public static void close(final Closeable closeable, final Level level,
            final String message) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (final IOException ignored) {
                LOG.log(level, message);
            }
        }
    }

    /**
     * Converts the byte array into a HEX string.
     * 
     * @param bytes
     *            The bytes to convert.
     * @return The string version.
     */
    public static String toHex(final byte[] bytes) {
        final StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (final byte b : bytes) {
            builder.append(HEX_CHARS[(b >> 4) & 0xF]);
            builder.append(HEX_CHARS[b & 0xF]);
        }
        return builder.toString();
    }

    /**
     * Stop creation of a new IOUtils.
     */
    private IOUtils() {
        // Nothing.
    }
}
