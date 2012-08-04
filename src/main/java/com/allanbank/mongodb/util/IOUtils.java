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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class IOUtils {

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
            appendNibble(builder, (b >> 8) & 0xF);
            appendNibble(builder, b & 0xF);
        }
        return builder.toString();
    }

    /**
     * Converts a single nibble value into hex and appends it to the string
     * builder.
     * 
     * @param builder
     *            The builder to append to.
     * @param nibble
     *            The nibble to convert.
     */
    private static void appendNibble(final StringBuilder builder,
            final int nibble) {
        switch (nibble) {
        case 0x0:
            builder.append('0');
            break;
        case 0x1:
            builder.append('1');
            break;
        case 0x2:
            builder.append('2');
            break;
        case 0x3:
            builder.append('3');
            break;
        case 0x4:
            builder.append('4');
            break;
        case 0x5:
            builder.append('5');
            break;
        case 0x6:
            builder.append('6');
            break;
        case 0x7:
            builder.append('7');
            break;
        case 0x8:
            builder.append('8');
            break;
        case 0x9:
            builder.append('9');
            break;
        case 0xA:
            builder.append('a');
            break;
        case 0xB:
            builder.append('b');
            break;
        case 0xC:
            builder.append('c');
            break;
        case 0xD:
            builder.append('d');
            break;
        case 0xE:
            builder.append('e');
            break;
        case 0xF:
            builder.append('f');
            break;
        }
    }

    /**
     * Stop creation of a new IOUtils.
     */
    private IOUtils() {
        // Nothing.
    }
}
