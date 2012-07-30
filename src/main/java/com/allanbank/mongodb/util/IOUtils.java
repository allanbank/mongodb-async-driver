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
     * Stop creation of a new IOUtils.
     */
    private IOUtils() {
        // Nothing.
    }
}
