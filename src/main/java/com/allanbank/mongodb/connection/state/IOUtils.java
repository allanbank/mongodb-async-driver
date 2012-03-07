/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.io.Closeable;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * IOUtils provides helper methods for dealing with I/O operations.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class IOUtils {

    /** The logger for the {@link ConnectionPinger}. */
    protected static final Logger LOG = Logger.getLogger(IOUtils.class
            .getCanonicalName());

    /**
     * Closes the {@link Closeable} and logs any error.
     * 
     * @param closeable
     *            The connection to close.
     */
    public static void close(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (final IOException ignored) {
                LOG.finest("I/O Exception closing: "
                        + closeable.getClass().getSimpleName());
            }
        }
    }
}
