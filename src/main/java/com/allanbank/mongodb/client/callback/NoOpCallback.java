/*
 * Copyright 2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import com.allanbank.mongodb.client.message.Reply;

/**
 * NoOpCallback provides a callback that does nothing.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class NoOpCallback implements ReplyCallback {
    /** A no-op callback. */
    public static final NoOpCallback NO_OP = new NoOpCallback();

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void callback(final Reply result) {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to do nothing.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        // Nothing.
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true.
     * </p>
     */
    @Override
    public boolean isLightWeight() {
        return true;
    }
}