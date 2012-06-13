/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * ServerLatencyCallback provides a special callback to measure the latency of
 * requests to a server.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerLatencyCallback implements Callback<Reply> {

    /** The server to update the latency for. */
    private final ServerState myServer;

    /** The {@link System#nanoTime()} when the callback was created. */
    private final long myStartTimeNanos;

    /**
     * Creates a new ServerLatencyCallback.
     * 
     * @param server
     *            The server we are tracking the latency for.
     */
    public ServerLatencyCallback(final ServerState server) {
        super();
        myServer = server;
        myStartTimeNanos = System.nanoTime();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to update the server's latency based on the round trip reply
     * time.
     * </p>
     */
    @Override
    public void callback(final Reply result) {
        final long end = System.nanoTime();
        final long milliDelta = TimeUnit.NANOSECONDS.toMillis(end
                - myStartTimeNanos);

        myServer.updateAverageLatency(milliDelta);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to ignore the error - we cannot be sure if it was a local
     * error or an error from the server.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        // Nothing. Cannot determine the latency from an exception.
    }
}
