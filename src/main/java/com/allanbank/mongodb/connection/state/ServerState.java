/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.state;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * Tracks the state of a single server. Currently two factors are tracked:
 * <ul>
 * <li>Is the server available to receive writes (inserts/updates/deletes)?</li>
 * <li>What is the exponential moving average for the server's reply latency.</li>
 * </ul>
 * <p>
 * {@link ServerState}s should generally be created and their writable state
 * updated via a {@link ClusterState}.
 * </p>
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerState {

    /** The decay rate for the exponential average for the latency. */
    public static final double DECAY_ALPHA;

    /** The decay period (number of samples) for the average latency. */
    public static final double DECAY_SAMPLES = 1000.0D;

    /** The default MongoDB port. */
    public static final int DEFAULT_PORT = ServerNameUtils.DEFAULT_PORT;

    static {
        DECAY_ALPHA = (2.0D / (DECAY_SAMPLES + 1));
    }

    /**
     * Tracks the average latency for the server connection. This is set when
     * the connection to the server is first created and then updated
     * periodically using an exponential moving average. We store the average (a
     * double) as long bits.
     */
    private final AtomicLong myAverageLatency;

    /** The normalized name of the server being tracked. */
    private final String myName;

    /**
     * A connection that may be owned by a logical connection but can be used
     * for direct communication with the server. Primarily used by the
     * {@link ConnectionPinger}.
     */
    private final AtomicReference<Connection> myPrimaryConnection;

    /**
     * If true then the primary connection has been claimed by a logical
     * connection and the state will not try and close it.
     */
    private final AtomicBoolean myPrimaryConnectionOwned;

    /** The server being tracked. */
    private final InetSocketAddress myServer;

    /** Tracking the tags for the server. */
    private final AtomicReference<Document> myTags;

    /** Tracking if write operations can occur to the server. */
    private final AtomicBoolean myWritable;

    /**
     * Creates a new {@link ServerState}. Package private to force creation
     * through the {@link ClusterState}.
     * 
     * @param server
     *            The server being tracked.
     */
    public ServerState(final String server) {
        myServer = ServerNameUtils.parse(server);
        myName = ServerNameUtils.normalize(server);
        myWritable = new AtomicBoolean(false);
        myAverageLatency = new AtomicLong(
                Double.doubleToLongBits(Double.MAX_VALUE));
        myTags = new AtomicReference<Document>(null);
        myPrimaryConnection = new AtomicReference<Connection>(null);
        myPrimaryConnectionOwned = new AtomicBoolean(false);
    }

    /**
     * Returns the current average latency (in milliseconds) seen in issuing
     * requests to the server. If the latency returns {@link Double#MAX_VALUE}
     * then we have no basis for determining the latency.
     * 
     * @return The current average latency (in milliseconds) seen in issuing
     *         requests to the server.
     */
    public double getAverageLatency() {
        return Double.longBitsToDouble(myAverageLatency.get());
    }

    /**
     * Returns the normalized name of the server being tracked.
     * 
     * @return The normalized name of the server being tracked.
     */
    public String getName() {
        return myName;
    }

    /**
     * Returns the address of the server being tracked.
     * 
     * @return The address of the server being tracked.
     */
    public InetSocketAddress getServer() {
        return myServer;
    }

    /**
     * Returns the tags for the server.
     * 
     * @return The tags for the server.
     */
    public Document getTags() {
        return myTags.get();
    }

    /**
     * Returns true if the server can be written to, false otherwise.
     * <p>
     * If writable it might be a standalone server, the primary in a replica
     * set, or a mongos in a sharded configuration. If not writable it is a
     * secondary server in a replica set or we cannot or have not connected to
     * the server yet.
     * </p>
     * <p>
     * To modify the writable flag update the {@link ClusterState} via a
     * {@link ClusterState#markNotWritable(ServerState)} or
     * {@link ClusterState#markWritable(ServerState)} call.
     * </p>
     * 
     * @return True if the server can be written to, false otherwise.
     */
    public boolean isWritable() {
        return myWritable.get();
    }

    /**
     * Sets the average latency (in milliseconds) for requests to the server.
     * The initial latency should be set for the "discovery commmands" sent to
     * the server.
     * 
     * @param latency
     *            The latency (in milliseconds) for requests to the server.
     */
    public void setAverageLatency(final double latency) {
        myAverageLatency.set(Double.doubleToLongBits(latency));
    }

    /**
     * Sets the tags for the server.
     * 
     * @param tags
     *            The tags for the server.
     */
    public void setTags(final Document tags) {
        myTags.set(tags);
    }

    /**
     * Updates the average latency (in milliseconds) for the server.
     * 
     * @param latency
     *            The latency seen sending a request and receiving a reply from
     *            the server.
     */
    public void updateAverageLatency(final long latency) {
        final double oldAverage = Double.longBitsToDouble(myAverageLatency
                .get());
        if (Double.MAX_VALUE == oldAverage) {
            myAverageLatency.set(Double.doubleToLongBits(latency));
        }
        else {
            final double newAverage = (DECAY_ALPHA * latency)
                    + ((1.0D - DECAY_ALPHA) * oldAverage);
            myAverageLatency.set(Double.doubleToLongBits(newAverage));
        }
    }

    /**
     * Sets if the server can be written to. This is package private to force
     * updates to occur through the ClusterState.
     * 
     * @param writable
     *            If true the server can be written to, false otherwise.
     * @return The previous writable state.
     */
    /* package */boolean setWritable(final boolean writable) {
        return myWritable.getAndSet(writable);
    }
}
