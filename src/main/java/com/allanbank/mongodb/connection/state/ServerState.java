/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
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
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
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

    /**
     * A connection that may be owned by a logical connection but can be used
     * for direct communication with the server. Primarily used by the
     * {@link ClusterPinger}.
     */
    private final AtomicReference<Connection> myConnection;

    /** The normalized name of the server being tracked. */
    private final String myName;

    /**
     * Tracks the last report of how many seconds the server is behind the
     * primary. We store the value (a double) as long bits.
     */
    private final AtomicLong mySecondsBehind;

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
    public ServerState(final InetSocketAddress server) {
        myServer = server;
        myName = ServerNameUtils.normalize(server);
        myWritable = new AtomicBoolean(false);
        myAverageLatency = new AtomicLong(
                Double.doubleToLongBits(Double.MAX_VALUE));
        mySecondsBehind = new AtomicLong(
                Double.doubleToLongBits(Double.MAX_VALUE));
        myTags = new AtomicReference<Document>(null);
        myConnection = new AtomicReference<Connection>(null);
    }

    /**
     * Adds a {@link Connection} to the state making it available for reuse.
     * <p>
     * Only a limited number of connections will be held (currently 1). If the
     * connection is not added to the state false is returned.
     * </p>
     * 
     * @param connection
     *            A {@link Connection} to the server making it available to
     *            other to use.
     * @return True if the connection has been added to the state, false
     *         otherwise.
     */
    public boolean addConnection(final Connection connection) {
        return myConnection.compareAndSet(null, connection);
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
     * Sets the last reported seconds behind the primary.
     * 
     * @return The seconds behind the primary server.
     */
    public double getSecondsBehind() {
        return Double.longBitsToDouble(mySecondsBehind.get());
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
     * Removes the shared connection.
     * 
     * @param conn
     *            The shared connection to remove.
     */
    public void removeConnection(final Connection conn) {
        myConnection.compareAndSet(conn, null);
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
     * Sets the last reported seconds behind the primary.
     * 
     * @param secondsBehind
     *            The seconds behind the primary server.
     */
    public void setSecondsBehind(final double secondsBehind) {
        mySecondsBehind.set(Double.doubleToLongBits(secondsBehind));
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
     * Returns a {@link Connection} to the server if one is available.
     * 
     * @return A {@link Connection} to the server if one is available.
     */
    public Connection takeConnection() {
        return myConnection.getAndSet(null);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to to return a human readable version of the server state.
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append(myName);
        builder.append("(");
        if (myWritable.get()) {
            builder.append("W,");
        }
        if (myTags.get() != null) {
            builder.append("T,");
        }
        builder.append(getAverageLatency());
        builder.append(")");

        return builder.toString();
    }

    /**
     * Updates the average latency (in milliseconds) for the server.
     * 
     * @param latency
     *            The latency seen sending a request and receiving a reply from
     *            the server.
     */
    public void updateAverageLatency(final double latency) {
        final double oldAverage = Double.longBitsToDouble(myAverageLatency
                .get());
        if (Double.MAX_VALUE == oldAverage) {
            myAverageLatency.set(Double.doubleToLongBits(latency));
            mySecondsBehind.compareAndSet(
                    Double.doubleToLongBits(Double.MAX_VALUE),
                    Double.doubleToLongBits(0.0));
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
