/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.state;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerState {

	/** The decay rate for the exponential average for the latency. */
	public static final double DECAY_ALPHA;

	/** The decay period (number of samples) for the average latency. */
	public static final double DECAY_SAMPLES = 1000.0D;

	/** The default MongoDB port. */
	public static final int DEFAULT_PORT = 27017;

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

	/** The server being tracked. */
	private final InetSocketAddress myServer;

	/** Tracking if write operations can occur to the server. */
	private final AtomicBoolean myWritable;

	/**
	 * Creates a new {@link ServerState}. Package private to force creation
	 * through the {@link ClusterState}.
	 * 
	 * @param server
	 *            The server being tracked.
	 */
	/* package */ServerState(final String server) {
		myServer = parse(server);
		myWritable = new AtomicBoolean(false);
		myAverageLatency = new AtomicLong(0);
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
	 * Returns the address of the server being tracked.
	 * 
	 * @return The address of the server being tracked.
	 */
	public InetSocketAddress getServer() {
		return myServer;
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
		} else {
			final double newAverage = (DECAY_ALPHA * latency)
					+ ((1.0D - DECAY_ALPHA) * oldAverage);
			myAverageLatency.set(Double.doubleToLongBits(newAverage));
		}
	}

	/**
	 * Parse the name into a {@link InetSocketAddress}. If a port component is
	 * not provided then port 27017 is assumed.
	 * 
	 * @param server
	 *            The server[:port] string.
	 * @return The {@link InetSocketAddress} parsed from the server string.
	 */
	protected InetSocketAddress parse(final String server) {
		String name = server;
		int port = DEFAULT_PORT;

		final int colonIndex = server.indexOf(':');
		if (colonIndex > 0) {
			final String portString = server.substring(colonIndex + 1);
			try {
				port = Integer.parseInt(portString);
				name = server.substring(0, colonIndex);
			} catch (final NumberFormatException nfe) {
				// Not a port after the colon. Move on.
			}
		}

		return new InetSocketAddress(name, port);
	}

	/**
	 * Sets if the server can be written to. This is package private to force
	 * updates to occur through the ClusterState.
	 * 
	 * @param writable
	 *            If true the server can be written to, false otherwise.
	 */
	/* package */void setWritable(final boolean writable) {
		myWritable.set(writable);
	}
}
