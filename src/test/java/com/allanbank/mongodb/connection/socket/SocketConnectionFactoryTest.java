/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;

/**
 * SocketConnectionFactoryTest provides tests for the
 * {@link SocketConnectionFactory} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionFactoryTest {
	/** A Mock MongoDB server to connect to. */
	private static MockMongoDBServer ourServer;

	/**
	 * Starts a Mock MongoDB server.
	 * 
	 * @throws IOException
	 *             On a failure to start the Mock MongoDB server.
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws IOException {
		ourServer = new MockMongoDBServer();
		ourServer.start();
	}

	/**
	 * Stops a Mock MongoDB server.
	 * 
	 * @throws IOException
	 *             On a failure to stop the Mock MongoDB server.
	 */
	@AfterClass
	public static void tearDownAfterClass() throws IOException {
		ourServer.setRunning(false);
		ourServer.close();
		ourServer = null;
	}

	/**
	 * Cleans up the test server.
	 */
	@After
	public void tearDown() {
		ourServer.clear();
	}

	/**
	 * Test method for {@link SocketConnectionFactory#connect()} .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test
	public void testConnect() throws IOException {
		final InetSocketAddress addr = ourServer.getInetSocketAddress();
		final MongoDbConfiguration config = new MongoDbConfiguration(addr);
		final SocketConnectionFactory factory = new SocketConnectionFactory(
				config);

		Connection conn = null;
		try {
			conn = factory.connect();

			assertTrue("Should have connected to the server.",
					ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

			conn.close();

			assertTrue("Should have disconnected from the server.",
					ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));
			conn = null;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	/**
	 * Test method for {@link SocketConnectionFactory#connect()} .
	 * 
	 * @throws IOException
	 *             On a failure connecting to the Mock MongoDB server.
	 */
	@Test(expected = SocketException.class)
	public void testConnectFailure() throws IOException {
		final InetSocketAddress addr = ourServer.getInetSocketAddress();

		// Force to the wrong port.
		final InetSocketAddress bad = new InetSocketAddress(addr.getAddress(),
				addr.getPort() + 1);
		final MongoDbConfiguration config = new MongoDbConfiguration(bad);

		final SocketConnectionFactory factory = new SocketConnectionFactory(
				config);

		Connection conn = null;
		try {
			conn = factory.connect();
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

}
