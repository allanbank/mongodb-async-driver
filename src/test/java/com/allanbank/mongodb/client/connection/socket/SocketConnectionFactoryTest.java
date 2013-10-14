/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.socket;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.ConnectionModel;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.connection.Connection;

/**
 * SocketConnectionFactoryTest provides tests for the
 * {@link SocketConnectionFactory} class.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SocketConnectionFactoryTest {
    /** A Mock MongoDB server to connect to. */
    private static MockSocketServer ourServer;

    /**
     * Starts a Mock MongoDB server.
     * 
     * @throws IOException
     *             On a failure to start the Mock MongoDB server.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        ourServer = new MockSocketServer();
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

    /** The factory under test. */
    private SocketConnectionFactory myTestFactory;

    /**
     * Cleans up the test server.
     */
    @After
    public void tearDown() {
        ourServer.clear();
        myTestFactory = null;
    }

    /**
     * Test method for {@link SocketConnectionFactory#close()} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testClose() throws IOException {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();
        final MongoClientConfiguration config = new MongoClientConfiguration(
                addr);
        myTestFactory = new SocketConnectionFactory(config);

        myTestFactory.close();

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
        final MongoClientConfiguration config = new MongoClientConfiguration(
                addr);
        myTestFactory = new SocketConnectionFactory(config);

        Connection conn = null;
        try {
            conn = myTestFactory.connect();
            assertThat(conn, instanceOf(SocketConnection.class));

            assertTrue("Should have connected to the server.",
                    ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

            conn.close();

            assertTrue("Should have disconnected from the server.",
                    ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));
            conn = null;
        }
        finally {
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
        final MongoClientConfiguration config = new MongoClientConfiguration(
                bad);

        myTestFactory = new SocketConnectionFactory(config);

        Connection conn = null;
        try {
            conn = myTestFactory.connect();
        }
        finally {
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
    @Test(expected = IOException.class)
    public void testConnectFailureNoAddresses() throws IOException {

        // Force to the wrong port.
        final MongoClientConfiguration config = new MongoClientConfiguration();

        myTestFactory = new SocketConnectionFactory(config);

        Connection conn = null;
        try {
            conn = myTestFactory.connect();
        }
        finally {
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
    @Test
    public void testConnectWithModeSendReceive() throws IOException {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();

        final MongoClientConfiguration config = new MongoClientConfiguration(
                addr);
        config.setConnectionModel(ConnectionModel.SENDER_RECEIVER_THREAD);

        myTestFactory = new SocketConnectionFactory(config);

        Connection conn = null;
        try {
            conn = myTestFactory.connect();

            assertThat(conn, instanceOf(TwoThreadSocketConnection.class));

            assertTrue("Should have connected to the server.",
                    ourServer.waitForClient(TimeUnit.SECONDS.toMillis(10)));

            conn.close();

            assertTrue("Should have disconnected from the server.",
                    ourServer.waitForDisconnect(TimeUnit.SECONDS.toMillis(10)));
            conn = null;
        }
        finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * Test method for {@link SocketConnectionFactory#getClusterType()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testGetClusterType() throws IOException {
        final InetSocketAddress addr = ourServer.getInetSocketAddress();
        final MongoClientConfiguration config = new MongoClientConfiguration(
                addr);
        myTestFactory = new SocketConnectionFactory(config);

        assertEquals(ClusterType.STAND_ALONE, myTestFactory.getClusterType());
    }
}