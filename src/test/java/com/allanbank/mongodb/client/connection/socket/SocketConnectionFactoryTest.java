/*
 * #%L
 * SocketConnectionFactoryTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
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
        updateVersion();

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
        updateVersion();

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

    /**
     * Updates the version of the server so ewe don't need to wait for it.
     */
    private void updateVersion() {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.pushArray("versionArray").add(1).add(2L).add(3.0);

        myTestFactory.getCluster().add(ourServer.getInetSocketAddress())
                .update(builder.build());
    }
}
