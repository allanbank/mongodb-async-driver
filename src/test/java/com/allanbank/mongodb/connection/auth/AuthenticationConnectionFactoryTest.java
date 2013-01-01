/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * AuthenticationConnectionFactoryTest provides test for the
 * {@link AuthenticationConnectionFactory}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticationConnectionFactoryTest {

    /** The factory being tested. */
    private AuthenticationConnectionFactory myTestFactory;

    /**
     * Cleans up the test connection.
     * 
     * @throws IOException
     *             On a failure to shutdown the test connection.
     */
    @After
    public void tearDown() throws IOException {
        myTestFactory = null;
    }

    /**
     * Test method for {@link AuthenticationConnectionFactory#close()} .
     * 
     * @throws IOException
     *             On a failure connecting to the Mock MongoDB server.
     */
    @Test
    public void testClose() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        myTestFactory = new AuthenticationConnectionFactory(mockFactory, config);

        mockFactory.close();
        expectLastCall();

        replay(mockFactory);

        myTestFactory.close();

        verify(mockFactory);
    }

    /**
     * Test method for {@link AuthenticationConnectionFactory#connect()}.
     * 
     * @throws IOException
     *             On a failure setting up the test mocks.
     */
    @Test
    public void testConnect() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        myTestFactory = new AuthenticationConnectionFactory(mockFactory, config);

        expect(mockFactory.connect()).andReturn(mockConnection);

        replay(mockFactory, mockConnection);

        final AuthenticatingConnection conn = myTestFactory.connect();
        assertSame(mockConnection, conn.getProxiedConnection());

        verify(mockFactory, mockConnection);
    }

    /**
     * Test method for
     * {@link AuthenticationConnectionFactory#connect(ServerState, MongoClientConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a test failure setting up mocks.
     */
    @Test
    public void testConnectInetSocketAddressMongoDbConfiguration()
            throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        myTestFactory = new AuthenticationConnectionFactory(mockFactory, config);

        expect(mockFactory.connect(anyObject(ServerState.class), eq(config)))
                .andReturn(mockConnection);

        replay(mockFactory, mockConnection);

        final AuthenticatingConnection conn = myTestFactory.connect(
                new ServerState(new InetSocketAddress("localhost", 27017)),
                config);
        assertSame(mockConnection, conn.getProxiedConnection());

        verify(mockFactory, mockConnection);
    }

    /**
     * Test method for {@link AuthenticationConnectionFactory#getClusterType()}.
     * 
     * @throws IOException
     *             on a test failure.
     */
    @Test
    public void testGetClusterType() throws IOException {
        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        myTestFactory = new AuthenticationConnectionFactory(mockFactory, config);

        expect(mockFactory.getClusterType()).andReturn(ClusterType.STAND_ALONE);

        replay(mockFactory);

        assertEquals(ClusterType.STAND_ALONE, myTestFactory.getClusterType());

        verify(mockFactory);
    }

    /**
     * Test method for
     * {@link AuthenticationConnectionFactory#getReconnectStrategy()}.
     */
    @Test
    public void testGetReconnectStrategy() {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final ReconnectStrategy mockStrategy = createMock(ReconnectStrategy.class);

        myTestFactory = new AuthenticationConnectionFactory(mockFactory, config);

        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        mockStrategy.setConnectionFactory(myTestFactory);
        expectLastCall();

        replay(mockFactory, mockStrategy);

        assertSame(mockStrategy, myTestFactory.getReconnectStrategy());

        verify(mockFactory, mockStrategy);
    }

}
