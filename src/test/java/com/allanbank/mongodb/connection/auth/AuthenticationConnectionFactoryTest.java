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
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
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
        final MongoDbConfiguration config = new MongoDbConfiguration(
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
        final MongoDbConfiguration config = new MongoDbConfiguration();
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
     * {@link AuthenticationConnectionFactory#connect(ServerState, MongoDbConfiguration)}
     * .
     * 
     * @throws IOException
     *             On a test failure setting up mocks.
     */
    @Test
    public void testConnectInetSocketAddressMongoDbConfiguration()
            throws IOException {
        final MongoDbConfiguration config = new MongoDbConfiguration();
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);
        final Connection mockConnection = createMock(Connection.class);

        final String socketAddr = "localhost:27017";

        myTestFactory = new AuthenticationConnectionFactory(mockFactory, config);

        expect(mockFactory.connect(anyObject(ServerState.class), eq(config)))
                .andReturn(mockConnection);

        replay(mockFactory, mockConnection);

        final AuthenticatingConnection conn = myTestFactory.connect(
                new ServerState(socketAddr), config);
        assertSame(mockConnection, conn.getProxiedConnection());

        verify(mockFactory, mockConnection);
    }

    /**
     * Test method for
     * {@link AuthenticationConnectionFactory#getReconnectStrategy()}.
     */
    @Test
    public void testGetReconnectStrategy() {
        final MongoDbConfiguration config = new MongoDbConfiguration();
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
