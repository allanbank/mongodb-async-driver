/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;

/**
 * AuthenticationConnectionFactoryTest provides test for the
 * {@link AuthenticationConnectionFactory}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticationConnectionFactoryTest {

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

        final AuthenticationConnectionFactory testFactory = new AuthenticationConnectionFactory(
                mockFactory, config);

        expect(mockFactory.connect()).andReturn(mockConnection);

        replay(mockFactory, mockConnection);

        final AuthenticatingConnection conn = testFactory.connect();
        assertSame(mockConnection, conn.getProxiedConnection());

        verify(mockFactory, mockConnection);
    }

    /**
     * Test method for
     * {@link AuthenticationConnectionFactory#connect(InetSocketAddress, MongoDbConfiguration)}
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

        final InetAddress addr = InetAddress.getLoopbackAddress();
        final InetSocketAddress socketAddr = new InetSocketAddress(addr, 1234);

        final AuthenticationConnectionFactory testFactory = new AuthenticationConnectionFactory(
                mockFactory, config);

        expect(mockFactory.connect(socketAddr, config)).andReturn(
                mockConnection);

        replay(mockFactory, mockConnection);

        final AuthenticatingConnection conn = testFactory.connect(socketAddr,
                config);
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

        final AuthenticationConnectionFactory testFactory = new AuthenticationConnectionFactory(
                mockFactory, config);

        expect(mockFactory.getReconnectStrategy()).andReturn(mockStrategy);
        mockStrategy.setConnectionFactory(testFactory);
        expectLastCall();

        replay(mockFactory, mockStrategy);

        assertSame(mockStrategy, testFactory.getReconnectStrategy());

        verify(mockFactory, mockStrategy);
    }

}
