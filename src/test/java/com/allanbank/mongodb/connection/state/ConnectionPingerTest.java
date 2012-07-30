/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.makeThreadSafe;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.easymock.Capture;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.ServerStatus;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ConnectionPingerTest provides tests for the {@link ConnectionPinger} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ConnectionPingerTest {

    /** The pinger being tested. */
    protected ConnectionPinger myPinger = null;

    /**
     * Cleans up the pinger.
     */
    @After
    public void tearDown() {
        IOUtils.close(myPinger);
        myPinger = null;
    }

    /**
     * Test method for {@link ConnectionPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRun() throws IOException, InterruptedException {
        final ClusterState cluster = new ClusterState();
        cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockConnection, true);
        makeThreadSafe(mockFactory, true);

        expect(
                mockFactory.connect(anyObject(InetSocketAddress.class),
                        anyObject(MongoDbConfiguration.class))).andReturn(
                mockConnection);

        mockConnection.send(capture(new CallbackCapture()),
                anyObject(ServerStatus.class));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ConnectionPinger(cluster, mockFactory,
                new MongoDbConfiguration());

        Thread.sleep(100);
        IOUtils.close(myPinger);

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ConnectionPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunPingFails() throws IOException, InterruptedException {
        final ClusterState cluster = new ClusterState();
        cluster.add("localhost:27017");

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockConnection, true);
        makeThreadSafe(mockFactory, true);

        expect(
                mockFactory.connect(anyObject(InetSocketAddress.class),
                        anyObject(MongoDbConfiguration.class))).andReturn(
                mockConnection);

        mockConnection.send(capture(new CallbackFailureCapture()),
                anyObject(ServerStatus.class));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ConnectionPinger(cluster, mockFactory,
                new MongoDbConfiguration());

        Thread.sleep(100);
        IOUtils.close(myPinger);

        verify(mockConnection, mockFactory);
    }

    /**
     * Test method for {@link ConnectionPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunThrowsIOException() throws IOException,
            InterruptedException {
        final ClusterState cluster = new ClusterState();
        cluster.add("localhost:27017");

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockFactory, true);

        expect(
                mockFactory.connect(anyObject(InetSocketAddress.class),
                        anyObject(MongoDbConfiguration.class))).andThrow(
                new IOException("This is a test."));

        replay(mockFactory);

        myPinger = new ConnectionPinger(cluster, mockFactory,
                new MongoDbConfiguration());

        Thread.sleep(250);

        verify(mockFactory);
    }

    /**
     * CallbackCapture provides the ability to trigger the ping callback.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class CallbackCapture extends Capture<Callback<Reply>> {
        /** Serialization version for the class. */
        private static final long serialVersionUID = -8744386051520804331L;

        @Override
        public void setValue(final Callback<Reply> value) {
            super.setValue(value);

            value.callback(null);

            Thread.currentThread().interrupt();
            try {
                myPinger.close();
            }
            catch (final IOException e) {
                // Ignored
            }
        }
    }

    /**
     * CallbackFailureCapture provides the ability to trigger the ping callback
     * failure.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected final class CallbackFailureCapture extends
            Capture<Callback<Reply>> {
        /** Serialization version for the class. */
        private static final long serialVersionUID = -8744386051520804331L;

        @Override
        public void setValue(final Callback<Reply> value) {
            super.setValue(value);

            value.exception(null);

            Thread.currentThread().interrupt();
            try {
                myPinger.close();
            }
            catch (final IOException e) {
                // Ignored
            }
        }
    }
}
