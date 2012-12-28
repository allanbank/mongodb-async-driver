/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.state;

import static com.allanbank.mongodb.connection.CallbackReply.cb;
import static com.allanbank.mongodb.connection.CallbackReply.reply;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.makeThreadSafe;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.CallbackReply;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.message.IsMaster;
import com.allanbank.mongodb.connection.message.ReplicaSetStatus;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ClusterPingerTest provides tests for the {@link ClusterPinger} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ClusterPingerTest {
    /** The pinger being tested. */
    protected ClusterPinger myPinger = null;

    /**
     * Cleans up the pinger.
     */
    @After
    public void tearDown() {
        IOUtils.close(myPinger);
        myPinger = null;
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweep() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(mockConnection.send(anyObject(IsMaster.class), cb(reply)))
                .andReturn(address);

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.initialSweep();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepCannotGiveBackConnection() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndCloseWithConn(reply, state, mockConnection)))
                .andReturn(address);
        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.initialSweep();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepConnectionAlreadyOpen() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndCloseWithConn(reply, state, mockConnection)))
                .andReturn(address);
        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        state.addConnection(mockConnection);
        myPinger.initialSweep();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepFails() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cb(new MongoDbException("Error")))).andReturn(address);

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.initialSweep();

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepReplicaSet() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(mockConnection.send(anyObject(IsMaster.class), cb(reply)))
                .andReturn(address);
        expect(
                mockConnection.send(anyObject(ReplicaSetStatus.class),
                        cb(reply))).andReturn(address);

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.REPLICA_SET,
                mockFactory, new MongoClientConfiguration());
        myPinger.initialSweep();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepThrowsIOException() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andThrow(
                new IOException("Injected"));

        replay(mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.initialSweep();

        verify(mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testInitialSweepThrowsMongoDdException() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        anyObject(ServerLatencyCallback.class))).andThrow(
                new MongoDbException("Injected"));

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.initialSweep();

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#initialSweep()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep in the test.
     */
    @Test
    public void testInitialSweepWhenInterrupted() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockFactory, true);
        makeThreadSafe(mockConnection, true);

        final Capture<ServerLatencyCallback> catureReply = new Capture<ServerLatencyCallback>();

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        capture(catureReply))).andReturn(address);

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        final Thread t = new Thread() {
            @Override
            public void run() {
                myPinger.initialSweep();
            }
        };
        t.start();
        Thread.sleep(20);
        t.interrupt();
        Thread.sleep(50);

        verify(mockConnection, mockFactory);

        catureReply.getValue().callback(reply(reply));

        t.join(1000);

        assertFalse(t.isAlive());

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRun() throws IOException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndClose(reply))).andReturn(address);

        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertNull(state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunBadPingReply() throws IOException {
        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(mockConnection.send(anyObject(IsMaster.class), cbAndClose()))
                .andReturn(address);

        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
        assertNull(state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunCannotGiveConnectionBack() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndCloseWithConn(reply, state, mockConnection)))
                .andReturn(address);

        // Have to shutdown the connection since state won't accept it.
        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunInThread() throws IOException, InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockConnection, true);
        makeThreadSafe(mockFactory, true);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(mockConnection.send(anyObject(IsMaster.class), cb(reply)))
                .andReturn(address);

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(30);
        myPinger.start();
        Thread.sleep(40);
        assertSame(mockConnection, state.takeConnection()); // stop will clear
                                                            // so check before.
        myPinger.stop();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunNoTags() throws IOException {

        final DocumentBuilder reply = BuilderFactory.start();
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndClose(reply))).andReturn(address);

        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertNull(state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunPingFails() throws IOException {
        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndCloseError())).andReturn(address);

        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();
        IOUtils.close(myPinger);

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
        assertNull(state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunSweepTwice() throws IOException, InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        cluster.myServers.put(address, new ServerState(address) {
            @Override
            public long getConnectionGeneration() {
                // Randomize the generation to make it look like a very busy
                // server.
                return (long) Math.ceil(Math.random() * 1000000000);
            }
        });

        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(mockConnection.send(anyObject(IsMaster.class), cb(reply)))
                .andReturn(address);

        // Second Sweep.
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndClose(reply))).andReturn(address);

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertSame(mockConnection, state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunSweepTwiceIdleConnection() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(mockConnection.send(anyObject(IsMaster.class), cb(reply)))
                .andReturn(address);
        mockConnection.shutdown();
        expectLastCall();

        // Second Sweep.
        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndClose(reply))).andReturn(address);
        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertNull(state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunSweepTwiceNotGiveBackConnection() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());
        reply.add("ismaster", true);

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbWithConn(reply, state, mockConnection))).andReturn(
                address);
        mockConnection.shutdown();
        expectLastCall();

        // Second Sweep.
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndClose(reply))).andReturn(address);
        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockConnection, mockFactory);

        assertEquals(reply.build().get("tags"), state.getTags());
        assertTrue(state.getAverageLatency() < 100);
        assertNull(state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunThrowsIOException() throws IOException {
        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andAnswer(
                a(new IOException("Injected")));

        replay(mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();

        verify(mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     */
    @Test
    public void testRunThrowsMongoDbException() throws IOException {
        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        cbAndCloseError())).andAnswer(
                throwA(new MongoDbException("Injected")));

        mockConnection.shutdown();
        expectLastCall();

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(1);
        myPinger.run();
        IOUtils.close(myPinger);

        verify(mockConnection, mockFactory);

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
        assertNull(state.takeConnection());
    }

    /**
     * Test method for {@link ClusterPinger#run()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks.
     * @throws InterruptedException
     *             On a failure to sleep.
     */
    @Test
    public void testRunWhenInterrupted() throws IOException,
            InterruptedException {

        final DocumentBuilder tags = BuilderFactory.start();
        tags.addInteger("f", 1).addInteger("b", 1);

        final DocumentBuilder reply = BuilderFactory.start();
        reply.addDocument("tags", tags.build());

        final String address = "localhost:27017";

        final ClusterState cluster = new ClusterState(
                new MongoClientConfiguration());
        final ServerState state = cluster.add(address);

        final Connection mockConnection = createMock(Connection.class);
        final ProxiedConnectionFactory mockFactory = createMock(ProxiedConnectionFactory.class);

        makeThreadSafe(mockConnection, true);
        makeThreadSafe(mockFactory, true);

        final Capture<ServerLatencyCallback> catureReply = new Capture<ServerLatencyCallback>();
        expect(
                mockFactory.connect(eq(state),
                        anyObject(MongoClientConfiguration.class))).andReturn(
                mockConnection);
        expect(
                mockConnection.send(anyObject(IsMaster.class),
                        capture(catureReply))).andReturn(address);

        replay(mockConnection, mockFactory);

        myPinger = new ClusterPinger(cluster, ClusterType.STAND_ALONE,
                mockFactory, new MongoClientConfiguration());
        myPinger.setIntervalUnits(TimeUnit.MILLISECONDS);
        myPinger.setPingSweepInterval(20);
        final Thread t = new Thread(myPinger);
        t.start();
        Thread.sleep(50); // Wait on a reply.
        t.interrupt();
        Thread.sleep(10);

        myPinger.stop();
        t.interrupt();

        verify(mockConnection, mockFactory);

        t.join(1000);
        assertFalse(t.isAlive());

        assertNull(state.getTags());
        assertEquals(Double.MAX_VALUE, state.getAverageLatency(), 0.0001);
    }

    /**
     * Creates a new CloseAnswer.
     * 
     * @param reply
     *            The reply to return.
     * @return The CloseAnswer.
     */
    protected <C> IAnswer<C> a(final C reply) {
        return new CloseAnswer<C>(reply);
    }

    /**
     * Creates a new CloseAnswer.
     * 
     * @param reply
     *            The reply to throw.
     * @return The CloseAnswer.
     */
    protected IAnswer<Connection> a(final Throwable reply) {
        return new CloseAnswer<Connection>(reply);
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param builders
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbAndClose(final DocumentBuilder... builders) {
        return cbAndClose(CallbackReply.reply(builders));
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param reply
     *            The reply to provide to the callback.
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbAndClose(final Reply reply) {
        EasyMock.capture(new CloseCallbackReply(reply));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param error
     *            The error to provide to the callback.
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbAndClose(final Throwable error) {
        EasyMock.capture(new CloseCallbackReply(error));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbAndCloseError() {
        EasyMock.capture(new CloseCallbackReply(new Throwable("Injected")));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param builder
     *            The reply to provide to the callback.
     * @param state
     *            The state to give the connection to.
     * @param conn
     *            The connection to give the server.
     * 
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbAndCloseWithConn(final DocumentBuilder builder,
            final ServerState state, final Connection conn) {
        class CloseCallbackWithSetConnection extends CloseCallbackReply {

            private static final long serialVersionUID = -2458416861114720698L;

            public CloseCallbackWithSetConnection(final Reply reply) {
                super(reply);
            }

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);
                state.addConnection(conn);
            }
        }
        EasyMock.capture(new CloseCallbackWithSetConnection(CallbackReply
                .reply(builder)));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param builder
     *            The reply to provide to the callback.
     * @param state
     *            The state to give the connection to.
     * @param conn
     *            The connection to give the server.
     * 
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbConnUsed(final DocumentBuilder builder,
            final ServerState state, final Connection conn) {
        class CloseCallbackWithSetConnection extends CloseCallbackReply {

            private static final long serialVersionUID = -2458416861114720698L;

            public CloseCallbackWithSetConnection(final Reply reply) {
                super(reply);
            }

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);
                state.addConnection(state.takeConnection());
            }
        }
        EasyMock.capture(new CloseCallbackWithSetConnection(CallbackReply
                .reply(builder)));
        return null;
    }

    /**
     * Creates a new CallbackReply.
     * 
     * @param builder
     *            The reply to provide to the callback.
     * @param state
     *            The state to give the connection to.
     * @param conn
     *            The connection to give the server.
     * 
     * @return The CallbackReply.
     */
    protected Callback<Reply> cbWithConn(final DocumentBuilder builder,
            final ServerState state, final Connection conn) {
        class CallbackWithSetConnection extends CallbackReply {

            private static final long serialVersionUID = -2458416861114720698L;

            public CallbackWithSetConnection(final Reply reply) {
                super(reply);
            }

            @Override
            public void setValue(final Callback<Reply> value) {
                super.setValue(value);
                state.addConnection(conn);
            }
        }
        EasyMock.capture(new CallbackWithSetConnection(CallbackReply
                .reply(builder)));
        return null;
    }

    /**
     * Creates a new CloseAnswer.
     * 
     * @param reply
     *            The reply to throw.
     * @return The CloseAnswer.
     */
    protected IAnswer<String> throwA(final Throwable reply) {
        return new CloseAnswer<String>(reply);
    }

    /**
     * A specialized {@link IAnswer} to close the pinger.
     * 
     * @param <C>
     *            The type for the answer.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public final class CloseAnswer<C> implements IAnswer<C> {
        /** The error to provide to the callback. */
        private final Throwable myError;

        /** The reply to provide to the callback. */
        private final C myReply;

        /**
         * Creates a new CallbackReply.
         * 
         * @param reply
         *            The reply to provide to the callback.
         */
        public CloseAnswer(final C reply) {
            myReply = reply;
            myError = null;
        }

        /**
         * Creates a new CallbackReply.
         * 
         * @param error
         *            The error to provide to the callback.
         */
        public CloseAnswer(final Throwable error) {
            myReply = null;
            myError = error;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to throw the error or return the reply.
         * </p>
         */
        @Override
        public C answer() throws Throwable {
            myPinger.close();

            if (myError != null) {
                throw myError;
            }
            return myReply;
        }
    }

    /**
     * A specialized callback reply to close the pinger when a value is set.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public class CloseCallbackReply extends CallbackReply {

        /** The serialization version for the class. */
        private static final long serialVersionUID = -5855409833338626339L;

        /**
         * Creates a new CloseCallbackReply.
         * 
         * @param reply
         *            The reply for the callback.
         */
        public CloseCallbackReply(final Reply reply) {
            super(reply);
        }

        /**
         * Creates a new CloseCallbackReply.
         * 
         * @param thrown
         *            The error for the callback.
         */
        public CloseCallbackReply(final Throwable thrown) {
            super(thrown);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to call super and then provide the reply or error to the
         * callback.
         * </p>
         */
        @Override
        public void setValue(final Callback<Reply> value) {
            super.setValue(value);
            myPinger.close();
        }

    }
}
