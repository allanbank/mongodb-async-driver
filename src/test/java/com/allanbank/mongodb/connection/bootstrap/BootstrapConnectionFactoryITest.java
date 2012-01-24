/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.bootstrap;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.rs.ReplicaSetConnectionFactory;
import com.allanbank.mongodb.connection.sharded.ShardedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;

/**
 * Integration test for the {@link BootstrapConnectionFactory}.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BootstrapConnectionFactoryITest {

    /** The directory containing the scripts. */
    private static final File SCRIPT_DIR = new File("src/test/scripts");

    /** A builder for executing background process scripts. */
    private ProcessBuilder myBuilder = null;

    /**
     * Creates a builder for executing background process scripts.
     */
    @Before
    public void setUp() {
        myBuilder = new ProcessBuilder();
        myBuilder.directory(SCRIPT_DIR);
    }

    /**
     * Stops all of the background processes started.
     */
    @After
    public void tearDown() {
        Process stop = null;
        try {
            myBuilder.command(
                    new File(SCRIPT_DIR, "replica_set.sh").getAbsolutePath(),
                    "stop");
            stop = myBuilder.start();
            stop.waitFor();
        }
        catch (final IOException ioe) {
            // Ignore - best effort.
        }
        catch (final InterruptedException e) {
            // Ignore - best effort.
        }

        try {
            myBuilder.command(
                    new File(SCRIPT_DIR, "sharded.sh").getAbsolutePath(),
                    "stop");
            stop = myBuilder.start();
            stop.waitFor();
        }
        catch (final IOException ioe) {
            // Ignore - best effort.
        }
        catch (final InterruptedException e) {
            // Ignore - best effort.
        }

        try {
            myBuilder.command(
                    new File(SCRIPT_DIR, "standalone.sh").getAbsolutePath(),
                    "stop");
            stop = myBuilder.start();
            stop.waitFor();
        }
        catch (final IOException ioe) {
            // Ignore - best effort.
        }
        catch (final InterruptedException e) {
            // Ignore - best effort.
        }
        myBuilder = null;
    }

    /**
     * Test method for
     * {@link BootstrapConnectionFactory#bootstrap(MongoDbConfiguration)} .
     */
    @Test
    public void testBootstrapReplicaSet() {
        try {
            myBuilder.command(
                    new File(SCRIPT_DIR, "replica_set.sh").getAbsolutePath(),
                    "start");
            final Process start = myBuilder.start();
            start.waitFor();

            final MongoDbConfiguration config = new MongoDbConfiguration(
                    new InetSocketAddress("127.0.0.1", 27017));
            final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                    config);

            assertThat("Wrong type of factory.", factory.getDelegate(),
                    CoreMatchers.instanceOf(ReplicaSetConnectionFactory.class));
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for
     * {@link BootstrapConnectionFactory#bootstrap(MongoDbConfiguration)} .
     */
    @Test
    public void testBootstrapSharded() {
        try {
            myBuilder.command(
                    new File(SCRIPT_DIR, "sharded.sh").getAbsolutePath(),
                    "start");
            final Process start = myBuilder.start();
            start.waitFor();

            final MongoDbConfiguration config = new MongoDbConfiguration(
                    new InetSocketAddress("127.0.0.1", 27017));
            final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                    config);

            assertThat("Wrong type of factory.", factory.getDelegate(),
                    CoreMatchers.instanceOf(ShardedConnectionFactory.class));
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for
     * {@link BootstrapConnectionFactory#bootstrap(MongoDbConfiguration)} .
     */
    @Test
    public void testBootstrapStandalone() {
        try {
            myBuilder.command(
                    new File(SCRIPT_DIR, "standalone.sh").getAbsolutePath(),
                    "start");
            final Process start = myBuilder.start();
            start.waitFor();

            final MongoDbConfiguration config = new MongoDbConfiguration(
                    new InetSocketAddress("127.0.0.1", 27017));
            final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                    config);

            assertThat("Wrong type of factory.", factory.getDelegate(),
                    CoreMatchers.instanceOf(SocketConnectionFactory.class));
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test method for {@link BootstrapConnectionFactory#connect()} .
     */
    @Test
    public void testConnect() {
        try {
            myBuilder.command(
                    new File(SCRIPT_DIR, "standalone.sh").getAbsolutePath(),
                    "start");
            final Process start = myBuilder.start();
            start.waitFor();

            final MongoDbConfiguration config = new MongoDbConfiguration(
                    new InetSocketAddress("127.0.0.1", 27017));
            final BootstrapConnectionFactory factory = new BootstrapConnectionFactory(
                    config);

            final Connection mockConnection = createMock(Connection.class);
            final ConnectionFactory mockFactory = createMock(ConnectionFactory.class);

            expect(mockFactory.connect()).andReturn(mockConnection);

            replay(mockConnection, mockFactory);

            factory.setDelegate(mockFactory);
            assertSame(mockConnection, factory.connect());

            verify(mockConnection, mockFactory);
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }
}
