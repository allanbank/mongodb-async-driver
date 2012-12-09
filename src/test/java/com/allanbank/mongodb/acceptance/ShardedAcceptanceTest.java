/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * BasicAcceptanceTestCases performs acceptance tests for the driver against a
 * sharded MongoDB configuration.
 * <p>
 * These are not meant to be exhaustive tests of the driver but instead attempt
 * to demonstrate that interactions with the MongoDB processes work.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedAcceptanceTest extends BasicAcceptanceTestCases {

    /**
     * Starts the sharded server before the tests.
     */
    @BeforeClass
    public static void startServer() {
        startSharded();
    }

    /**
     * Stops the servers running in a sharded mode.
     */
    @AfterClass
    public static void stopServer() {
        stopSharded();
    }

    /**
     * Sets up to create a connection to MongoDB.
     */
    @Before
    @Override
    public void connect() {
        myConfig = new MongoDbConfiguration();
        myConfig.addServer(new InetSocketAddress("127.0.0.1", DEFAULT_PORT));
        myConfig.setAutoDiscoverServers(true);
        myConfig.setMaxConnectionCount(1);
        myConfig.setReconnectTimeout(60000);

        super.connect();
    }

    /**
     * Tests the handling of a mongos server getting shutdown.
     */
    @Test
    public void testSuddenFailureHandling() {
        myConfig.setAutoDiscoverServers(true);
        myConfig.setMaxConnectionCount(1);
        myConfig.setReconnectTimeout(60000);

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().build());

        assertEquals(Arrays.asList(TEST_DB_NAME, "config"),
                myMongo.listDatabases());

        try {
            // Stop the main mongos.
            final ProcessBuilder builder = new ProcessBuilder("pkill", "-f",
                    "27017");
            final Process kill = builder.start();
            kill.waitFor();

            // Quick command that should then fail.
            myMongo.listDatabases();

            // ... but its OK if it misses getting out before the Process dies.
        }
        catch (final ConnectionLostException cle) {
            // Good.
        }
        catch (final Exception e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }

        try {
            Thread.sleep(1000);

            // Should switch to the other shards.
            myMongo.listDatabases();
        }
        catch (final Exception e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
        finally {
            // Make sure the server is restarted for the other tests.
            // disconnect();
            startServer();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true.
     * </p>
     */
    @Override
    protected boolean isShardedConfiguration() {
        return true;
    }
}
