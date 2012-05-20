/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;

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
     * Tests the handling of a mongos server getting shutdown.
     */
    @Test
    @Ignore("Need to do validation.")
    public void testSuddenFailureHandling() {
        myConfig.setAutoDiscoverServers(true);

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().get());

        assertEquals(Arrays.asList(TEST_DB_NAME, "config"),
                myMongo.listDatabases());

        try {
            // Stop the main mongos.
            ourBuilder.command("pkill", "-f", "27017");
            final Process kill = ourBuilder.start();
            kill.waitFor();

            myConfig.addServer(new InetSocketAddress("127.0.0.1",
                    DEFAULT_PORT - 1));

            // Should switch to the other mongos.
            myMongo.listDatabases();
        }
        catch (final Exception e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
        finally {
            // Make sure the server is restarted for the other tests.
            startServer();
        }
    }
}
