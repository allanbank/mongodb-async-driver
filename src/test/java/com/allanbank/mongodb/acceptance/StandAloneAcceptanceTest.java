/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.error.CannotConnectException;

/**
 * StandAloneAcceptanceTest performs acceptance tests for the driver against a
 * standalone MongoDB shard server.
 * <p>
 * These are not meant to be exhaustive tests of the driver but instead attempt
 * to demonstrate that interactions with the MongoDB processes work.
 * </p>
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class StandAloneAcceptanceTest extends BasicAcceptanceTestCases {

    /**
     * Starts the standalone server before the tests.
     */
    @BeforeClass
    public static void startServer() {
        startStandAlone();
    }

    /**
     * Stops the servers running in a standalone mode.
     */
    @AfterClass
    public static void stopServer() {
        stopStandAlone();
    }

    /**
     * Tests the graceful handling of the server getting shutdown.
     */
    @Test
    public void testReconnectHandling() {
        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().get());

        assertEquals(Arrays.asList(TEST_DB_NAME, "local"),
                myMongo.listDatabases());

        // Stop the server.
        try {
            stopServer();
            startServer();
            // Don't assert the databases since the stop/start scripts remove
            // the data directories.
            myMongo.listDatabases();
        }
        finally {
            // Make sure the server is restarted for the other tests.
            startServer();
        }
    }

    /**
     * Tests the graceful handling of the server getting shutdown.
     */
    @Test
    public void testSuddenFailureHandling() {
        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().get());

        assertEquals(Arrays.asList(TEST_DB_NAME, "local"),
                myMongo.listDatabases());

        // Stop the server.
        try {
            stopServer();

            myMongo.listDatabases();
            fail("Should have thrown an exception.");
        }
        catch (final CannotConnectException good) {
            // Good.
        }
        finally {
            // Make sure the server is restarted for the other tests.
            startServer();
        }
    }

}
