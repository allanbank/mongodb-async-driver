/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * BasicAcceptanceTestCases provides TODO - Finish
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetAcceptanceTest extends BasicAcceptanceTestCases {

    /**
     * Starts the sharded server before the tests.
     */
    @BeforeClass
    public static void startServer() {
        startReplicaSet();
    }

    /**
     * Stops the servers running in a sharded mode.
     */
    @AfterClass
    public static void stopServer() {
        stopReplicaSet();
    }

    /**
     * Tests recovery from a graceful step-down of a server.
     */
    @Test
    @Ignore("Not recoverying yet.")
    public void testGracefulStepdownRecovery() {
        myConfig.setAutoDiscoverServers(true);

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().get());

        assertTrue(myMongo.listDatabases().contains(TEST_DB_NAME));

        try {
            // Stop the main shard.
            ourBuilder.command("pkill", "-f", "27018");
            final Process kill = ourBuilder.start();
            kill.waitFor();

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
            startServer();
        }
    }

    /**
     * Test recovery from a sudden server failure.
     */
    @Test
    @Ignore("Not recoverying yet.")
    public void testSuddenFailureRecovery() {
        myConfig.setAutoDiscoverServers(true);

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().get());

        assertTrue(myMongo.listDatabases().contains(TEST_DB_NAME));

        try {
            // Stop the main shard.
            ourBuilder.command("pkill", "-f", "27018");
            final Process kill = ourBuilder.start();
            kill.waitFor();

            // Should switch to the other shard.
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
