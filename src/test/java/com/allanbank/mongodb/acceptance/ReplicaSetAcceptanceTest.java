/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * BasicAcceptanceTestCases provides acceptance test cases for when interacting
 * with a replica set.
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
    public void testGracefulStepdownRecovery() {
        myConfig.setAutoDiscoverServers(true);
        myConfig.setReconnectTimeout(90000);

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().build());

        assertTrue(myMongo.listDatabases().contains(TEST_DB_NAME));

        try {
            // Need to give the primary time to discover the others are
            // secondaries are around and are up to date. Otherwise he refuses
            // to step down.
            TimeUnit.SECONDS.sleep(20);

            // Stop the main shard.
            myMongo.getDatabase("admin").runAdminCommand("replSetStepDown");

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
            Thread.sleep(500);

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
            disconnect();
            startServer();
        }
    }

    /**
     * Test recovery from a sudden server failure.
     */
    // @Test
    // public void testSuddenFailureRecovery() {
    // myConfig.setAutoDiscoverServers(true);
    // myConfig.setReconnectTimeout(90000);
    //
    // // Make sure the collection/db exist and we are connected.
    // myCollection.insert(BuilderFactory.start().build());
    //
    // assertTrue(myMongo.listDatabases().contains(TEST_DB_NAME));
    //
    // try {
    // // Stop the main shard.
    // ourBuilder.command("pkill", "-f", "27018");
    // final Process kill = ourBuilder.start();
    // kill.waitFor();
    //
    // // Quick command that should then fail.
    // myMongo.listDatabases();
    //
    // // ... but its OK if it misses getting out before the Process dies.
    // }
    // catch (final ConnectionLostException cle) {
    // // Good.
    // }
    // catch (final Exception e) {
    // final AssertionError error = new AssertionError(e.getMessage());
    // error.initCause(e);
    // throw error;
    // }
    //
    // try {
    // Thread.sleep(100);
    //
    // // Should switch to the other shards.
    // myMongo.listDatabases();
    // }
    // catch (final Exception e) {
    // final AssertionError error = new AssertionError(e.getMessage());
    // error.initCause(e);
    // throw error;
    // }
    // finally {
    // // Make sure the server is restarted for the other tests.
    // startServer();
    // }
    // }
}
