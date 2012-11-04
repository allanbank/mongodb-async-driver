/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.ServerStatus;
import com.allanbank.mongodb.connection.socket.SocketConnection;
import com.allanbank.mongodb.connection.state.ServerState;
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
     * Sets up to create a connection to MongoDB.
     */
    @Before
    @Override
    public void connect() {
        myConfig = new MongoDbConfiguration();
        myConfig.addServer(new InetSocketAddress("127.0.0.1", DEFAULT_PORT));
        myConfig.setAutoDiscoverServers(true);
        myConfig.setMaxConnectionCount(1);
        myConfig.setReconnectTimeout(90000);

        super.connect();
    }

    /**
     * Tests recovery from a graceful step-down of a server.
     */
    @Test
    public void testGracefulStepdownRecovery() {

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
            disconnect();
            startServer();
        }
    }

    /**
     * Tests that using the secondary preferred read preference submits queries
     * to secondaries.
     * 
     * @throws IOException
     *             On a test failure.
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testSecondaryPreferredLoading() throws IOException,
            InterruptedException, ExecutionException {

        final int count = SMALL_COLLECTION_COUNT * 5;

        // Collect the current metrics for each server.
        final int[] ports = new int[] { 27018, 27019, 27020 };
        final SocketConnection[] conns = new SocketConnection[ports.length];

        disconnect();
        Thread.sleep(5000);
        connect();
        myConfig.setAutoDiscoverServers(true);
        myConfig.setMaxConnectionCount(5);
        myConfig.setDefaultReadPreference(ReadPreference.preferSecondary());
        myConfig.setDefaultDurability(Durability.replicaDurable(2, 1000));

        for (int i = 0; i < ports.length; ++i) {
            final int port = ports[i];

            conns[i] = new SocketConnection(
                    new ServerState("localhost:" + port), myConfig);
            conns[i].start();
        }

        final int[] beforeInsert = new int[ports.length];
        final int[] beforeUpdate = new int[ports.length];
        final int[] beforeDelete = new int[ports.length];
        final int[] beforeGetmore = new int[ports.length];
        final int[] beforeQuery = new int[ports.length];
        final int[] beforeCommand = new int[ports.length];
        for (int i = 0; i < ports.length; ++i) {

            final FutureCallback<Reply> replyFuture = new FutureCallback<Reply>();
            conns[i].send(new ServerStatus(), replyFuture);

            final Reply reply = replyFuture.get();
            assertEquals(1, reply.getResults().size());

            final Document doc = reply.getResults().get(0);
            beforeInsert[i] = extractOpCounter(doc, "insert");
            beforeUpdate[i] = extractOpCounter(doc, "update");
            beforeDelete[i] = extractOpCounter(doc, "delete");
            beforeGetmore[i] = extractOpCounter(doc, "getmore");
            beforeQuery[i] = extractOpCounter(doc, "query");
            beforeCommand[i] = extractOpCounter(doc, "command");

            // int port = ports[i];
            // System.out.println("localhost:" + port + " insert="
            // + beforeInsert[i] + " update=" + beforeUpdate[i]
            // + " delete=" + beforeDelete[i] + " getmore="
            // + beforeGetmore[i] + " query=" + beforeQuery[i]
            // + " command=" + beforeCommand[i]);
        }

        // Now apply some load.
        final DocumentBuilder builder = BuilderFactory.start();
        for (int i = 0; i < count; ++i) {
            builder.reset();
            builder.add("_id", i);
            builder.add("foo", -i);
            builder.add("bar", i);
            builder.add("baz", String.valueOf(i));

            myCollection.insertAsync(builder);
        }

        // Now go find them all -- Spin very fast until they are all found.
        final int[] afterInsert = new int[ports.length];
        final int[] afterUpdate = new int[ports.length];
        final int[] afterDelete = new int[ports.length];
        final int[] afterGetmore = new int[ports.length];
        final int[] afterQuery = new int[ports.length];
        final int[] afterCommand = new int[ports.length];
        int missed = 1;
        int tries = 0;
        while (missed > 0) {
            missed = 0;
            tries += 1;
            for (int i = 0; i < count; ++i) {
                if (myCollection.findOne(QueryBuilder.where("_id").equals(i)) == null) {
                    missed += 1;
                }
            }

            // System.out.println("Missed " + missed + " of "
            // + SMALL_COLLECTION_COUNT);

            for (int i = 0; i < ports.length; ++i) {

                final FutureCallback<Reply> replyFuture = new FutureCallback<Reply>();
                conns[i].send(new ServerStatus(), replyFuture);

                final Reply reply = replyFuture.get();
                assertEquals(1, reply.getResults().size());

                final Document doc = reply.getResults().get(0);
                afterInsert[i] = extractOpCounter(doc, "insert");
                afterUpdate[i] = extractOpCounter(doc, "update");
                afterDelete[i] = extractOpCounter(doc, "delete");
                afterGetmore[i] = extractOpCounter(doc, "getmore");
                afterQuery[i] = extractOpCounter(doc, "query");
                afterCommand[i] = extractOpCounter(doc, "command");

                // int port = ports[i];
                // System.out.println("localhost:" + port + " insert="
                // + afterInsert[i] + " update=" + afterUpdate[i]
                // + " delete=" + afterDelete[i] + " getmore="
                // + afterGetmore[i] + " query=" + afterQuery[i]
                // + " command=" + afterCommand[i]);
            }
        }

        // System.out.println("Took " + tries);

        // Look for tries * count queries.
        int queries = 0;
        final int[] deltas = new int[ports.length];
        for (int i = 0; i < ports.length; ++i) {
            final int delta = (afterQuery[i] - beforeQuery[i]);

            queries += delta;
            deltas[i] = delta;
        }
        assertTrue((tries * count) <= queries);

        // Look for a delta that is << count - The primary.
        int index = -1;
        for (int i = 0; i < ports.length; ++i) {
            if (deltas[i] < (tries * 10)) {
                if (index == -1) {
                    index = i;
                }
                else {
                    fail("Found to servers with very limited queries.");
                }
            }
        }
        assertFalse("Did not find a server with limited queries.", index == -1);

        // The remaining servers should be within 90% of (tries *
        // count) / (ports.length - 1) - Local servers so the clock has a hard
        // time measuring the latencies and causes large deltas which effect the
        // distributions.
        final int expected = (tries * count) / (ports.length - 1);
        final int low = (int) (expected * 0.1);
        final int high = (int) (expected * 1.9);
        for (int i = 0; i < ports.length; ++i) {
            if (i != index) {
                assertTrue("Server got below the expected number of queries '"
                        + low + "' vs. '" + deltas[i] + "'", low <= deltas[i]);
                assertTrue("Server got above the expected number of queries '"
                        + deltas[i] + "' vs. '" + high + "'", deltas[i] <= high);
            }
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
    // Thread.sleep(1000);
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

    /**
     * Extracts the specified opcounter value from the document.
     * 
     * @param doc
     *            The document to pull the value from.
     * @param op
     *            The operation to pull.
     * @return The operation count or -1 if it is not found.
     */
    private int extractOpCounter(final Document doc, final String op) {
        final List<NumericElement> es = doc.find(NumericElement.class,
                "opcounters", op);
        if (es.isEmpty()) {
            return -1;
        }
        return es.get(0).getIntValue();
    }
}
