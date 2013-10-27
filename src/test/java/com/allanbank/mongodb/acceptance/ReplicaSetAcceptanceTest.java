/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
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
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.connection.socket.SocketConnection;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.ServerStatus;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.error.ConnectionLostException;

/**
 * BasicAcceptanceTestCases provides acceptance test cases for when interacting
 * with a replica set.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetAcceptanceTest extends BasicAcceptanceTestCases {

    /** The expected ports for the servers. */
    private static final int[] PORTS = new int[] { 27018, 27019, 27020 };

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
        myConfig = new MongoClientConfiguration();
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
        myConfig.setReconnectTimeout(90000);

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().build());

        assertTrue(myMongo.listDatabaseNames().contains(TEST_DB_NAME));

        // Step down the primary shard.
        stepDownPrimary(15);

        try {
            TimeUnit.MILLISECONDS.sleep(100);

            // Should switch to the other shards.
            myMongo.listDatabaseNames();
        }
        catch (final Exception e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
        finally {
            // Make sure the server is restarted for the other tests.
            repairReplicaSet();
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

        final SocketConnection[] conns = new SocketConnection[PORTS.length];

        disconnect();
        Thread.sleep(5000);
        connect();
        myConfig.setAutoDiscoverServers(true);
        myConfig.setMaxConnectionCount(5);
        myConfig.setDefaultReadPreference(ReadPreference.preferSecondary());
        myConfig.setDefaultDurability(Durability.replicaDurable(2, 1000));

        final Cluster cluster = new Cluster(myConfig);
        for (int i = 0; i < PORTS.length; ++i) {
            final int port = PORTS[i];

            conns[i] = new SocketConnection(cluster.add(new InetSocketAddress(
                    "localhost", port)), myConfig);
            conns[i].start();
        }

        final int[] beforeInsert = new int[PORTS.length];
        final int[] beforeUpdate = new int[PORTS.length];
        final int[] beforeDelete = new int[PORTS.length];
        final int[] beforeGetmore = new int[PORTS.length];
        final int[] beforeQuery = new int[PORTS.length];
        final int[] beforeCommand = new int[PORTS.length];
        for (int i = 0; i < PORTS.length; ++i) {

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
        final int[] afterInsert = new int[PORTS.length];
        final int[] afterUpdate = new int[PORTS.length];
        final int[] afterDelete = new int[PORTS.length];
        final int[] afterGetmore = new int[PORTS.length];
        final int[] afterQuery = new int[PORTS.length];
        final int[] afterCommand = new int[PORTS.length];
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
        }

        // Collect the counters again.
        for (int i = 0; i < PORTS.length; ++i) {

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
        }

        // Look for tries * count queries.
        int queries = 0;
        final int[] deltas = new int[PORTS.length];
        for (int i = 0; i < PORTS.length; ++i) {
            final int delta = (afterQuery[i] - beforeQuery[i]);

            queries += delta;
            deltas[i] = delta;
        }
        assertTrue((tries * count) <= queries);

        // Look for a delta that is << count - The primary.
        int index = -1;
        for (int i = 0; i < PORTS.length; ++i) {
            if (deltas[i] < (tries * 10)) {
                if (index == -1) {
                    index = i;
                }
                else {
                    fail("Found two servers with very limited queries: threshold="
                            + (tries * 10)
                            + ", delta["
                            + index
                            + "]: "
                            + deltas[index]
                            + ", delta["
                            + i
                            + "]: "
                            + deltas[i]);
                }
            }
        }
        assertFalse("Did not find a server (primary) with limited queries.",
                index == -1);

        // The remaining servers should be within 90% of (tries *
        // count) / (ports.length - 1) - Local servers so the clock has a hard
        // time measuring the latencies and causes large deltas which effect the
        // distributions.
        final int expected = (tries * count) / (PORTS.length - 1);
        final int low = (int) (expected * 0.1);
        final int high = (int) (expected * 1.9);
        for (int i = 0; i < PORTS.length; ++i) {
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
     * 
     * @throws InterruptedException
     *             On a failure to sleep in the test.
     */
    @Test
    public void testStillQuerySecondariesWhenNoPrimary()
            throws InterruptedException {
        final int stepUpSeconds = 15;
        final int deferSeconds = (stepUpSeconds * PORTS.length) * 2;
        final Find.Builder query = Find.builder().query(Find.ALL);

        myConfig.setAutoDiscoverServers(true);
        myConfig.setReconnectTimeout((int) TimeUnit.SECONDS
                .toMillis(stepUpSeconds * 2));

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().build());

        assertTrue(myMongo.listDatabaseNames().contains(TEST_DB_NAME));

        // Step down all of the shards.
        query.readPreference(ReadPreference.PRIMARY);
        final int servers = PORTS.length;
        for (int i = 0; i < servers; ++i) {
            // Make sure we have a connection to the primary.
            assertThat(myCollection.findOne(query),
                    notNullValue(Document.class));

            stepDownPrimary(deferSeconds);

            // Pause a beat to make sure the driver sees the stepdown.
            TimeUnit.MILLISECONDS.sleep(100);
        }

        // Now do a query to a secondary (which is everyone).
        query.readPreference(ReadPreference.PREFER_SECONDARY);
        assertThat(myCollection.findOne(query), notNullValue(Document.class));

        // Even prefer primary should work.
        query.readPreference(ReadPreference.PREFER_PRIMARY);
        assertThat(myCollection.findOne(query), notNullValue(Document.class));

        // Wait for a primary again. Repair blocks until there is a primary.
        repairReplicaSet();

        // And we can query the primary again.
        query.readPreference(ReadPreference.PRIMARY);
        assertThat(myCollection.findOne(query), notNullValue(Document.class));
    }

    /**
     * Test recovery from a sudden server failure.
     */
    @Test
    public void testSuddenFailureRecovery() {
        myConfig.setAutoDiscoverServers(true);
        myConfig.setReconnectTimeout(90000);

        // Make sure the collection/db exist and we are connected.
        myCollection.insert(BuilderFactory.start().build());

        assertTrue(myMongo.listDatabaseNames().contains(TEST_DB_NAME));

        try {
            // Stop the main shard.
            final ProcessBuilder builder = new ProcessBuilder("pkill", "-f",
                    "27018");
            final Process kill = builder.start();
            kill.waitFor();

            // Quick command that should then fail.
            myMongo.listDatabaseNames();

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
            myMongo.listDatabaseNames();
        }
        catch (final Exception e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
        finally {
            // Make sure the server is restarted for the other tests.
            repairReplicaSet();
        }
    }

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

    /**
     * Keeps trying to step down the primary until it works.
     * 
     * @param deferSeconds
     *            The number of seconds to stay a secondary after the step down.
     * @throws MongoDbException
     *             On a failure to step down the primary.
     * @throws AssertionError
     *             On a fatal error.
     */
    private void stepDownPrimary(final int deferSeconds)
            throws MongoDbException, AssertionError {
        final Document stepDownCommand = new ImmutableDocument(BuilderFactory
                .start().add("replSetStepDown", deferSeconds)
                .add("force", true));

        long now = System.currentTimeMillis();
        final long deadline = now + TimeUnit.SECONDS.toMillis(30);
        while (now < deadline) {
            try {
                // Stepdown the primary shard.
                myMongo.getDatabase("admin").runCommand(stepDownCommand);

                // ... the replStepDown will throw an exception.

                // ... or we need to give the primary time to discover the
                // others are secondaries are around and are up to date.
                // Otherwise the primary refuses to step down.
                TimeUnit.SECONDS.sleep(1);
                now = System.currentTimeMillis();
            }
            catch (final ConnectionLostException cle) {
                // Good - Step down done.
                break;
            }
            catch (final InterruptedException e) {
                final AssertionError error = new AssertionError(e.getMessage());
                error.initCause(e);
                throw error;
            }
        }
    }
}
