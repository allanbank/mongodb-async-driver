/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.performance;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.Mongo;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.client.MongoClient;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.WriteConcern;

/**
 * Performance Test harness.
 * 
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PerformanceITest {

    /** A builder for executing background process scripts. */
    private static ProcessBuilder ourBuilder = null;

    /** The directory containing the scripts. */
    private static final File SCRIPT_DIR = new File("src/test/scripts");

    /**
     * Starts the MongoDB server for the test.
     * 
     * @throws IOException
     *             On a failure to initialize the MongoDB server
     * @throws InterruptedException
     *             On a failure to start the MongoDB server.
     */
    @BeforeClass
    public static void startServer() throws IOException, InterruptedException {
        ourBuilder = new ProcessBuilder();
        ourBuilder.directory(SCRIPT_DIR);
        ourBuilder.command(
                new File(SCRIPT_DIR, "standalone.sh").getAbsolutePath(),
                "start");
        final Process start = ourBuilder.start();
        start.waitFor();
    }

    /**
     * Starts the MongoDB server for the test.
     * 
     * @throws IOException
     *             On a failure to initialize the MongoDB server
     * @throws InterruptedException
     *             On a failure to start the MongoDB server.
     */
    @AfterClass
    public static void stopServer() throws IOException, InterruptedException {
        Process stop = null;
        try {
            ourBuilder.command(
                    new File(SCRIPT_DIR, "standalone.sh").getAbsolutePath(),
                    "stop");
            stop = ourBuilder.start();
            stop.waitFor();
        }
        catch (final IOException ioe) {
            // Ignore - best effort.
        }
        catch (final InterruptedException e) {
            // Ignore - best effort.
        }
        ourBuilder = null;
    }

    /** The asynchronous collection instance. */
    private MongoCollection myAsyncCollection = null;

    /** The asynchronous database instance. */
    private MongoDatabase myAsyncDb = null;

    /** The asynchronous mongo instance. */
    private Mongo myAsyncMongo = null;

    /** The synchronous collection instance. */
    private com.mongodb.DBCollection mySyncCollection = null;

    /** The synchronous database instance. */
    private com.mongodb.DB mySyncDb = null;

    /** The synchronous mongo instance. */
    private com.mongodb.Mongo mySyncMongo = null;

    /**
     * Cleans up the collections and databases from the test.
     */
    @After
    public void cleanup() {
        if (myAsyncDb != null) {
            myAsyncDb.drop();
        }
        if (mySyncDb != null) {
            mySyncDb.dropDatabase();
        }
    }

    /**
     * Creates a builder for executing background process scripts.
     */
    @Before
    public void setUp() {
        try {
            final MongoDbConfiguration config = new MongoDbConfiguration(
                    new InetSocketAddress("127.0.0.1", 27017));
            config.setMaxConnectionCount(1);
            config.setMaxPendingOperationsPerConnection(4096);

            myAsyncMongo = new MongoClient(config);
            myAsyncDb = myAsyncMongo.getDatabase("asyncTest");
            myAsyncCollection = myAsyncDb.getCollection("test");

            final MongoOptions options = new MongoOptions();
            options.connectionsPerHost = 1;

            mySyncMongo = new com.mongodb.Mongo("127.0.0.1:27017", options);
            mySyncDb = mySyncMongo.getDB("syncTest");
            mySyncCollection = mySyncDb.getCollection("test");
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
    }

    /**
     * Stops all of the background processes started.
     */
    @After
    public void tearDown() {
        myAsyncCollection = null;
        if (myAsyncDb != null) {
            myAsyncDb.drop();
            myAsyncDb = null;
        }
        if (myAsyncMongo != null) {
            try {
                myAsyncMongo.close();
                myAsyncMongo = null;
            }
            catch (final IOException e) {
                // Ignored.
            }
        }

        mySyncCollection = null;
        if (mySyncDb != null) {
            mySyncDb.dropDatabase();
            mySyncDb = null;
        }
        if (mySyncMongo != null) {
            mySyncMongo.close();
            mySyncMongo = null;
        }
    }

    /**
     * Test to measure the performance of performing a series of inserts into a
     * collection.
     */
    @Test
    @Ignore
    public void testInsertRate() {

        final int maxCount = 1000000;
        int count;
        double async = 0;
        double sync = 0;
        double callback = 0;
        double legacy = 0;
        Durability durability;

        final List<String> cases = new ArrayList<String>();
        cases.add("none");
        cases.add("ack");
        cases.add("normal");
        cases.add("journal");
        cases.add("fsync");

        final List<String> runCases = new ArrayList<String>();
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);

        final List<String> runOrder = new ArrayList<String>();
        runOrder.add("async");
        runOrder.add("sync");
        runOrder.add("callback");
        runOrder.add("legacy");

        cleanup();

        final String dataFormat = "%-20s | %8.5f | %8.5f | %8.5f | %8.5g \n";
        System.out.printf("%-20s | %-8s | %-8s | %-8s | %-8s \n",
                "Inserts (\u00B5s/insert)", "Legacy", "Sync", "Async",
                "Callback");

        Collections.shuffle(runCases);
        for (final String runCase : runCases) {
            if ("none".equals(runCase)) {
                count = maxCount;
                durability = Durability.NONE;

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncInsertRate(WriteConcern.NONE, count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncInsertRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncInsertRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncInsertRateUsingCallback(durability,
                                count);
                    }
                }
                System.out.printf(dataFormat, "NONE",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("normal".equals(runCase)) {
                count = maxCount;
                durability = Durability.NONE;

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncInsertRate(WriteConcern.NORMAL, count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncInsertRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncInsertRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncInsertRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "NORMAL/NONE",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("ack".equals(runCase)) {
                count = maxCount;
                durability = Durability.ACK;

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncInsertRate(WriteConcern.SAFE, count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncInsertRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncInsertRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncInsertRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "SAFE/ACK",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("journal".equals(runCase)) {
                count = (maxCount / 10);
                durability = Durability.journalDurable(1000);

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncInsertRate(WriteConcern.JOURNAL_SAFE,
                                count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncInsertRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncInsertRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncInsertRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "JOURNAL",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("fsync".equals(runCase)) {
                count = maxCount / 10;
                durability = Durability.journalDurable(1000);

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncInsertRate(WriteConcern.FSYNC_SAFE,
                                count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncInsertRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncInsertRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncInsertRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "FSYNC",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
        }
    }

    /**
     * Test to measure the performance of performing a series of pdateinserts
     * into a collection.
     */
    @Test
    @Ignore
    public void testUpdateRate() {

        final int maxCount = 1000000;
        int count;
        double async = 0;
        double sync = 0;
        double callback = 0;
        double legacy = 0;
        Durability durability;

        final List<String> cases = new ArrayList<String>();
        cases.add("none");
        cases.add("normal");
        cases.add("ack");
        cases.add("journal");
        cases.add("fsync");

        final List<String> runCases = new ArrayList<String>();
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);
        runCases.addAll(cases);

        final List<String> runOrder = new ArrayList<String>();
        runOrder.add("async");
        runOrder.add("sync");
        runOrder.add("callback");
        runOrder.add("legacy");

        cleanup();

        final String dataFormat = "%-20s | %8.5f | %8.5f | %8.5f | %8.5g \n";
        System.out.printf("%-20s | %-8s | %-8s | %-8s | %-8s \n",
                "Updates (\u00B5s/update)", "Legacy", "Sync", "Async",
                "Callback");

        Collections.shuffle(runCases);
        for (final String runCase : runCases) {
            if ("none".equals(runCase)) {
                count = maxCount;
                durability = Durability.NONE;

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncUpdateRate(WriteConcern.NONE, count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncUpdateRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncUpdateRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncUpdateRateUsingCallback(durability,
                                count);
                    }
                }
                System.out.printf(dataFormat, "NONE",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("normal".equals(runCase)) {
                count = maxCount;
                durability = Durability.NONE;

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncUpdateRate(WriteConcern.NORMAL, count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncUpdateRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncUpdateRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncUpdateRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "NORMAL/NONE",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("ack".equals(runCase)) {
                count = maxCount;
                durability = Durability.ACK;

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncUpdateRate(WriteConcern.SAFE, count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncUpdateRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncUpdateRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncUpdateRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "SAFE/ACK",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("journal".equals(runCase)) {
                count = (maxCount / 10);
                durability = Durability.journalDurable(1000);

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncUpdateRate(WriteConcern.JOURNAL_SAFE,
                                count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncUpdateRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncUpdateRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncUpdateRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "JOURNAL",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
            else if ("fsync".equals(runCase)) {
                count = maxCount / 10;
                durability = Durability.journalDurable(1000);

                Collections.shuffle(runOrder);
                for (final String toRun : runOrder) {
                    cleanup();
                    if ("legacy".equals(toRun)) {
                        legacy = runSyncUpdateRate(WriteConcern.FSYNC_SAFE,
                                count);
                    }
                    else if ("async".equals(toRun)) {
                        async = runAsyncUpdateRate(durability, count);
                    }
                    else if ("sync".equals(toRun)) {
                        sync = runAsyncUpdateRateUsingSync(durability, count);
                    }
                    else if ("callback".equals(toRun)) {
                        callback = runAsyncUpdateRateUsingCallback(durability,
                                count);
                    }
                }

                System.out.printf(dataFormat, "FSYNC",
                        Double.valueOf(legacy * 1000.0),
                        Double.valueOf(sync * 1000.0),
                        Double.valueOf(async * 1000.0),
                        Double.valueOf(callback * 1000.0));
            }
        }
    }

    /**
     * Test to measure the performance of performing a series of inserts into a
     * collection.
     * 
     * @param durability
     *            The durability of the insert requests.
     * @param count
     *            The number of inserts.
     * @return The rate (in ms/insert) of inserts
     */
    protected double runAsyncInsertRate(final Durability durability,
            final int count) {
        try {
            final Future<?>[] results = new Future<?>[count];
            final long startTime = System.nanoTime();
            for (int i = 0; i < count; ++i) {
                final DocumentBuilder builder = BuilderFactory.start();
                builder.addInteger("_id", i);

                results[i] = myAsyncCollection.insertAsync(durability,
                        builder.get());
            }
            for (int i = 0; i < count; ++i) {
                results[i].get();
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MILLISECONDS.toNanos(1);
            return (delta / count);
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test to measure the performance of performing a series of inserts into a
     * collection.
     * 
     * @param durability
     *            The durability of the insert requests.
     * @param count
     *            The number of inserts.
     * @return The rate (in ms/insert) of inserts
     */
    protected double runAsyncInsertRateUsingCallback(
            final Durability durability, final int count) {
        final Callback<Integer> noop = new NoopCallback<Integer>();
        final long startTime = System.nanoTime();
        for (int i = 0; i < count; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myAsyncCollection.insertAsync(noop, durability, builder.get());
        }

        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MILLISECONDS.toNanos(1);
        return (delta / count);
    }

    /**
     * Test to measure the performance of performing a series of inserts into a
     * collection.
     * 
     * @param durability
     *            The durability of the insert requests.
     * @param count
     *            The number of inserts.
     * @return The rate (in ms/insert) of inserts
     */
    protected double runAsyncInsertRateUsingSync(final Durability durability,
            final int count) {
        final long startTime = System.nanoTime();
        for (int i = 0; i < count; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myAsyncCollection.insert(durability, builder.get());
        }

        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MILLISECONDS.toNanos(1);
        return (delta / count);
    }

    /**
     * Test to measure the performance of performing a series of updates to a a
     * single document in the collection.
     * 
     * @param durability
     *            The durability of the update requests.
     * @param count
     *            The number of updates.
     * @return The rate (in ms/update) of updates
     */
    protected double runAsyncUpdateRate(final Durability durability,
            final int count) {
        try {
            final Future<?>[] results = new Future<?>[count];

            final ObjectId id = new ObjectId();
            DocumentBuilder builder = BuilderFactory.start();
            builder.addObjectId("_id", id);
            builder.addLong("c", 0);
            myAsyncCollection.insert(durability, builder.get());

            builder = BuilderFactory.start();
            builder.addObjectId("_id", id);
            final Document query = builder.get();

            builder = BuilderFactory.start();
            builder.push("$inc").addLong("c", 1);
            final Document update = builder.get();

            final long startTime = System.nanoTime();
            for (int i = 0; i < count; ++i) {
                results[i] = myAsyncCollection.updateAsync(query, update,
                        durability);
            }
            for (int i = 0; i < count; ++i) {
                results[i].get();
            }
            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MILLISECONDS.toNanos(1);
            return (delta / count);
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
        catch (final ExecutionException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test to measure the performance of performing a series of updates to a a
     * single document in the collection.
     * 
     * @param durability
     *            The durability of the update requests.
     * @param count
     *            The number of updates.
     * @return The rate (in ms/update) of updates
     */
    protected double runAsyncUpdateRateUsingCallback(
            final Durability durability, final int count) {
        final Callback<Long> callback = new NoopCallback<Long>();

        final ObjectId id = new ObjectId();
        DocumentBuilder builder = BuilderFactory.start();
        builder.addObjectId("_id", id);
        builder.addLong("c", 0);
        myAsyncCollection.insert(durability, builder.get());

        builder = BuilderFactory.start();
        builder.addObjectId("_id", id);
        final Document query = builder.get();

        builder = BuilderFactory.start();
        builder.push("$inc").addLong("c", 1);
        final Document update = builder.get();

        final long startTime = System.nanoTime();
        for (int i = 0; i < count; ++i) {
            myAsyncCollection.updateAsync(callback, query, update, durability);
        }
        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MILLISECONDS.toNanos(1);
        return (delta / count);
    }

    /**
     * Test to measure the performance of performing a series of updates to a a
     * single document in the collection.
     * 
     * @param durability
     *            The durability of the update requests.
     * @param count
     *            The number of updates.
     * @return The rate (in ms/update) of updates
     */
    protected double runAsyncUpdateRateUsingSync(final Durability durability,
            final int count) {
        final ObjectId id = new ObjectId();
        DocumentBuilder builder = BuilderFactory.start();
        builder.addObjectId("_id", id);
        builder.addLong("c", 0);
        myAsyncCollection.insert(durability, builder.get());

        builder = BuilderFactory.start();
        builder.addObjectId("_id", id);
        final Document query = builder.get();

        builder = BuilderFactory.start();
        builder.push("$inc").addLong("c", 1);
        final Document update = builder.get();

        final long startTime = System.nanoTime();
        for (int i = 0; i < count; ++i) {
            myAsyncCollection.update(query, update, durability);
        }
        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MILLISECONDS.toNanos(1);
        return (delta / count);
    }

    /**
     * Test to measure the performance of performing a series of inserts into a
     * collection.
     * 
     * @param writeConcern
     *            The writeConcern of the insert requests.
     * @param count
     *            The number of inserts.
     * @return The rate (in ms/insert) of inserts
     */
    protected double runSyncInsertRate(final WriteConcern writeConcern,
            final int count) {
        try {
            final long startTime = System.nanoTime();
            for (int i = 0; i < count; ++i) {
                final DBObject obj = new BasicDBObject("_id",
                        Integer.valueOf(i));

                mySyncCollection.insert(obj, writeConcern);
            }
            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MILLISECONDS.toNanos(1);
            return (delta / count);
        }
        catch (final MongoException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Test to measure the performance of performing a series of updates to a
     * single document in the collection.
     * 
     * @param writeConcern
     *            The writeConcern of the update requests.
     * @param count
     *            The number of inserts.
     * @return The rate (in ms/update) of updates.
     */
    protected double runSyncUpdateRate(final WriteConcern writeConcern,
            final int count) {
        try {
            final org.bson.types.ObjectId id = new org.bson.types.ObjectId();
            final BasicDBObject obj = new BasicDBObject("_id", id);
            obj.append("c", Long.valueOf(0));
            mySyncCollection.insert(writeConcern, obj);

            final long startTime = System.nanoTime();
            for (int i = 0; i < count; ++i) {
                final BasicDBObject query = new BasicDBObject("_id", id);

                final BasicDBObject update = new BasicDBObject("$inc",
                        new BasicDBObject("c", Long.valueOf(1)));

                mySyncCollection.update(query, update, false, false,
                        writeConcern);
            }
            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MILLISECONDS.toNanos(1);
            return (delta / count);
        }
        catch (final MongoException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Callback that does nothing.
     * 
     * @param <T>
     *            The type of the ignored callback.
     * 
     * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
     */
    public class NoopCallback<T> implements Callback<T> {

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to do nothing.
         * </p>
         */
        @Override
        public void callback(final T result) {
            // Nothing.
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to do nothing.
         * </p>
         */
        @Override
        public void exception(final Throwable thrown) {
            // Nothing.
        }
    }
}
