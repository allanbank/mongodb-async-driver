/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.builder.Find;
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
        myCollection.insert(BuilderFactory.start().build());

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
        myCollection.insert(BuilderFactory.start().build());

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

    /**
     * Test case for using tailable cursors.
     */
    @Test
    public void testTailableCursorBlocks() {
        final int batches = 5;
        final int countPerBatch = 10;
        int insert = 0;

        final DocumentBuilder builder = BuilderFactory.start();

        myConfig.setMaxConnectionCount(2);
        myDb.createCappedCollection("capped", 1000000);

        final MongoCollection collection = myDb.getCollection("capped");
        ClosableIterator<Document> iter = null;
        Thread backgroundReader = null;
        try {
            // Appear to need at least 1 document in the collection.
            collection.insert(Durability.ACK,
                    builder.reset().add("_id", insert++));

            final Find find = new Find.Builder().setQuery(builder.reset())
                    .tailable().build();
            iter = collection.find(find);

            backgroundReader = new Thread(new BackgroundTailableCursorReader(
                    iter, batches * countPerBatch), "testTailableCursorBlocks");
            backgroundReader.start();

            for (int b = 0; b < batches; ++b) {

                assertTrue("Background thread died prematurely",
                        backgroundReader.isAlive());

                Thread.sleep(10000);

                assertTrue("Background thread died prematurely",
                        backgroundReader.isAlive());

                // Insert N.
                for (int i = 0; i < countPerBatch; ++i) {
                    collection.insert(Durability.ACK,
                            builder.reset().add("_id", insert++));
                }
            }

            // Should finish soon.
            backgroundReader.join(2000);

            assertFalse("Background thread should have died.",
                    backgroundReader.isAlive());
        }
        catch (final InterruptedException e) {
            fail(e.getMessage());
        }
        finally {
            if (backgroundReader != null) {
                backgroundReader.interrupt();
            }
            if (iter != null) {
                iter.close();
            }
            collection.drop();
        }
    }

    /**
     * Test case for using tailable cursors.
     */
    @Test
    public void testTailableCursors() {
        int insert = 0;
        int read = 0;

        final DocumentBuilder builder = BuilderFactory.start();

        myConfig.setMaxConnectionCount(2);
        myDb.createCappedCollection("capped", 1000000);

        final MongoCollection collection = myDb.getCollection("capped");
        ClosableIterator<Document> iter = null;
        try {
            // Appear to need at least 1 document in the collection.
            collection.insert(Durability.ACK,
                    builder.reset().add("_id", insert++));

            final Find find = new Find.Builder().setQuery(builder.reset())
                    .tailable().build();
            iter = collection.find(find);

            for (int n = 0; n < 1000; ++n) {
                // Insert N.
                for (int i = 0; i < n; ++i) {
                    collection.insert(Durability.ACK,
                            builder.reset().add("_id", insert++));
                }

                // Read 7.
                for (int i = 0; i < n; ++i) {
                    assertTrue("n=" + n + ", i=" + i, iter.hasNext());
                    assertEquals(builder.reset().add("_id", read++).build(),
                            iter.next());
                }
            }
        }
        finally {
            if (iter != null) {
                iter.close();
            }
            collection.drop();
        }
    }

    /**
     * BackgroundTailableCursorReader provides a background runnable for reading
     * documents.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class BackgroundTailableCursorReader implements Runnable {

        /** The number of documents to read. */
        private final int myDocsToRead;

        /** The iterator to read from. */
        private final ClosableIterator<Document> myIterator;

        /** The thrown exception, if any. */
        private RuntimeException myThrown;

        /**
         * Creates a new BackgroundTailableCursorReader.
         * 
         * @param iterator
         *            The iterator to read from.
         * @param docsToRead
         *            The number of documents to read.
         */
        public BackgroundTailableCursorReader(
                final ClosableIterator<Document> iterator, final int docsToRead) {
            myIterator = iterator;
            myDocsToRead = docsToRead;
            myThrown = null;
        }

        /**
         * Returns the thrown exception, if any.
         * 
         * @return The thrown exception, if any.
         */
        public Throwable getThrown() {
            return myThrown;
        }

        /**
         * Reads over the documents on the background.
         */
        @Override
        public void run() {
            try {
                for (int i = 0; i < myDocsToRead; ++i) {
                    if (myIterator.hasNext()) {
                        myIterator.next();
                    }
                    else {
                        myThrown = new IllegalStateException(
                                "Did not read all of the expected messages.");
                        return;
                    }
                }
            }
            catch (final RuntimeException re) {
                myThrown = re;
            }
        }
    }
}
