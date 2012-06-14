/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.performance;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.BsonReader;
import com.allanbank.mongodb.bson.io.BsonWriter;

/**
 * BsonPerformanceITest provides performance tests for BSON reads and writes.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BsonPerformanceITest {

    /** The format for the data in the report. */
    public static final String DATA_FORMAT = "%40s | %8.5f | %8.5f | %8.5f\n";

    /** The number of iterations in a loop. */
    public static final int ITERATIONS = 1000000;

    /** The format for the label in the report. */
    public static final String LABEL_FORMAT = "%40s | %-8s | %-8s | %-8s\n";

    /** Number of levels in the binary tree for the large document. */
    public static final int LARGE_DOC_LEVELS = 18;

    /** The text value for a small document. */
    public static final String SMALL_VALUE = "Now is the time for all good men to come "
            + "to the aid of their country.";

    /** The bytes for a medium document. */
    private static final byte[] ourMediumDocBytes;

    /** The bytes for a micro document. */
    private static final byte[] ourMicroDocBytes;

    /** The bytes for a small document. */
    private static final byte[] ourSmallDocBytes;

    static {

        final BasicBSONEncoder encoder = new BasicBSONEncoder();

        // Micro
        BasicBSONObject obj = new BasicBSONObject("_id",
                Integer.valueOf((int) Double.doubleToLongBits(Math.random())));
        ourMicroDocBytes = encoder.encode(obj);

        // Small
        obj = new BasicBSONObject("_id", Integer.valueOf((int) Double
                .doubleToLongBits(Math.random())));
        obj.put("v", SMALL_VALUE);
        ourSmallDocBytes = encoder.encode(obj);

        // Medium
        final List<String> words = new ArrayList<String>();
        for (int j = 0; j < 20; ++j) {
            words.add("10gen");
            words.add("web");
            words.add("open");
            words.add("source");
            words.add("application");
            words.add("paas");
            words.add("platforma-as-a-service");
            words.add("technology");
            words.add("helps");
            words.add("developers");
            words.add("focus");
            words.add("building");
            words.add("mongodb");
            words.add("mongo");
        }
        final BasicBSONObject meta = new BasicBSONObject();
        meta.append("description", "i am a long description string");
        meta.append("author", "Holly Man");
        meta.append("dynamically_create_meta_tag", "who know\n what");

        final BasicBSONObject struct = new BasicBSONObject();
        struct.append("counted_tags", Integer.valueOf(3450));
        struct.append("no_of_js_attached", Integer.valueOf(10));
        struct.append("no_of_images", Integer.valueOf(6));

        obj = new BasicBSONObject("_id", new org.bson.types.ObjectId());
        obj.append("base_url", "http://www.example.com/test-me");
        obj.append("total_world_count", Integer.valueOf(6743));
        obj.append("access_time", new Date()); // current time
        obj.append("meta_tags", meta);
        obj.append("page_structure", struct);
        obj.append("harvested_words", words);
        ourMediumDocBytes = encoder.encode(obj);

        // Large - Done in test for each level.
    }

    /**
     * Prints a header line.
     */
    @BeforeClass
    public static void setUpClass() {
        System.out.printf(LABEL_FORMAT, "OP (\u00B5s/op)", "Legacy", "BStream",
                "BWriter");
    }

    /** The source of random value for the documents. */
    protected Random myRandom;

    /**
     * Initializes common data.
     */
    @Before
    public void setUp() {
        myRandom = new Random(System.currentTimeMillis());
    }

    /**
     * Clears the test state.
     */
    @After
    public void tearDown() {
        myRandom = null;
    }

    /**
     * Measures the relative performance for reading and writing a large
     * document of the format below. The integer values are randomly generated.
     * <blockquote> <code><pre>
     * {
     *    _id : &lt;integer&gt;,
     *    left_1 : {
     *       left_2: {
     *          ... to N levels of nesting.
     *          v : &lt;integer&gt;
     *       }
     *       right_2: {
     *          ... to N levels of nesting.
     *          v : &lt;integer&gt;
     *       }
     *    }
     *    right_1 : {
     *       left_2: {
     *          ... to N levels of nesting.
     *          v : &lt;integer&gt;
     *       }
     *       right_2: {
     *          ... to N levels of nesting.
     *          v : &lt;integer&gt;
     *       }
     *    }
     * }
     * </pre></code></blockquote>
     */
    @Test
    public void testLargeDocumentReadWritePerformance() {

        final BasicBSONEncoder encoder = new BasicBSONEncoder();

        for (int level = 1; level <= LARGE_DOC_LEVELS; level += 3) {

            final BasicBSONObject obj = new BasicBSONObject("_id",
                    Integer.valueOf(myRandom.nextInt()));
            addLegacyLevel(obj, 1, level);
            final byte[] docBytes = encoder.encode(obj);

            String label = "Read BSON " + level + " Level Tree ("
                    + docBytes.length + " Bytes)";

            double legacy = doLegacyRead(docBytes.clone(), (level << 1));
            double bstream = doBStreamRead(docBytes.clone(), (level << 1));
            double bwrite = doBReaderRead(docBytes.clone(), (level << 1));

            System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                    Double.valueOf(bstream), Double.valueOf(bwrite));

            // Now write it.
            label = "Write BSON " + level + " Level Tree";

            legacy = doLegacyLargeDocWrite(level);
            bstream = doBStreamLargeDocWrite(level);
            bwrite = doBWriteLargeDocWrite(level);

            System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                    Double.valueOf(bstream), Double.valueOf(bwrite));
        }
    }

    /**
     * Measures the relative performance for reading a medium document of the
     * format specified in the {@link #testMediumDocumentWritePerformance()}
     * method.
     * 
     * @see #testMediumDocumentWritePerformance()
     */
    @Test
    public void testMediumDocumentReadPerformance() {

        final String label = "Read Medium BSON Document ("
                + ourMediumDocBytes.length + " Bytes):";

        final byte[] docBytes = ourMediumDocBytes;
        final double legacy = doLegacyRead(docBytes.clone());
        final double bstream = doBStreamRead(docBytes.clone());
        final double bwrite = doBReaderRead(docBytes.clone());

        System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                Double.valueOf(bstream), Double.valueOf(bwrite));
    }

    /**
     * Measures the relative performance for writing a medium document of the
     * format below. <blockquote> <code><pre>
     * {
     *    _id : ObjectId(),
     *    base_url: "http://www.example.com/test-me",
     *    total_world_count: 6743,
     *    access_time: &lt;now&gt;, // current time 
     *    meta_tags : { 
     *       description : "i am a long description string",
     *       author : "Holly Man",
     *       dynamically_create_meta_tag : "who know\n what"
     *    },
     *    page_structure : { 
     *       counted_tags : 3450,
     *       no_of_js_attached : 10,
     *       no_of_images : 6
     *    },
     *    harvested_words : [
     *       "10gen", "web", "open", "source", "application", "paas",
     *       "platforma-as-a-service", "technology", "helps", 
     *       "developers", "focus", "building", "mongodb", "mongo",
     *       // ... repeat above words 19 more times for a total of 280 entries in the array.
     *    ]
     * }
     * </pre></code></blockquote>
     * <p>
     * Adapted from: <a
     * href="http://www.mongodb.org/pages/viewpage.action?pageId=2753117"
     * >Performance Testing (Wiki Archive)</a>.
     * </p>
     */
    @Test
    public void testMediumDocumentWritePerformance() {

        final String label = "Write Medium BSON Document:";

        final double legacy = doLegacyMediumDocWrite();
        final double bstream = doBStreamMediumDocWrite();
        final double bwrite = doBWriteMediumDocWrite();

        System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                Double.valueOf(bstream), Double.valueOf(bwrite));
    }

    /**
     * Measures the relative performance for reading a microscopic document of
     * the format specified in the {@link #testMicroDocumentWritePerformance()}
     * method.
     * 
     * @see #testMicroDocumentWritePerformance()
     */
    @Test
    public void testMicroDocumentReadPerformance() {

        final String label = "Read Microscopic BSON Document ("
                + ourMicroDocBytes.length + " Bytes):";

        final byte[] docBytes = ourMicroDocBytes;
        final double legacy = doLegacyRead(docBytes.clone());
        final double bstream = doBStreamRead(docBytes.clone());
        final double bwrite = doBReaderRead(docBytes.clone());

        System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                Double.valueOf(bstream), Double.valueOf(bwrite));
    }

    /**
     * Measures the relative performance for writing a microscopic document of
     * the format below. The integer value is randomly generated. <blockquote>
     * <code><pre>
     * {
     *    _id : &lt;int_value&gt;
     * }
     * </pre></code></blockquote>
     */
    @Test
    public void testMicroDocumentWritePerformance() {

        final String label = "Write Microscopic BSON Document:";

        final double legacy = doLegacyMicroDocWrite();
        final double bstream = doBStreamMicroDocWrite();
        final double bwrite = doBWriteMicroDocWrite();

        System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                Double.valueOf(bstream), Double.valueOf(bwrite));
    }

    /**
     * Measures the relative performance for reading a small document of the
     * format specified in the {@link #testSmallDocumentWritePerformance()}
     * method.
     * 
     * @see #testSmallDocumentWritePerformance()
     */
    @Test
    public void testSmallDocumentReadPerformance() {

        final String label = "Read Small BSON Document ("
                + ourSmallDocBytes.length + " Bytes):";

        final byte[] docBytes = ourSmallDocBytes;
        final double legacy = doLegacyRead(docBytes.clone());
        final double bstream = doBStreamRead(docBytes.clone());
        final double bwrite = doBReaderRead(docBytes.clone());

        System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                Double.valueOf(bstream), Double.valueOf(bwrite));
    }

    /**
     * Measures the relative performance for writing a small document of the
     * format below. The integer value is randomly generated. <blockquote>
     * <code><pre>
     * {
     *    _id : &lt;int_value&gt;,
     *    v : "Now is the time for all good men to come to the aid of their country."
     * }
     * </pre></code></blockquote>
     */
    @Test
    public void testSmallDocumentWritePerformance() {

        final String label = "Write Small BSON Document:";

        final double legacy = doLegacySmallDocWrite();
        final double bstream = doBStreamSmallDocWrite();
        final double bwrite = doBWriteSmallDocWrite();

        System.out.printf(DATA_FORMAT, label, Double.valueOf(legacy),
                Double.valueOf(bstream), Double.valueOf(bwrite));
    }

    /**
     * Reads documents via a {@link BsonInputStream}.
     * 
     * @param bytes
     *            The bytes for the document to read.
     * @return The time to read each document in microseconds.
     */
    protected double doBReaderRead(final byte[] bytes) {
        return doBReaderRead(bytes, 1);
    }

    /**
     * Reads documents via a {@link BsonInputStream}.
     * 
     * @param bytes
     *            The bytes for the document to read.
     * @param divisor
     *            The divisor for the number of {@link #ITERATIONS}.
     * @return The time to read each document in microseconds.
     */
    protected double doBReaderRead(final byte[] bytes, final int divisor) {

        final ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        final BsonReader bin = new BsonReader(in);
        try {
            final int iterations = ITERATIONS / divisor;
            final long startTime = System.nanoTime();
            for (int i = 0; i < iterations; ++i) {
                in.reset();

                bin.readDocument();
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / iterations);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
        finally {
            close(bin);
        }
    }

    /**
     * Writes large documents to a {@link BsonOutputStream}.
     * 
     * @param levels
     *            The number of levels to create in the tree.
     * @return The time to write each document in microseconds.
     * @see #testLargeDocumentReadWritePerformance()
     */
    protected double doBStreamLargeDocWrite(final int levels) {

        final DocumentBuilder builder = BuilderFactory.start();
        final BsonOutputStream bout = new BsonOutputStream(
                new DevNullOutputStream());
        try {
            final int iterations = ITERATIONS / (levels << 1);

            final long startTime = System.nanoTime();
            for (int i = 0; i < iterations; ++i) {
                builder.reset();
                builder.addInteger("_id", myRandom.nextInt());

                addLevel(builder, 1, levels);

                bout.writeDocument(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / iterations);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
    }

    /**
     * Writes medium documents to a {@link BsonOutputStream}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testMediumDocumentWritePerformance()
     */
    protected double doBStreamMediumDocWrite() {

        final DocumentBuilder builder = BuilderFactory.start();
        final BsonOutputStream bout = new BsonOutputStream(
                new DevNullOutputStream());
        try {
            final long startTime = System.nanoTime();
            for (int i = 0; i < ITERATIONS; ++i) {
                builder.reset();
                builder.addObjectId("_id",
                        new com.allanbank.mongodb.bson.element.ObjectId());
                builder.addString("base_url", "http://www.example.com/test-me");
                builder.addInteger("total_world_count", 6743);
                builder.addTimestamp("access_time", System.currentTimeMillis()); // current
                                                                                 // time

                DocumentBuilder subBuilder = builder.push("meta_tags");
                subBuilder.addString("description",
                        "i am a long description string");
                subBuilder.addString("author", "Holly Man");
                subBuilder.addString("dynamically_create_meta_tag",
                        "who know\n what");

                subBuilder = builder.push("page_structure");
                subBuilder.addInteger("counted_tags", 3450);
                subBuilder.addInteger("no_of_js_attached", 10);
                subBuilder.addInteger("no_of_images", 6);

                final ArrayBuilder aBuilder = builder
                        .pushArray("harvested_words");
                for (int j = 0; j < 20; ++j) {
                    aBuilder.addString("10gen");
                    aBuilder.addString("web");
                    aBuilder.addString("open");
                    aBuilder.addString("source");
                    aBuilder.addString("application");
                    aBuilder.addString("paas");
                    aBuilder.addString("platforma-as-a-service");
                    aBuilder.addString("technology");
                    aBuilder.addString("helps");
                    aBuilder.addString("developers");
                    aBuilder.addString("focus");
                    aBuilder.addString("building");
                    aBuilder.addString("mongodb");
                    aBuilder.addString("mongo");
                }

                bout.writeDocument(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / ITERATIONS);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
    }

    /**
     * Writes micro documents to a {@link BsonOutputStream}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testMicroDocumentWritePerformance()
     */
    protected double doBStreamMicroDocWrite() {

        final DocumentBuilder builder = BuilderFactory.start();
        final BsonOutputStream bout = new BsonOutputStream(
                new DevNullOutputStream());
        try {
            final long startTime = System.nanoTime();
            for (int i = 0; i < ITERATIONS; ++i) {
                builder.reset();
                builder.addInteger("_id", myRandom.nextInt());

                bout.writeDocument(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / ITERATIONS);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
    }

    /**
     * Reads documents via a {@link BsonInputStream}.
     * 
     * @param bytes
     *            The bytes for the document to read.
     * @return The time to read each document in microseconds.
     */
    protected double doBStreamRead(final byte[] bytes) {
        return doBStreamRead(bytes, 1);
    }

    /**
     * Reads documents via a {@link BsonInputStream}.
     * 
     * @param bytes
     *            The bytes for the document to read.
     * @param divisor
     *            The divisor for the number of {@link #ITERATIONS}.
     * @return The time to read each document in microseconds.
     */
    protected double doBStreamRead(final byte[] bytes, final int divisor) {

        final ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        final BsonInputStream bin = new BsonInputStream(in);
        try {
            final int iterations = ITERATIONS / divisor;
            final long startTime = System.nanoTime();
            for (int i = 0; i < iterations; ++i) {
                in.reset();

                bin.readDocument();
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / iterations);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
        finally {
            close(bin);
        }
    }

    /**
     * Writes small documents to a {@link BsonOutputStream}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testSmallDocumentWritePerformance()
     */
    protected double doBStreamSmallDocWrite() {

        final DocumentBuilder builder = BuilderFactory.start();
        final BsonOutputStream bout = new BsonOutputStream(
                new DevNullOutputStream());
        try {
            final long startTime = System.nanoTime();
            for (int i = 0; i < ITERATIONS; ++i) {
                builder.reset();
                builder.addInteger("_id", myRandom.nextInt());
                builder.addString("v", SMALL_VALUE);

                bout.writeDocument(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / ITERATIONS);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
    }

    /**
     * Writes large documents to a {@link BsonWriter}.
     * 
     * @param levels
     *            The number of levels to create in the tree.
     * @return The time to write each document in microseconds.
     * @see #testLargeDocumentReadWritePerformance()
     */
    protected double doBWriteLargeDocWrite(final int levels) {
        final DocumentBuilder builder = BuilderFactory.start();
        final BsonWriter bwriter = new BsonWriter(new DevNullOutputStream());
        try {
            final int iterations = ITERATIONS / (levels << 1);

            final long startTime = System.nanoTime();
            for (int i = 0; i < iterations; ++i) {
                builder.reset();
                builder.addInteger("_id", myRandom.nextInt());

                addLevel(builder, 1, levels);

                bwriter.write(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / iterations);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
        finally {
            close(bwriter);
        }
    }

    /**
     * Writes medium documents to a {@link BsonWriter}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testMediumDocumentWritePerformance()
     */
    protected double doBWriteMediumDocWrite() {
        final DocumentBuilder builder = BuilderFactory.start();
        final BsonWriter bwriter = new BsonWriter(new DevNullOutputStream());
        try {
            final long startTime = System.nanoTime();

            for (int i = 0; i < ITERATIONS; ++i) {
                builder.reset();
                builder.addObjectId("_id",
                        new com.allanbank.mongodb.bson.element.ObjectId());
                builder.addString("base_url", "http://www.example.com/test-me");
                builder.addInteger("total_world_count", 6743);
                builder.addTimestamp("access_time", System.currentTimeMillis()); // current
                                                                                 // time

                DocumentBuilder subBuilder = builder.push("meta_tags");
                subBuilder.addString("description",
                        "i am a long description string");
                subBuilder.addString("author", "Holly Man");
                subBuilder.addString("dynamically_create_meta_tag",
                        "who know\n what");

                subBuilder = builder.push("page_structure");
                subBuilder.addInteger("counted_tags", 3450);
                subBuilder.addInteger("no_of_js_attached", 10);
                subBuilder.addInteger("no_of_images", 6);

                final ArrayBuilder aBuilder = builder
                        .pushArray("harvested_words");
                for (int j = 0; j < 20; ++j) {
                    aBuilder.addString("10gen");
                    aBuilder.addString("web");
                    aBuilder.addString("open");
                    aBuilder.addString("source");
                    aBuilder.addString("application");
                    aBuilder.addString("paas");
                    aBuilder.addString("platforma-as-a-service");
                    aBuilder.addString("technology");
                    aBuilder.addString("helps");
                    aBuilder.addString("developers");
                    aBuilder.addString("focus");
                    aBuilder.addString("building");
                    aBuilder.addString("mongodb");
                    aBuilder.addString("mongo");
                }

                bwriter.write(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / ITERATIONS);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
        finally {
            close(bwriter);
        }
    }

    /**
     * Writes micro documents to a {@link BsonWriter}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testMicroDocumentWritePerformance()
     */
    protected double doBWriteMicroDocWrite() {
        final DocumentBuilder builder = BuilderFactory.start();
        final BsonWriter bwriter = new BsonWriter(new DevNullOutputStream());
        try {
            final long startTime = System.nanoTime();

            for (int i = 0; i < ITERATIONS; ++i) {
                builder.reset();
                builder.addInteger("_id", myRandom.nextInt());

                bwriter.write(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / ITERATIONS);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
        finally {
            close(bwriter);
        }
    }

    /**
     * Writes small documents to a {@link BsonWriter}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testSmallDocumentWritePerformance()
     */
    protected double doBWriteSmallDocWrite() {
        final DocumentBuilder builder = BuilderFactory.start();
        final BsonWriter bwriter = new BsonWriter(new DevNullOutputStream());
        try {
            final long startTime = System.nanoTime();

            for (int i = 0; i < ITERATIONS; ++i) {
                builder.reset();
                builder.addInteger("_id", myRandom.nextInt());
                builder.addString("v", SMALL_VALUE);

                bwriter.write(builder.get());
            }

            final long endTime = System.nanoTime();
            final double delta = ((double) (endTime - startTime))
                    / TimeUnit.MICROSECONDS.toNanos(1);
            return (delta / ITERATIONS);
        }
        catch (final IOException error) {
            fail(error.getMessage());
            return -1;
        }
        finally {
            close(bwriter);
        }
    }

    /**
     * Writes large documents to a {@link BasicBSONEncoder}.
     * 
     * @param levels
     *            The number of levels to create in the tree.
     * @return The time to write each document in microseconds.
     * @see #testLargeDocumentReadWritePerformance()
     */
    protected double doLegacyLargeDocWrite(final int levels) {
        final BasicBSONEncoder encoder = new BasicBSONEncoder();

        final int iterations = ITERATIONS / (levels << 1);

        final long startTime = System.nanoTime();
        for (int i = 0; i < iterations; ++i) {

            final BasicBSONObject obj = new BasicBSONObject("_id",
                    Integer.valueOf(myRandom.nextInt()));

            addLegacyLevel(obj, 1, levels);

            encoder.encode(obj);
        }

        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MICROSECONDS.toNanos(1);
        return (delta / iterations);
    }

    /**
     * Writes medium documents to a {@link BasicBSONEncoder}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testMediumDocumentWritePerformance()
     */
    protected double doLegacyMediumDocWrite() {
        final BasicBSONEncoder encoder = new BasicBSONEncoder();

        final long startTime = System.nanoTime();
        for (int i = 0; i < ITERATIONS; ++i) {

            final List<String> words = new ArrayList<String>();
            for (int j = 0; j < 20; ++j) {
                words.add("10gen");
                words.add("web");
                words.add("open");
                words.add("source");
                words.add("application");
                words.add("paas");
                words.add("platforma-as-a-service");
                words.add("technology");
                words.add("helps");
                words.add("developers");
                words.add("focus");
                words.add("building");
                words.add("mongodb");
                words.add("mongo");
            }
            final BasicBSONObject meta = new BasicBSONObject();
            meta.append("description", "i am a long description string");
            meta.append("author", "Holly Man");
            meta.append("dynamically_create_meta_tag", "who know\n what");

            final BasicBSONObject struct = new BasicBSONObject();
            struct.append("counted_tags", Integer.valueOf(3450));
            struct.append("no_of_js_attached", Integer.valueOf(10));
            struct.append("no_of_images", Integer.valueOf(6));

            final BasicBSONObject obj = new BasicBSONObject("_id",
                    new org.bson.types.ObjectId());

            obj.append("base_url", "http://www.example.com/test-me");
            obj.append("total_world_count", Integer.valueOf(6743));
            obj.append("access_time", new Date()); // current time
            obj.append("meta_tags", meta);
            obj.append("page_structure", struct);
            obj.append("harvested_words", words);

            encoder.encode(obj);
        }

        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MICROSECONDS.toNanos(1);
        return (delta / ITERATIONS);
    }

    /**
     * Writes micro documents to a {@link BasicBSONEncoder}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testMicroDocumentWritePerformance()
     */
    protected double doLegacyMicroDocWrite() {
        final BasicBSONEncoder encoder = new BasicBSONEncoder();

        final long startTime = System.nanoTime();
        for (int i = 0; i < ITERATIONS; ++i) {
            final BasicBSONObject obj = new BasicBSONObject("_id",
                    Integer.valueOf(myRandom.nextInt()));

            encoder.encode(obj);
        }

        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MICROSECONDS.toNanos(1);
        return (delta / ITERATIONS);
    }

    /**
     * Reads documents via a {@link BasicBSONDecoder}.
     * 
     * @param bytes
     *            The bytes of the document to be read.
     * @return The time to read each document in microseconds.
     * @see #testLargeDocumentReadWritePerformance()
     */
    protected double doLegacyRead(final byte[] bytes) {
        return doLegacyRead(bytes, 1);
    }

    /**
     * Reads documents via a {@link BasicBSONDecoder}.
     * 
     * @param bytes
     *            The bytes of the document to be read.
     * @param divisor
     *            The divisor for the number of {@link #ITERATIONS}.
     * @return The time to read each document in microseconds.
     * @see #testLargeDocumentReadWritePerformance()
     */
    protected double doLegacyRead(final byte[] bytes, final int divisor) {
        final BasicBSONDecoder decoder = new BasicBSONDecoder();

        final int iterations = ITERATIONS / divisor;
        final long startTime = System.nanoTime();
        for (int i = 0; i < iterations; ++i) {
            decoder.readObject(bytes);
        }

        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MICROSECONDS.toNanos(1);
        return (delta / iterations);
    }

    /**
     * Writes small documents to a {@link BasicBSONEncoder}.
     * 
     * @return The time to write each document in microseconds.
     * @see #testSmallDocumentWritePerformance()
     */
    protected double doLegacySmallDocWrite() {
        final BasicBSONEncoder encoder = new BasicBSONEncoder();

        final long startTime = System.nanoTime();
        for (int i = 0; i < ITERATIONS; ++i) {
            final BasicBSONObject obj = new BasicBSONObject("_id",
                    Integer.valueOf(myRandom.nextInt()));
            obj.put("v", SMALL_VALUE);

            encoder.encode(obj);
        }

        final long endTime = System.nanoTime();
        final double delta = ((double) (endTime - startTime))
                / TimeUnit.MICROSECONDS.toNanos(1);
        return (delta / ITERATIONS);
    }

    /**
     * Adds a level to the legacy BSON document tree.
     * 
     * @param obj
     *            The object to att the level to.
     * @param current
     *            The current level.
     * @param maxLevel
     *            The maximum level.
     */
    private void addLegacyLevel(final BasicBSONObject obj, final int current,
            final int maxLevel) {
        if (maxLevel <= current) {
            obj.append("v", Integer.valueOf(myRandom.nextInt()));
        }
        else {
            BasicBSONObject subObj;

            subObj = new BasicBSONObject();
            addLegacyLevel(subObj, current + 1, maxLevel);
            obj.append("left_" + current, subObj);

            subObj = new BasicBSONObject();
            addLegacyLevel(subObj, current + 1, maxLevel);
            obj.append("right_" + current, subObj);
        }
    }

    /**
     * Adds a level to the legacy BSON document tree.
     * 
     * @param obj
     *            The object to att the level to.
     * @param current
     *            The current level.
     * @param maxLevel
     *            The maximum level.
     */
    private void addLevel(final DocumentBuilder obj, final int current,
            final int maxLevel) {
        if (maxLevel <= current) {
            obj.addInteger("v", myRandom.nextInt());
        }
        else {
            addLevel(obj.push("left_" + current), current + 1, maxLevel);
            addLevel(obj.push("right_" + current), current + 1, maxLevel);
        }
    }

    /**
     * Closes the {@link Closeable} and logs any error.
     * 
     * @param closeable
     *            The connection to close.
     */
    private void close(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (final IOException ignored) {
                // Ignore.
            }
        }
    }

    /**
     * NoopOutputStream provides an {@link OutputStream} implementation to
     * nowhere.
     * 
     * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static class DevNullOutputStream extends java.io.OutputStream {

        /**
         * Creates a new DevNullOutputStream.
         */
        public DevNullOutputStream() {
            super();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void flush() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void write(final byte b[]) {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void write(final byte b[], final int off, final int len) {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void write(final int b) {
        }
    }
}
