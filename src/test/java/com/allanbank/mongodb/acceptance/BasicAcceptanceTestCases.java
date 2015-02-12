/*
 * #%L
 * BasicAcceptanceTestCases.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.acceptance;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.id;
import static com.allanbank.mongodb.builder.AggregationProjectFields.includeWithoutId;
import static com.allanbank.mongodb.builder.GeoJson.p;
import static com.allanbank.mongodb.builder.QueryBuilder.and;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.Sort.asc;
import static com.allanbank.mongodb.builder.Sort.desc;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.set;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.awt.geom.Point2D;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.BatchedAsyncMongoCollection;
import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ProfilingStatus;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.AggregationGeoNear;
import com.allanbank.mongodb.builder.AggregationProjectFields;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.builder.BatchedWriteMode;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.Count;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GeoJson;
import com.allanbank.mongodb.builder.GeospatialOperator;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.Index;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.builder.MiscellaneousOperator;
import com.allanbank.mongodb.builder.ParallelScan;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.allanbank.mongodb.builder.Sort;
import com.allanbank.mongodb.builder.write.InsertOperation;
import com.allanbank.mongodb.builder.write.WriteOperation;
import com.allanbank.mongodb.builder.write.WriteOperationType;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.error.BatchedWriteException;
import com.allanbank.mongodb.error.CursorNotFoundException;
import com.allanbank.mongodb.error.DocumentToLargeException;
import com.allanbank.mongodb.error.DuplicateKeyException;
import com.allanbank.mongodb.error.DurabilityException;
import com.allanbank.mongodb.error.MaximumTimeLimitExceededException;
import com.allanbank.mongodb.error.QueryFailedException;
import com.allanbank.mongodb.error.ReplyException;
import com.allanbank.mongodb.error.ServerVersionException;
import com.allanbank.mongodb.gridfs.GridFs;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * BasicAcceptanceTestCases provides the base tests for the interactions with
 * the MongoDB server.
 * <p>
 * These are not meant to be exhaustive tests of the driver but instead attempt
 * to demonstrate that interactions with the MongoDB processes work. These tests
 * are expected to verify that the format and structure of the messages the
 * driver generates are acceptable to the MongoDB servers.
 * </p>
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class BasicAcceptanceTestCases extends ServerTestDriverSupport {

    /** The name of the test collection to use. */
    public static final String GEO_TEST_COLLECTION_NAME = "geo";

    /** The name of the test Grid FS chunks collection to use. */
    public static final String GRIDFS_COLLECTION_ROOT_NAME = "gridfs_";

    /** One million - used when we want a large collection of document. */
    public static final int LARGE_COLLECTION_COUNT = 1000000;

    /** One hundred - used when we only need a small collection. */
    public static final int SMALL_COLLECTION_COUNT = 100;

    /** The name of the test collection to use. */
    public static final String TEST_COLLECTION_NAME = "acceptance";

    /** The name of the test database to use. */
    public static final String TEST_DB_NAME = "acceptance_test";

    /** A unique value to add to each collection name to ensure test isolation. */
    protected static int ourUniqueId = 0;

    /**
     * Creates a large collection of documents that test should only read from.
     */
    protected static void buildLargeCollection() {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer(createAddress());

        final MongoClient mongoClient = MongoFactory.createClient(config);
        try {
            final MongoCollection collection = largeCollection(mongoClient);

            // Use the Future delayed strategy.
            final BlockingQueue<Future<Integer>> sent = new ArrayBlockingQueue<Future<Integer>>(
                    5000);
            for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
                final DocumentBuilder builder = BuilderFactory.start();
                builder.addInteger("_id", i);

                final Future<Integer> result = collection.insertAsync(builder
                        .build());
                while (!sent.offer(result)) {
                    sent.take().get();
                }
            }
            for (final Future<Integer> result : sent) {
                result.get();
            }
        }
        catch (final InterruptedException e) {
            fail(e.getMessage());
        }
        catch (final ExecutionException e) {
            fail(e.getMessage());
        }
        finally {
            IOUtils.close(mongoClient);
        }
    }

    /**
     * Creates the address to connect to.
     * 
     * @return The {@link InetSocketAddress} for the MongoDB server.
     */
    protected static InetSocketAddress createAddress() {
        final String remote = System.getenv("MONGODB_HOST");
        if (remote != null) {
            return ServerNameUtils.parse(remote);
        }
        return new InetSocketAddress("127.0.0.1", DEFAULT_PORT);
    }

    /**
     * Creates a large collection of documents that test should only read from.
     */
    protected static void disableBalancer() {

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addServer(createAddress());

        final MongoClient mongoClient = MongoFactory.createClient(config);
        try {
            // Turn off the balancer - Can confuse the test counts.
            final boolean upsert = true;
            mongoClient
                    .getDatabase("config")
                    .getCollection("settings")
                    .update(where("_id").equals("balancer"),
                            d(e("$set", d(e("stopped", true)))), false, upsert);
        }
        finally {
            IOUtils.close(mongoClient);
        }
    }

    /**
     * Returns the large collection handle.
     * 
     * @param mongoClient
     *            The client to connect to the collection.
     * @return The handle to the large collection.
     */
    protected static MongoCollection largeCollection(
            final MongoClient mongoClient) {
        return mongoClient.getDatabase(TEST_DB_NAME + "_large").getCollection(
                TEST_COLLECTION_NAME);
    }

    /** The default collection for the test. */
    protected MongoCollection myCollection = null;

    /** The configuration for the test. */
    protected MongoClientConfiguration myConfig = null;

    /** The default database to use for the test. */
    protected MongoDatabase myDb = null;

    /** The Geospatial collection using a {@code 2d} index for the test. */
    protected MongoCollection myGeoCollection = null;

    /** The Geospatial collection using a {@code 2dsphere} index for the test. */
    protected MongoCollection myGeoSphereCollection = null;

    /** The connection to MongoDB for the test. */
    protected MongoClient myMongo = null;

    /** A source of random for the tests. */
    protected Random myRandom = null;

    /**
     * Sets up to create a connection to MongoDB.
     */
    @Before
    public void connect() {
        if (myConfig == null) {
            myConfig = new MongoClientConfiguration();
            myConfig.addServer(createAddress());
        }

        myMongo = MongoFactory.createClient(myConfig);
        myDb = myMongo.getDatabase(TEST_DB_NAME);
        myCollection = myDb.getCollection(TEST_COLLECTION_NAME + "_"
                + (++ourUniqueId));

        myRandom = new Random(System.currentTimeMillis());
    }

    /**
     * Disconnects from MongoDB.
     */
    @After
    public void disconnect() {
        try {
            if (myCollection != null) {
                myCollection.drop();
            }
            if (myGeoCollection != null) {
                myGeoCollection.drop();
            }
            if (myGeoSphereCollection != null) {
                myGeoSphereCollection.drop();
            }

            // Other collections.
            if (myDb != null) {
                for (final String name : myDb.listCollectionNames()) {
                    if (name.startsWith(GRIDFS_COLLECTION_ROOT_NAME)) {
                        myDb.getCollection(name).drop();
                    }
                }

                myDb.drop();
            }
            if (myMongo != null) {
                myMongo.close();
            }
        }
        catch (final IOException e) {
            // Ignore. Trying to cleanup.
        }
        catch (final MongoDbException e) {
            // Ignore. Trying to cleanup.
        }
        finally {
            myMongo = null;
            myDb = null;
            myCollection = null;
            myGeoCollection = null;
            myGeoSphereCollection = null;
            myConfig = null;
            myRandom = null;

        }
    }

    /**
     * Verifies the function of Aggregate framework.
     * <p>
     * Using the drivers support classes: <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.builder.AggregationGroupField#set com.allanbank.mongodb.builder.AggregateGroupField.set};
     * import static {@link com.allanbank.mongodb.builder.AggregationGroupId#id com.allanbank.mongodb.builder.AggregateGroupId.id};
     * import static {@link com.allanbank.mongodb.builder.AggregationProjectFields#includeWithoutId com.allanbank.mongodb.builder.AggregateProjectFields.includeWithoutId};
     * import static {@link com.allanbank.mongodb.builder.QueryBuilder#where com.allanbank.mongodb.builder.QueryBuilder.where};
     * import static {@link com.allanbank.mongodb.builder.Sort#asc com.allanbank.mongodb.builder.Sort.asc};
     * import static {@link com.allanbank.mongodb.builder.Sort#desc com.allanbank.mongodb.builder.Sort.desc};
     * import static {@link com.allanbank.mongodb.builder.expression.Expressions#constant com.allanbank.mongodb.builder.expression.Expressions.constant};
     * import static {@link com.allanbank.mongodb.builder.expression.Expressions#field com.allanbank.mongodb.builder.expression.Expressions.field};
     * import static {@link com.allanbank.mongodb.builder.expression.Expressions#set com.allanbank.mongodb.builder.expression.Expressions.set};
     * 
     * DocumentBuilder b1 = BuilderFactory.start();
     * DocumentBuilder b2 = BuilderFactory.start();
     * Aggregate.Builder builder = new Aggregate.Builder();
     * 
     * builder.match(where("state").notEqualTo("NZ"))
     *         .group(id().addField("state").addField("city"),
     *                 set("pop").sum("pop"))
     *         .sort(asc("pop"))
     *         .group(id("_id.state"), set("biggestcity").last("_id.city"),
     *                 set("biggestpop").last("pop"),
     *                 set("smallestcity").first("_id.city"),
     *                 set("smallestpop").first("pop"))
     *         .project(
     *                 includeWithoutId(),
     *                 set("state", field("_id")),
     *                 set("biggestCity",
     *                         b1.add(set("name", field("biggestcity"))).add(
     *                                 set("pop", field("biggestpop")))),
     *                 set("smallestCity",
     *                         b2.add(set("name", field("smallestcity"))).add(
     *                                 set("pop", field("smallestpop")))))
     *         .sort(desc("biggestCity.pop"));
     * </code>
     * </pre>
     * 
     * </blockquote>
     * </p>
     * <p>
     * Using the MongoDB Shell: <blockquote>
     * 
     * <pre>
     * <code>
     * > db.things.insert( { state : "NZ", city : "big", pop : 1000  } );
     * > db.things.insert( { state : "MD", city : "big", pop : 1000  } );
     * > db.things.insert( { state : "MD", city : "medium", pop : 10  } );
     * > db.things.insert( { state : "MD", city : "small", pop : 1  } );
     * > db.things.insert( { state : "CA", city : "big", pop : 10000  } );
     * > db.things.insert( { state : "CA", city : "small", pop : 11  } );
     * > db.things.insert( { state : "CA", city : "small", pop : 10  } );
     * > db.things.insert( { state : "NY", city : "big", pop : 100000  } );
     * > db.things.insert( { state : "NY", city : "small", pop : 20  } );
     * > db.things.insert( { state : "NY", city : "small", pop : 5  } );
     * > db.things.aggregate( [
     *              { $match : { state : { $ne : "NZ" } } },
     *              { $group :
     *                { _id : { state : "$state", city : "$city" },
     *                  pop : { $sum : "$pop" } } },
     *              { $sort : { pop : 1 } },
     *              { $group :
     *                { _id : "$_id.state",
     *                  biggestcity : { $last : "$_id.city" },
     *                  biggestpop : { $last : "$pop" },
     *                  smallestcity : { $first : "$_id.city" },
     *                  smallestpop : { $first : "$pop" } } },
     *              { $project :
     *                { _id : 0,
     *                  state : "$_id",
     *                  biggestCity : { name : "$biggestcity", pop: "$biggestpop" },
     *                  smallestCity : { name : "$smallestcity", pop : "$smallestpop" } } }
     *              { $sort : { "biggestCity.pop : -1 } }
     *            ] );
     * {
     *     "result" : [
     *         {
     *             "state" : "NY",
     *             "biggestCity" : {
     *                 "name" : "big",
     *                 "pop" : 100000
     *             },
     *             "smallestCity" : {
     *                 "name" : "small",
     *                 "pop" : 25
     *             }
     *         },
     *         {
     *             "state" : "CA",
     *             "biggestCity" : {
     *                 "name" : "big",
     *                 "pop" : 10000
     *             },
     *             "smallestCity" : {
     *                 "name" : "small",
     *                 "pop" : 21
     *             }
     *         },
     *         {
     *             "state" : "MD",
     *             "biggestCity" : {
     *                 "name" : "big",
     *                 "pop" : 1000
     *             },
     *             "smallestCity" : {
     *                 "name" : "small",
     *                 "pop" : 1
     *             }
     *         }
     *     ],
     *     "ok" : 1
     * }
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @see <a
     *      href="http://docs.mongodb.org/manual/tutorial/aggregation-examples/#largest-and-smallest-cities-by-state">Inspired
     *      By</a>
     */
    @Test
    public void testAggregate() {
        myConfig.setDefaultDurability(Durability.ACK);

        final DocumentBuilder doc = BuilderFactory.start();
        final MongoCollection aggregate = myDb.getCollection("aggregate");

        // > db.things.insert( { state : "NZ", city : "big", pop : 1000 } );
        doc.addString("state", "NZ").addString("city", "big")
                .addInteger("pop", 1000);
        aggregate.insert(doc);
        doc.reset();

        // > db.things.insert( { state : "MD", city : "big", pop : 1000 } );
        // > db.things.insert( { state : "MD", city : "medium", pop : 10 } );
        // > db.things.insert( { state : "MD", city : "small", pop : 1 } );
        doc.addString("state", "MD").addString("city", "big")
                .addInteger("pop", 1000);
        aggregate.insert(doc);
        doc.reset();
        doc.addString("state", "MD").addString("city", "medium")
                .addInteger("pop", 10);
        aggregate.insert(doc);
        doc.reset();
        doc.addString("state", "MD").addString("city", "small")
                .addInteger("pop", 1);
        aggregate.insert(doc);
        doc.reset();

        // > db.things.insert( { state : "CA", city : "big", pop : 10000 } );
        // > db.things.insert( { state : "CA", city : "medium", pop : 11 } );
        // > db.things.insert( { state : "CA", city : "small", pop : 10 } );
        doc.addString("state", "CA").addString("city", "big")
                .addInteger("pop", 10000);
        aggregate.insert(doc);
        doc.reset();
        doc.addString("state", "CA").addString("city", "small")
                .addInteger("pop", 11);
        aggregate.insert(doc);
        doc.reset();
        doc.addString("state", "CA").addString("city", "small")
                .addInteger("pop", 10);
        aggregate.insert(doc);
        doc.reset();

        // > db.things.insert( { state : "NY", city : "big", pop : 100000 } );
        // > db.things.insert( { state : "NY", city : "small", pop : 20 } );
        // > db.things.insert( { state : "NY", city : "small", pop : 5 } );
        doc.addString("state", "NY").addString("city", "big")
                .addInteger("pop", 100000);
        aggregate.insert(doc);
        doc.reset();
        doc.addString("state", "NY").addString("city", "small")
                .addInteger("pop", 20);
        aggregate.insert(doc);
        doc.reset();
        doc.addString("state", "NY").addString("city", "small")
                .addInteger("pop", 5);
        aggregate.insert(doc);
        doc.reset();

        final DocumentBuilder b1 = BuilderFactory.start();
        final DocumentBuilder b2 = BuilderFactory.start();
        final Aggregate.Builder builder = new Aggregate.Builder();

        builder.match(where("state").notEqualTo("NZ"))
                .group(id().addField("state").addField("city"),
                        set("pop").sum("pop"))
                .sort(asc("pop"))
                .group(id("_id.state"), set("biggestcity").last("_id.city"),
                        set("biggestpop").last("pop"),
                        set("smallestcity").first("_id.city"),
                        set("smallestpop").first("pop"))
                .project(
                        includeWithoutId(),
                        set("state", field("_id")),
                        set("biggestCity",
                                b1.add(set("name", field("biggestcity"))).add(
                                        set("pop", field("biggestpop")))),
                        set("smallestCity",
                                b2.add(set("name", field("smallestcity"))).add(
                                        set("pop", field("smallestpop")))))
                .sort(desc("biggestCity.pop"));

        final DocumentBuilder expected1 = BuilderFactory.start();
        expected1.addString("state", "NY");
        expected1.push("biggestCity").addString("name", "big")
                .addInteger("pop", 100000);
        expected1.push("smallestCity").addString("name", "small")
                .addInteger("pop", 25);

        final DocumentBuilder expected2 = BuilderFactory.start();
        expected2.addString("state", "CA");
        expected2.push("biggestCity").addString("name", "big")
                .addInteger("pop", 10000);
        expected2.push("smallestCity").addString("name", "small")
                .addInteger("pop", 21);

        final DocumentBuilder expected3 = BuilderFactory.start();
        expected3.addString("state", "MD");
        expected3.push("biggestCity").addString("name", "big")
                .addInteger("pop", 1000);
        expected3.push("smallestCity").addString("name", "small")
                .addInteger("pop", 1);

        final List<Document> expected = new ArrayList<Document>();
        expected.add(expected1.build());
        expected.add(expected2.build());
        expected.add(expected3.build());

        MongoIterator<Document> iter = null;
        try {
            final List<Document> results = new ArrayList<Document>();
            iter = aggregate.aggregate(builder.build());
            while (iter.hasNext()) {
                results.add(iter.next());
            }

            assertEquals(expected, results);
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Aggregate.REQUIRED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies the {@link Aggregate} command will use a cursor.
     */
    @Test
    public void testAggregateCursor() {
        myConfig.setDefaultDurability(Durability.ACK);

        final MongoCollection collection = largeCollection(myMongo);

        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.match(Find.ALL);
        builder.project(AggregationProjectFields.include("a"));

        builder.useCursor();
        builder.setBatchSize(100);

        MongoIterator<Document> iter = null;
        try {
            int count = 0;
            iter = collection.aggregate(builder);
            for (final Document found : iter) {
                assertNotNull(found);

                count += 1;
            }

            assertEquals(LARGE_COLLECTION_COUNT, count);
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the cursor attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Aggregate.CURSOR_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Verifies the function of Aggregate explain capability.
     */
    @Test
    public void testAggregateExplain() {
        myConfig.setDefaultDurability(Durability.ACK);

        final MongoCollection aggregate = myDb.getCollection("aggregate");

        final DocumentBuilder b1 = BuilderFactory.start();
        final DocumentBuilder b2 = BuilderFactory.start();
        final Aggregate.Builder builder = new Aggregate.Builder();

        builder.match(where("state").notEqualTo("NZ"))
                .group(id().addField("state").addField("city"),
                        set("pop").sum("pop"))
                .sort(asc("pop"))
                .group(id("_id.state"), set("biggestcity").last("_id.city"),
                        set("biggestpop").last("pop"),
                        set("smallestcity").first("_id.city"),
                        set("smallestpop").first("pop"))
                .project(
                        includeWithoutId(),
                        set("state", field("_id")),
                        set("biggestCity",
                                b1.add(set("name", field("biggestcity"))).add(
                                        set("pop", field("biggestpop")))),
                        set("smallestCity",
                                b2.add(set("name", field("smallestcity"))).add(
                                        set("pop", field("smallestpop")))))
                .sort(desc("biggestCity.pop"));

        try {
            final Document explanation = aggregate.explain(builder.build());

            // Just a quick look to make sure it looks like an explain plan.
            final ArrayElement stages = explanation.get(ArrayElement.class,
                    "stages");
            assertThat(stages, notNullValue());
            assertThat(stages.getEntries().size(), is(6));
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Aggregate.EXPLAIN_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies the {@link Aggregate} command will use a cursor.
     */
    @Test
    public void testAggregateStream() {
        myConfig.setDefaultDurability(Durability.ACK);

        final MongoCollection collection = largeCollection(myMongo);

        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.match(Find.ALL);
        builder.project(AggregationProjectFields.include("a"));

        builder.setCusorLimit(345);
        builder.useCursor();
        builder.setBatchSize(100);

        final MongoIterator<Document> iter = null;
        try {
            final Aggregate command = builder.build();
            final DocumentCallback callback = new DocumentCallback();
            collection.stream(callback, command);

            callback.waitFor(TimeUnit.SECONDS.toMillis(60));

            assertTrue(callback.isTerminated());
            assertFalse(callback.isTerminatedByNull());
            assertFalse(callback.isTerminatedByException());
            assertEquals(command.getCursorLimit(), callback.getCount());
            assertNull(callback.getException());
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Aggregate.CURSOR_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Verifies the {@link Aggregate} command will timeout.
     */
    @Test
    public void testAggregateTimeout() {
        myConfig.setDefaultDurability(Durability.ACK);

        final MongoCollection collection = largeCollection(myMongo);

        final Aggregate.Builder builder = new Aggregate.Builder();

        builder.maximumTime(1, TimeUnit.MILLISECONDS);
        builder.match(where("state").notEqualTo("NZ"));

        try {
            collection.aggregate(builder.build());
            fail("Should have thrown a timeout exception.");
        }
        catch (final MaximumTimeLimitExceededException expected) {
            // Good.
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Aggregate.MAX_TIMEOUT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies the {@link Aggregate} command will allow disk use.
     */
    @Test
    public void testAggregateWithAllowDiskUsage() {
        myConfig.setDefaultDurability(Durability.ACK);
        
        final int limit = 100;

        final MongoCollection collection = largeCollection(ourMongo);

        final Aggregate.Builder builder = new Aggregate.Builder();
        builder.match(Find.ALL);
        builder.project(AggregationProjectFields.include("a"));
        builder.limit(limit);

        builder.allowDiskUsage();

        MongoIterator<Document> iter = null;
        try {
            int count = 0;
            iter = collection.aggregate(builder);
            for (final Document found : iter) {
                assertNotNull(found);

                count += 1;
            }

            assertEquals(limit, count);
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the allowDiskUse attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Aggregate.ALLOW_DISK_USAGE_REQUIRED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Verifies using the $geoNear with the Aggregate Framework.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testAggregateWithGeoNear() {
        final double x = 5.1;
        final double y = 5.1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addDouble(x + 1).addDouble(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 2).addDouble(y + 2);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        MongoIterator<Document> iter = null;
        try {
            final List<Document> docs = new ArrayList<Document>();
            iter = getGeoCollection().aggregate(
                    Aggregate.builder().geoNear(
                            AggregationGeoNear.builder().location(p(x, y))
                                    .distanceField("d")));
            while (iter.hasNext()) {
                docs.add(iter.next());
            }

            assertThat(docs.size(), is(3));
            // Don't really care about the distance. Copy from the received
            // document.
            assertThat(docs.get(0), is(doc1.add(docs.get(0).get("d")).build()));
            assertThat(docs.get(1), is(doc2.add(docs.get(1).get("d")).build()));
            assertThat(docs.get(2), is(doc3.add(docs.get(2).get("d")).build()));
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Aggregate.GEO_NEAR_REQUIRED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies the ability to submit batched operations.
     * <p>
     * Batching requests is accomplished via the
     * {@link BatchedAsyncMongoCollection} interface which we get from the
     * {@link MongoCollection#startBatch()} method. The batch needs to always be
     * closed to submit the requests so we use a try-finally. In Java 1.7 we
     * could use a try-with-resources.
     * </p>
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws IllegalArgumentException
     *             On a test failure.
     */
    @Test
    public void testBatchedOperations() throws IllegalArgumentException,
            InterruptedException, ExecutionException {
        final List<Future<Integer>> insertResults = new ArrayList<Future<Integer>>();
        Future<Document> found = null;
        Future<Long> update = null;
        Future<Long> delete = null;
        Future<Long> count = null;
        Future<Document> found2 = null;
        BatchedAsyncMongoCollection batch = null;
        try {
            batch = myCollection.startBatch();

            // Now we can do as many CRUD operations we want. Even commands like
            // are supported.

            // We need some data. Lets create a documents with the _id field 'a'
            // thru 'z'.
            final DocumentBuilder builder = BuilderFactory.start();
            for (char c = 'a'; c <= 'z'; ++c) {
                builder.reset().add("_id", String.valueOf(c));

                // Returns a Future that will only complete once the batch
                // completes.
                insertResults.add(batch.insertAsync(builder));
            }

            // A query works.
            final Find.Builder find = Find.builder();
            find.query(where("_id").equals("a"));
            found = batch.findOneAsync(find);

            // An update too.
            final DocumentBuilder updateDoc = BuilderFactory.start();
            updateDoc.push("$set").add("marked", true);
            update = batch.updateAsync(Find.ALL, updateDoc, true, false);

            // Delete should work.
            delete = batch.deleteAsync(where("_id").equals("b"));

            // Commands... It is all there.
            count = batch.countAsync(Find.ALL);

            // Lets look at the 'a' doc one more time. It should have the
            // "marked" field now.
            found2 = batch.findOneAsync(find);

            // At this point nothing has been sent to the server. All of the
            // messages have been "spooled" waiting to be sent.
            // All of the messages will use the same connection
            // (unless a read preference directs a query to a different
            // server).

            // Lets prove it by waiting (not too long) on the first insert's
            // Future.
            try {
                insertResults.get(0).get(1, TimeUnit.SECONDS);
                fail("The insert should not finish until we close the batch.");
            }
            catch (final TimeoutException good) {
                // Good.
            }
        }
        finally {
            // Send the batch.
            if (batch != null) {
                batch.flush(); // Could also use batch.close().
            }
        }

        // Check out the results.
        final DocumentBuilder expected = BuilderFactory.start();

        // The inserts...
        for (final Future<Integer> insert : insertResults) {
            // Just checking for an error.
            assertThat(insert.get(), either(is(1)).or(is(0)));
        }
        assertThat(found.get(), is(expected.reset().add("_id", "a").build()));
        assertThat(update.get(), is(26L));
        assertThat(delete.get(), is(1L));
        assertThat(count.get(), is(25L));
        assertThat(
                found2.get(),
                is(expected.reset().add("_id", "a").add("marked", true).build()));
    }

    /**
     * Verifies the ability to submit batched writes. This should run on any
     * server just much faster on 2.6.
     */
    @Test
    public void testBatchedWriteReordered() {
        // Trigger the connection to set versions, etc.
        myCollection.count();
        myCollection.setReadPreference(ReadPreference.PRIMARY);

        final BatchedWrite.Builder write = BatchedWrite.builder();

        write.setMode(BatchedWriteMode.REORDERED);
        write.setDurability(Durability.ACK);
        final DocumentBuilder builder = BuilderFactory.start();
        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            builder.reset()
                    .add("_id", i)
                    .add("t",
                            "Now is the time for all good men to come to the aid.");

            write.insert(builder);
        }

        long result = myCollection.write(write);
        // Zero for before 2.6 and LARGE_COLLECTION_COUNT for after.
        assertThat(result, either(is((long) LARGE_COLLECTION_COUNT)).or(is(0L)));
        assertThat(myCollection.count(), is((long) LARGE_COLLECTION_COUNT));

        write.reset();
        write.setMode(BatchedWriteMode.REORDERED);
        write.setDurability(Durability.ACK);
        final DocumentBuilder update = BuilderFactory.start();
        update.push("$set").add("t", "Turns out it was not the time.");
        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            builder.reset().add("_id", i);

            write.update(builder, update);
        }
        result = myCollection.write(write);
        assertThat(result, is((long) LARGE_COLLECTION_COUNT));
        assertThat(myCollection.count(), is((long) LARGE_COLLECTION_COUNT));

        write.reset();
        write.setMode(BatchedWriteMode.REORDERED);
        write.setDurability(Durability.ACK);
        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            builder.reset().add("_id", i);

            write.delete(builder);
        }
        result = myCollection.write(write);
        assertThat(result, is((long) LARGE_COLLECTION_COUNT));
        assertThat(myCollection.count(), is(0L));
    }

    /**
     * Verifies the ability to submit batched writes. This should run on any
     * server just much faster on 2.6.
     */
    @Test
    public void testBatchedWriteSerialized() {
        // Trigger the connection to set versions.
        myCollection.count();
        myCollection.setReadPreference(ReadPreference.PRIMARY);

        final BatchedWrite.Builder write = BatchedWrite.builder();

        write.setMode(BatchedWriteMode.SERIALIZE_AND_CONTINUE);
        write.setDurability(Durability.ACK);
        final DocumentBuilder builder = BuilderFactory.start();
        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            builder.reset()
                    .add("_id", i)
                    .add("t",
                            "Now is the time for all good men to come to the aid.");

            write.insert(builder);
        }

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$set").add("t", "Turns out it was not the time.");
        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            builder.reset().add("_id", i);

            write.update(builder, update);
        }

        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            builder.reset().add("_id", i);

            write.delete(builder);
        }

        final long result = myCollection.write(write);
        assertThat(
                result,
                either(is(LARGE_COLLECTION_COUNT * 3L)).or(
                        is(LARGE_COLLECTION_COUNT * 2L)));
        assertThat(myCollection.count(), is(0L));
    }

    /**
     * Verifies the ability to submit batched writes. This should run on any
     * server just much faster on 2.6.
     */
    @Test
    public void testBatchedWriteSerializedAndStop() {
        // Trigger the connection to set versions.
        myCollection.count();

        final int count = 100;

        final BatchedWrite.Builder write = BatchedWrite.builder();

        write.setMode(BatchedWriteMode.SERIALIZE_AND_STOP);
        write.setDurability(Durability.ACK);
        final DocumentBuilder builder = BuilderFactory.start();
        for (int i = 0; i < count; ++i) {
            builder.reset()
                    .add("_id", i)
                    .add("t",
                            "Now is the time for all good men to come to the aid.");

            write.insert(builder);
        }

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$set").add("t", "Turns out it was not the time.");
        for (int i = 0; i < count; ++i) {
            builder.reset().add("_id", i);

            write.update(builder, update);
        }

        // Try and insert with the same id.
        builder.reset().add("_id", 10);
        write.insert(builder);

        // This one would have worked...
        builder.reset().add("_id", count + 1);
        write.insert(builder);

        for (int i = 0; i < count; ++i) {
            builder.reset().add("_id", i);

            write.delete(builder);
        }

        try {
            myCollection.write(write);
            fail("Should have thrown a BatchedWriteException");
        }
        catch (final BatchedWriteException error) {
            // Good.

            final Map<WriteOperation, Throwable> errors = error.getErrors();
            assertThat(errors.size(), is(1));
            final WriteOperation errorOp = errors.keySet().iterator().next();
            final Throwable errorThrown = errors.values().iterator().next();

            assertThat(errorOp.getType(), is(WriteOperationType.INSERT));
            assertThat(((InsertOperation) errorOp).getDocument(), is(builder
                    .reset().add("_id", 10).build()));
            assertThat(errorThrown, instanceOf(DuplicateKeyException.class));

            // Check the skipped items.
            final List<WriteOperation> skipped = error.getSkipped();
            assertThat(skipped.size(), is(count + 1));

            assertThat(skipped.get(0).getType(), is(WriteOperationType.INSERT));
            for (int i = 0; i < count; ++i) {
                assertThat(skipped.get(i + 1).getType(),
                        is(WriteOperationType.DELETE));
            }
        }

        assertThat(myCollection.count(), is((long) count));
    }

    /**
     * Verifies the ability to get the collection statistics.
     */
    @Test
    public void testCollectionStats() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        myCollection.insert(Durability.ACK, BuilderFactory.start());

        final Document result = myCollection.stats();
        assertEquals(new StringElement("ns", myDb.getName() + "."
                + myCollection.getName()), result.get("ns"));
    }

    /**
     * Verifies counting the number of documents in the collection.
     */
    @Test
    public void testCount() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        // Insert a million (tiny?) documents.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.insert(builder.build());

            assertEquals(i + 1, myCollection.count(MongoCollection.ALL));
        }
    }

    /**
     * Verifies that we cannot send a command (count in this case) with a query
     * that is over the maximum size.
     */
    @Test
    public void testCountDocumentToLarge() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 1);
        builder.add("bytes", new byte[Client.MAX_DOCUMENT_SIZE]);

        try {
            // Should not get to the point of being submitted.
            myCollection.countAsync(builder);
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertEquals(Client.MAX_DOCUMENT_SIZE, dtle.getMaximumSize());
        }
    }

    /**
     * Verifies counting the number of documents in the collection will timeout
     * if it takes too long.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testCountTimeout() throws ExecutionException,
            InterruptedException {

        final Count.Builder builder = new Count.Builder();
        // Need a query do it does not use an index of the collection meta-data.
        builder.query(where("g").lessThan(123));
        builder.maximumTime(1, TimeUnit.MILLISECONDS);

        try {
            final long before = System.currentTimeMillis();
            largeCollection(myMongo).count(builder.build());
            final long after = System.currentTimeMillis();

            assertThat("Should have thrown a timeout exception. Elapsed time: "
                    + (after - before) + " ms", after - before, lessThan(50L));
        }
        catch (final MaximumTimeLimitExceededException expected) {
            // Good.
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Count.MAX_TIMEOUT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies the ability to create a capped collection in the database.
     */
    @Test
    public void testCreateCappedCollection() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final String name = String.valueOf("testCreateCollection_"
                + Math.abs(myRandom.nextLong()));
        assertTrue(myDb.createCappedCollection(name, 100000));
        assertFalse(myDb.createCollection(name, BuilderFactory.start()));
        assertTrue(myDb.listCollectionNames().contains(name));

        assertTrue(myDb.getCollection(name).isCapped());

        myDb.getCollection(name).drop();
    }

    /**
     * Verifies the ability to create a collection in the database.
     */
    @Test
    public void testCreateCollection() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final String name = String.valueOf("testCreateCollection_"
                + myRandom.nextLong());
        assertTrue(myDb.createCollection(name, BuilderFactory.start()));
        assertFalse(myDb.createCollection(name, BuilderFactory.start()));
        assertTrue(myDb.listCollectionNames().contains(name));

        myDb.getCollection(name).drop();
    }

    /**
     * Tests that an index is successfully created.
     */
    @Test
    public void testCreateIndex() {

        myCollection.createIndex(Index.asc("foo"), Index.asc("bar"));

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        // Add some entries into the collection with the index fields.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);
            builder.addInteger("foo", 0);
            builder.addInteger("bar", i);

            myCollection.insert(builder.build());
        }

        // Now go find all of them by the covering index.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.query(where("foo").equals(0));
        findBuilder.projection(BuilderFactory.start().addBoolean("_id", false)
                .addBoolean("foo", true).addBoolean("bar", true).build());
        findBuilder.sort(Sort.asc("bar"));
        final MongoIterator<Document> iter = myCollection.find(findBuilder
                .build());
        int expectedId = 0;
        for (final Document found : iter) {

            assertNotNull(found);
            assertTrue(found.contains("foo"));
            assertEquals(new IntegerElement("foo", 0), found.get("foo"));
            assertTrue(found.contains("bar"));
            assertEquals(new IntegerElement("bar", expectedId),
                    found.get("bar"));

            expectedId += 1;
        }

        assertEquals(SMALL_COLLECTION_COUNT, expectedId);
    }

    /**
     * Verifies the ability to get the database statistics.
     */
    @Test
    public void testDatabaseStats() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final Document result = myDb.stats();
        Element element = result.get("db");
        if (isShardedConfiguration()) {
            element = result.findFirst("raw", ".*", "db");
        }
        assertEquals(new StringElement("db", myDb.getName()), element);
    }

    /**
     * Verifies the ability to delete a set of documents from the database.
     */
    @Test
    public void testDelete() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        // Insert (tiny?) documents.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.insert(builder.build());
        }

        assertEquals(SMALL_COLLECTION_COUNT,
                myCollection.count(MongoCollection.ALL));

        myConfig.setDefaultDurability(Durability.ACK);
        assertEquals(SMALL_COLLECTION_COUNT,
                myCollection.delete(MongoCollection.ALL));

        assertEquals(0, myCollection.count(MongoCollection.ALL));

    }

    /**
     * Verifies that we cannot send a delete with a query that is over the
     * maximum size.
     */
    @Test
    public void testDeleteDocumentToLarge() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 1);
        builder.add("bytes", new byte[Client.MAX_DOCUMENT_SIZE]);

        try {
            // Should not get to the point of being submitted.
            myCollection.deleteAsync(builder);
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertEquals(Client.MAX_DOCUMENT_SIZE, dtle.getMaximumSize());
            assertEquals(builder.build(), dtle.getDocument());
        }
    }

    /**
     * Verifies running a distinct command. <blockquote>
     * 
     * <pre>
     * <code>
     * db.addresses.insert({"zip-code": 10010})
     * db.addresses.insert({"zip-code": 10010})
     * db.addresses.insert({"zip-code": 99701})
     * 
     * db.addresses.distinct("zip-code");
     * [ 10010, 99701 ]
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Test
    public void testDistinct() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addString("zip-code", "10010");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addString("zip-code", "10010");

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addString("zip-code", "99701");

        myCollection.insert(Durability.ACK, doc1.build(), doc2.build(),
                doc3.build());

        final Set<String> expected = new HashSet<String>();
        expected.add("10010");
        expected.add("99701");

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("zip-code");
        final List<Element> items = myCollection.distinct(builder.build())
                .toList();

        final Set<String> actual = new HashSet<String>();
        for (final Element element : items) {
            actual.add(element.getValueAsString());
        }

        assertEquals(expected, actual);
    }

    /**
     * Verifies that a {@link Distinct} will timeout.
     */
    @Test
    public void testDistinctTimeout() {

        final MongoCollection collection = largeCollection(myMongo);

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("zip-code");
        builder.maximumTime(1, TimeUnit.MILLISECONDS);

        try {
            collection.distinct(builder.build());
            fail("Should have thrown a timeout exception.");
        }
        catch (final MaximumTimeLimitExceededException expected) {
            // Good.
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Distinct.MAX_TIMEOUT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies that a collection is removed from the database on a drop.
     */
    @Test
    public void testDropCollection() {
        // Make sure the collection/db exist.
        myCollection.insert(Durability.ACK, BuilderFactory.start().build());

        assertTrue(myDb.listCollectionNames().contains(myCollection.getName()));

        myCollection.drop();

        assertFalse(myDb.listCollectionNames().contains(myCollection.getName()));
    }

    /**
     * Verifies that a database is removed from the server on a drop.
     * 
     * @throws InterruptedException
     *             On a failure to sleep as part of the test.
     */
    @Test
    public void testDropDatabase() throws InterruptedException {
        // Make sure the collection/db exist.
        myCollection.insert(Durability.ACK, BuilderFactory.start().build());

        List<String> names = myMongo.listDatabaseNames();
        assertTrue("Database should be in the list: '" + TEST_DB_NAME + "' in "
                + names, names.contains(TEST_DB_NAME));

        // Pause for the config server to update.
        if (!myDb.drop() || isShardedConfiguration()) {
            // long now = System.currentTimeMillis();
            // final long deadline = now + TimeUnit.SECONDS.toMillis(30);
            // while ((now < deadline)
            // && myMongo.listDatabaseNames().contains(TEST_DB_NAME)) {
            // Thread.sleep(50);
            // now = System.currentTimeMillis();
            // }

            return;
        }

        names = myMongo.listDatabaseNames();
        assertFalse("Database should not be in the list any more: '"
                + TEST_DB_NAME + "' not in " + names,
                names.contains(TEST_DB_NAME));
    }

    /**
     * Verifies that indexes are properly dropped from the system indexes.
     */
    @Test
    public void testDropIndex() {
        myCollection.createIndex(Index.asc("foo"), Index.asc("bar"));

        Document found = myDb.getCollection("system.indexes").findOne(
                BuilderFactory.start()
                        .addRegularExpression("name", ".*foo.*", "").build());
        assertNotNull(found);

        myCollection.dropIndex(Index.asc("foo"), Index.asc("bar"));
        found = myDb.getCollection("system.indexes").findOne(
                BuilderFactory.start()
                        .addRegularExpression("name", ".*foo.*", "").build());
        assertNull(found);
    }

    /**
     * Verifies the ability to get a query plan via explain.
     */
    @Test
    public void testExplain() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        myCollection.createIndex(Index.asc("a"), Index.asc("b"));

        Document result = myCollection.explain(QueryBuilder.where("a")
                .equals(3).and("b").equals(5));
        assertEquals(new StringElement("cursor", "BtreeCursor a_1_b_1"),
                result.get("cursor"));

        result = myCollection.explain(QueryBuilder.where("f").equals(42));
        assertEquals(new StringElement("cursor", "BasicCursor"),
                result.get("cursor"));
    }

    /**
     * Verifies submitting a findAndModify command.
     */
    @Test
    public void testFindAndModify() {
        final DocumentBuilder doc = BuilderFactory.start();
        doc.addInteger("_id", 0);
        doc.addInteger("i", 0);

        myCollection.insert(doc.build());

        final DocumentBuilder query = BuilderFactory.start();
        query.addInteger("_id", 0);

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$inc").addInteger("i", 1);

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query.build());
        builder.setUpdate(update.build());
        builder.setReturnNew(true);

        final Document newDoc = myCollection.findAndModify(builder.build());
        assertNotNull(newDoc);
        assertEquals(new IntegerElement("i", 1), newDoc.get("i"));
    }

    /**
     * Verifies submitting a {@link FindAndModify} command timeout.
     */
    @Test
    public void testFindAndModifyTimeout() {

        final DocumentBuilder query = BuilderFactory.start();
        query.addInteger("g", 0);

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$inc").addInteger("i", 1);

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query.build());
        builder.setUpdate(update.build());
        builder.setReturnNew(true);

        builder.maximumTime(1, TimeUnit.MILLISECONDS);

        try {
            final long before = System.currentTimeMillis();
            largeCollection(myMongo).findAndModify(builder.build());
            final long after = System.currentTimeMillis();
            assertThat("Should have thrown a timeout exception. Elapsed time: "
                    + (after - before) + " ms", after - before, lessThan(50L));
        }
        catch (final MaximumTimeLimitExceededException expected) {
            // Good.
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(FindAndModify.MAX_TIMEOUT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies submitting a findAndModify command.
     */
    @Test
    public void testFindAndModifyWithNonExistantDocumentAndNoUpsert() {

        final DocumentBuilder query = BuilderFactory.start();
        query.addInteger("_id", 5);

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$inc").addInteger("i", 1);

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query.build());
        builder.setUpdate(update.build());
        builder.setReturnNew(true);

        try {
            final Document newDoc = myCollection.findAndModify(builder.build());
            assertNull(newDoc);
        }
        catch (final ReplyException re) {
            // MongoDB 1.8.X returns an error: No matching object found
            assumeThat(re.getMessage(),
                    not(containsString("No matching object found")));

            // Humm - Should have worked. Rethrow the error.
            throw re;
        }
    }

    /**
     * Verifies submitting a findAndModify command.
     */
    @Test
    public void testFindAndModifyWithUpsert() {

        final DocumentBuilder query = BuilderFactory.start();
        query.addInteger("_id", 0);

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$inc").addInteger("i", 1);

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query.build());
        builder.setUpdate(update.build());
        builder.upsert();
        builder.setReturnNew(true);

        final Document newDoc = myCollection.findAndModify(builder.build());
        assertNotNull(newDoc);
        assertEquals(new IntegerElement("i", 1), newDoc.get("i"));
    }

    /**
     * Test method for {@link ConditionBuilder#and}.
     */
    @Test
    public void testFindOneWithSubsetOfFields() {
        final ObjectId doc1Id = new ObjectId();
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", doc1Id);
        doc1.addInteger("a", 1);
        doc1.addInteger("b", 1);
        doc1.addInteger("c", 1);
        doc1.addInteger("d", 1);
        doc1.addInteger("e", 1);
        doc1.addInteger("f", 1);

        final ObjectId doc2Id = new ObjectId();
        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", doc2Id);
        doc2.addInteger("a", 1);
        doc2.addInteger("b", 2);
        doc2.addInteger("c", 3);
        doc2.addInteger("d", 4);
        doc2.addInteger("e", 5);
        doc2.addInteger("f", 6);
        doc2.addInteger("g", 7);

        myCollection.insert(Durability.ACK, doc1, doc2);

        // Expect the subset of fields.
        doc1.reset();
        doc1.addObjectId("_id", doc1Id);
        doc1.addInteger("a", 1);
        doc1.addInteger("b", 1);

        doc2.reset();
        doc2.addObjectId("_id", doc2Id);
        doc2.addInteger("a", 1);
        doc2.addInteger("b", 2);

        final Find.Builder find = new Find.Builder();
        find.setProjection(BuilderFactory.start().add("a", 1).add("b", 1));

        find.setQuery(and(where("a").equals(1), where("b").equals(1)));
        assertEquals(doc1.build(), myCollection.findOne(find.build()));

        find.setQuery(and(where("a").equals(1), where("b").equals(2)));
        assertEquals(doc2.build(), myCollection.findOne(find.build()));
    }

    /**
     * Verifies that a {@link Find} will timeout.
     */
    @Test
    public void testFindTimeout() {
        final MongoCollection collection = largeCollection(myMongo);

        final Find.Builder find = new Find.Builder();

        // A query without an index (so it takes longer).
        find.query(where("g").greaterThan(25));
        find.maximumTime(1, TimeUnit.MILLISECONDS);

        try {
            collection.find(find);
            fail("Should have thrown a timeout exception.");
        }
        catch (final MaximumTimeLimitExceededException expected) {
            // Good.
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Find.MAX_TIMEOUT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Test method for {@link ConditionBuilder#and}.
     */
    @Test
    public void testFindWithComment() {
        final ObjectId doc1Id = new ObjectId();
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", doc1Id);

        final ObjectId doc2Id = new ObjectId();
        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", doc2Id);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final Find.Builder find = new Find.Builder();
        find.setQuery(where("_id").equals(doc1Id).comment("Test comment"));

        try {
            final MongoIterator<Document> iter = myCollection
                    .find(find.build());
            try {
                assertTrue(iter.hasNext());
                assertEquals(doc1.build(), iter.next());
                assertFalse(iter.hasNext());
            }
            finally {
                iter.close();
            }
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(MiscellaneousOperator.COMMENT
                            .getVersion()));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Test method for {@link ConditionBuilder#and}.
     */
    @Test
    public void testFindWithCommentInProfile() {
        // Would have to find the right system.profile collection.
        if (!isShardedConfiguration()) {
            final ObjectId doc1Id = new ObjectId();
            final DocumentBuilder doc1 = BuilderFactory.start();
            doc1.addObjectId("_id", doc1Id);

            final ObjectId doc2Id = new ObjectId();
            final DocumentBuilder doc2 = BuilderFactory.start();
            doc2.addObjectId("_id", doc2Id);

            myCollection.insert(Durability.ACK, doc1, doc2);

            final Find.Builder find = new Find.Builder();
            find.setQuery(where("_id").equals(doc1Id).comment("Test comment"));

            myDb.setProfilingStatus(ProfilingStatus.ON);

            try {
                final MongoIterator<Document> iter = myCollection.find(find
                        .build());
                try {
                    assertTrue(iter.hasNext());
                    assertEquals(doc1.build(), iter.next());
                    assertFalse(iter.hasNext());
                }
                finally {
                    myDb.setProfilingStatus(ProfilingStatus.OFF);
                    iter.close();
                }
            }
            catch (final ServerVersionException sve) {
                // Check if we are talking to a recent MongoDB instance.
                assumeThat(sve.getActualVersion(),
                        greaterThanOrEqualTo(MiscellaneousOperator.COMMENT
                                .getVersion()));

                // Humm - Should have worked. Rethrow the error.
                throw sve;
            }

            final MongoCollection profile = myDb
                    .getCollection("system.profile");
            final MongoIterator<Document> iter = profile.find(where(
                    "query.$comment").equals("Test comment"));
            try {
                assertTrue(iter.hasNext());
                iter.next();
                assertFalse(iter.hasNext());
            }
            finally {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#and}.
     */
    @Test
    public void testFindWithSubsetOfFields() {
        final ObjectId doc1Id = new ObjectId();
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", doc1Id);
        doc1.addInteger("a", 1);
        doc1.addInteger("b", 1);
        doc1.addInteger("c", 1);
        doc1.addInteger("d", 1);
        doc1.addInteger("e", 1);
        doc1.addInteger("f", 1);

        final ObjectId doc2Id = new ObjectId();
        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", doc2Id);
        doc2.addInteger("a", 1);
        doc2.addInteger("b", 2);
        doc2.addInteger("c", 3);
        doc2.addInteger("d", 4);
        doc2.addInteger("e", 5);
        doc2.addInteger("f", 6);
        doc2.addInteger("g", 7);

        myCollection.insert(Durability.ACK, doc1, doc2);

        // Expect the subset of fields.
        doc1.reset();
        doc1.addObjectId("_id", doc1Id);
        doc1.addInteger("a", 1);
        doc1.addInteger("b", 1);

        doc2.reset();
        doc2.addObjectId("_id", doc2Id);
        doc2.addInteger("a", 1);
        doc2.addInteger("b", 2);

        final Find.Builder find = new Find.Builder();
        find.setProjection(BuilderFactory.start().add("a", 1).add("b", 1));

        find.setQuery(and(where("a").equals(1), where("b").equals(1)));
        MongoIterator<Document> iter = myCollection.find(find.build());
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        find.setQuery(and(where("a").equals(1), where("b").equals(2)));
        iter = myCollection.find(find.build());
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Verifies the ability to save and read files from GridFS.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGridFs() {

        final String name = GRIDFS_COLLECTION_ROOT_NAME + "grid_fs";

        shardCollection(name + GridFs.FILES_SUFFIX);
        shardCollection(name + GridFs.CHUNKS_SUFFIX,
                Index.asc(GridFs.FILES_ID_FIELD));

        myDb.setDurability(Durability.ACK);

        final long seed = System.currentTimeMillis();
        final byte[] buffer = new byte[313];
        final int blocks = 10000;

        File inFile = null;
        File outFile = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            inFile = File.createTempFile("infile", ".dat");
            outFile = File.createTempFile("outfile", ".dat");

            // Create the file.
            Random random = new Random(seed);
            out = new FileOutputStream(inFile);
            for (int i = 0; i < blocks; ++i) {
                random.nextBytes(buffer);
                out.write(buffer);
            }
            IOUtils.close(out);
            out = null;

            final GridFs gridfs = new GridFs(myDb, name);
            gridfs.createIndexes();

            in = new FileInputStream(inFile);
            gridfs.unlink("foo");
            gridfs.write("foo", in);
            IOUtils.close(in);
            in = null;

            out = new FileOutputStream(outFile);
            gridfs.read("foo", out);
            IOUtils.close(out);
            out = null;

            // Now compare the results.
            assertThat(outFile.length(), is(inFile.length()));

            random = new Random(seed); // Reset random to get the same stream of
            // values.
            final byte[] buffer2 = new byte[buffer.length];
            in = new FileInputStream(outFile);
            final DataInputStream din = new DataInputStream(in);
            for (int i = 0; i < blocks; ++i) {
                random.nextBytes(buffer2);
                din.readFully(buffer);

                assertThat(buffer2, is(buffer));
            }
            IOUtils.close(din);
            IOUtils.close(in);
            in = null;
        }
        catch (final IOException ioe) {
            fatal(ioe);
        }
        finally {
            IOUtils.close(in);
            IOUtils.close(out);
            if (inFile != null) {
                inFile.delete();
            }
            if (outFile != null) {
                outFile.delete();
            }
        }
    }

    /**
     * Verifies the ability to check the integrity of the Grid FS volume.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGridFsFsck() {

        final String name = GRIDFS_COLLECTION_ROOT_NAME + "fsck";

        shardCollection(name + GridFs.FILES_SUFFIX);
        shardCollection(name + GridFs.CHUNKS_SUFFIX,
                Index.asc(GridFs.FILES_ID_FIELD));

        myDb.setDurability(Durability.ACK);

        final long seed = 4567891234L; // Fixed seed so we get the same MD5.
        final byte[] buffer = new byte[313];
        final int blocks = 10000;

        File inFile = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            inFile = File.createTempFile("infile", ".dat");

            // Create the file.
            final Random random = new Random(seed);
            out = new FileOutputStream(inFile);
            for (int i = 0; i < blocks; ++i) {
                random.nextBytes(buffer);
                out.write(buffer);
            }
            IOUtils.close(out);
            out = null;

            final GridFs gridfs = new GridFs(myDb, name);

            gridfs.createIndexes();

            in = new FileInputStream(inFile);
            gridfs.unlink("foo");
            final ObjectId id = gridfs.write("foo", in);
            IOUtils.close(in);
            in = null;

            // Now damage all of the 'n' values.
            final MongoCollection chunks = myDb.getCollection(name
                    + GridFs.CHUNKS_SUFFIX);
            final DocumentBuilder update = BuilderFactory.start();
            final DocumentBuilder query = BuilderFactory.start();
            for (int i = 0; i < 10; ++i) {
                query.reset().add(GridFs.CHUNK_NUMBER_FIELD, i);
                update.reset().push("$set")
                        .add(GridFs.CHUNK_NUMBER_FIELD, myRandom.nextInt());
                chunks.update(query, update, true, false);
            }

            // Now Verify the results.
            assertThat(gridfs.validate(id), is(false));

            // And fsck finds the problem?
            Map<Object, List<String>> results = gridfs.fsck(false);
            assertThat(
                    results,
                    is(Collections.singletonMap(
                            (Object) id,
                            Arrays.asList("MD5 sums do not match. File document contains "
                                    + "'md5 : '36789b692294a0a9c43b989f664b688f'' and the "
                                    + "filemd5 command produced 'null'."))));

            // And see if the fsck repair works.
            results = gridfs.fsck(true);
            assertThat(
                    results,
                    is(Collections.singletonMap(
                            (Object) id,
                            Arrays.asList(
                                    "MD5 sums do not match. File document contains "
                                            + "'md5 : '36789b692294a0a9c43b989f664b688f'' and the "
                                            + "filemd5 command produced 'null'.",
                                    "File repaired."))));

            // Now Verify the results.
            assertThat(gridfs.validate(id), is(true));
        }
        catch (final IOException ioe) {
            fatal(ioe);
        }
        finally {
            IOUtils.close(in);
            IOUtils.close(out);
            if (inFile != null) {
                inFile.delete();
            }
        }
    }

    /**
     * Verifies the ability to verify a Grid FS file.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGridFsVerify() {

        final String name = GRIDFS_COLLECTION_ROOT_NAME + "verify";

        shardCollection(name + GridFs.FILES_SUFFIX);
        shardCollection(name + GridFs.CHUNKS_SUFFIX,
                Index.asc(GridFs.FILES_ID_FIELD));

        myDb.setDurability(Durability.ACK);

        final long seed = System.currentTimeMillis();
        final byte[] buffer = new byte[313];
        final int blocks = 10000;

        File inFile = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            inFile = File.createTempFile("infile", ".dat");

            // Create the file.
            final Random random = new Random(seed);
            out = new FileOutputStream(inFile);
            for (int i = 0; i < blocks; ++i) {
                random.nextBytes(buffer);
                out.write(buffer);
            }
            IOUtils.close(out);
            out = null;

            final GridFs gridfs = new GridFs(myDb, name);

            gridfs.createIndexes();

            in = new FileInputStream(inFile);
            gridfs.unlink("foo");
            final ObjectId id = gridfs.write("foo", in);
            IOUtils.close(in);
            in = null;

            // Now Verify the results.
            assertThat(gridfs.validate(id), is(true));
        }
        catch (final IOException ioe) {
            fatal(ioe);
        }
        finally {
            IOUtils.close(in);
            IOUtils.close(out);
            if (inFile != null) {
                inFile.delete();
            }
        }
    }

    /**
     * Verifies the function of a GroupBy command. <blockquote>
     * 
     * <pre>
     * <code>
     * { domain: "www.mongodb.org"
     * , invoked_at: {d:"2009-11-03", t:"17:14:05"}
     * , response_time: 0.05
     * , http_action: "GET /display/DOCS/Aggregate"
     * }
     * 
     * db.test.group(
     *    { cond: {"invoked_at.d": {$gte: "2009-11", $lt: "2009-12"}}
     *    , key: {http_action: true}
     *    , initial: {count: 0, total_time:0}
     *    , reduce: function(doc, out){ out.count++; out.total_time+=doc.response_time }
     *    , finalize: function(out){ out.avg_time = out.total_time / out.count }
     *    } );
     * 
     * [
     *   {
     *     "http_action" : "GET /display/DOCS/Aggregate",
     *     "count" : 1,
     *     "total_time" : 0.05,
     *     "avg_time" : 0.05
     *   }
     * ]
     * 
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Test
    public void testGroupBy() {
        final DocumentBuilder doc = BuilderFactory.start();
        doc.addString("domain", "www.mongodb.org");
        doc.push("invoked_at").addString("d", "2009-11-03")
                .addString("t", "17:14:05");
        doc.addDouble("response_time", 0.05);
        doc.addString("http_action", "GET /display/DOCS/Aggregate");

        myCollection.insert(Durability.ACK, doc.build());

        final DocumentBuilder query = BuilderFactory.start();
        query.push("invoked_at.d").addString("$gte", "2009-11")
                .addString("$lt", "2009-12");

        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("http_action"));
        builder.setInitialValue(BuilderFactory.start().addInteger("count", 0)
                .addDouble("total_time", 0.0).build());
        builder.setQuery(query.build());
        builder.setReduceFunction("function(doc, out){ out.count++; out.total_time+=doc.response_time }");
        builder.setFinalizeFunction("function(out){ out.avg_time = out.total_time / out.count }");

        final List<Element> results = myCollection.groupBy(builder.build())
                .toList();

        assertEquals(1, results.size());
        final Element entry = results.get(0);
        assertThat(entry, instanceOf(DocumentElement.class));

        final DocumentElement result = (DocumentElement) entry;
        assertEquals(new StringElement("http_action",
                "GET /display/DOCS/Aggregate"), result.get("http_action"));
        assertEquals(new DoubleElement("count", 1.0), result.get("count"));
        assertEquals(new DoubleElement("total_time", 0.05),
                result.get("total_time"));
        assertEquals(new DoubleElement("avg_time", 0.05),
                result.get("avg_time"));
    }

    /**
     * Verifies that a {@link GroupBy} will timeout.
     */
    @Test
    public void testGroupByTimeout() {

        final DocumentBuilder query = BuilderFactory.start();
        query.push("invoked_at.d").addString("$gte", "2009-11")
                .addString("$lt", "2009-12");

        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("http_action"));
        builder.setInitialValue(BuilderFactory.start().addInteger("count", 0)
                .addDouble("total_time", 0.0).build());
        builder.setQuery(query.build());
        builder.setReduceFunction("function(doc, out){ out.count++; out.total_time+=doc.response_time }");
        builder.setFinalizeFunction("function(out){ out.avg_time = out.total_time / out.count }");

        // Minimum amount of time.
        builder.maximumTime(1, TimeUnit.MILLISECONDS);

        try {
            final MongoCollection collection = largeCollection(myMongo);
            final long before = System.currentTimeMillis();
            collection.groupBy(builder.build());
            final long after = System.currentTimeMillis();
            assertThat("Should have thrown a timeout exception. Elapsed time: "
                    + (after - before) + " ms", after - before, lessThan(50L));
        }
        catch (final MaximumTimeLimitExceededException expected) {
            // Good.
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(GroupBy.MAX_TIMEOUT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies that we can insert a series of documents and then fetch them
     * from MongoDB one at a time via their _id.
     */
    @Test
    public void testInsertAlreadyExists() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("_id", 1);

        // Insert a doc.
        myCollection.insert(builder.build());

        // Insert a doc again. Should fail.
        try {
            myCollection.insert(builder.build());
            fail("Should have thrown a DuplicateKeyException");
        }
        catch (final DuplicateKeyException dke) {
            // Good.
            assertEquals(11000, dke.getErrorNumber());
        }
    }

    /**
     * Verifies that we can insert a series of documents and then fetch them
     * from MongoDB one at a time via their _id.
     */
    @Test
    public void testInsertAndFindOne() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        // Insert a million (tiny?) documents.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.insert(builder.build());
        }

        // Now go find each one.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            final Document found = myCollection.findOne(builder.build());
            assertNotNull(found);
            assertTrue(found.contains("_id"));
            assertEquals(new IntegerElement("_id", i), found.get("_id"));
        }
    }

    /**
     * Verifies that we cannot send a document that is over the maximum size.
     */
    @Test
    public void testInsertDocumentToLarge() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 1);
        builder.add("bytes", new byte[Client.MAX_DOCUMENT_SIZE]);

        try {
            // Should not get to the point of being submitted.
            myCollection.insertAsync(builder.build());
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertEquals(Client.MAX_DOCUMENT_SIZE, dtle.getMaximumSize());
            assertEquals(builder.build(), dtle.getDocument());
        }
    }

    /**
     * Verifies that the MongoDB iteration over a large collection works as
     * expected.
     * 
     * @throws InterruptedException
     *             On a failure of the test to wait.
     * @throws ExecutionException
     *             On a test failure.
     */
    @Test
    public void testIteratorAsync() throws InterruptedException,
            ExecutionException {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());

        final TestIteratorAsyncCallback cb = new TestIteratorAsyncCallback();
        collection.findAsync(cb, findBuilder.build());

        cb.check(); // Blocks.

        int count = 0;
        for (final Document found : cb.iter()) {

            assertNotNull(found);

            count += 1;
        }

        assertEquals(LARGE_COLLECTION_COUNT, count);
        cb.check();
    }

    /**
     * Verifies that the MongoDB iteration over a large collection works as
     * expected.
     * 
     * @throws InterruptedException
     *             On a failure of the test to wait.
     * @throws ExecutionException
     *             On a test failure.
     */
    @Test
    public void testIteratorAsyncRead() throws InterruptedException,
            ExecutionException {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());

        final TestIterateInAsyncCallback cb = new TestIterateInAsyncCallback();
        collection.findAsync(cb, findBuilder.build());

        cb.check(); // Blocks.
        assertEquals(LARGE_COLLECTION_COUNT, cb.count());
        cb.check();
    }

    /**
     * Verifies that the driver throws an exception when the durability is
     * violated.
     */
    @Test
    public void testJournalDurabilityThrows() {
        try {
            // We test with the journal off so this should fail for MongoDB 2.6
            // and later.
            myCollection.insert(Durability.journalDurable(10), BuilderFactory
                    .start().build());

            // Need to check the server version is after 2.6...
            // Use the maxTimeMs to check that for us since it was added at the
            // same time.
            testFindTimeout();

            fail("Should not be able to do a Journaled Write with a MongoDB "
                    + "server without Journaling on.");
        }
        catch (final DurabilityException error) {
            // Good.
            assertThat(
                    error.getMessage(),
                    anyOf(containsString("cannot use 'j' option when a host "
                            + "does not have journaling enabled"),
                            containsString("journaling not enabled on this "
                                    + "server"),
                            containsString("timed out waiting for slaves")));
        }
    }

    /**
     * Verifies the ability to list the collections for a database on the
     * server.
     */
    @Test
    public void testListCollections() {
        // Make sure the collection/db exist.
        myCollection.insert(Durability.ACK, BuilderFactory.start().build());

        final Collection<String> names = myDb.listCollectionNames();

        assertTrue(names.contains(myCollection.getName()));
        assertTrue(names.contains("system.indexes"));
    }

    /**
     * Verifies the ability to list the databases on the server.
     */
    @Test
    public void testListDatabases() {
        // Make sure the collection/db exist.
        myCollection.insert(Durability.ACK, BuilderFactory.start().build());

        final List<String> names = myMongo.listDatabaseNames();

        assertTrue(
                "Missing the '" + TEST_DB_NAME + "' database name: " + names,
                names.contains(TEST_DB_NAME));
    }

    /**
     * Verifies the function of MapReduce via a sample Map/Reduce <blockquote>
     * 
     * <pre>
     * <code>
     * > db.things.insert( { _id : 1, tags : ['dog', 'cat'] } );
     * > db.things.insert( { _id : 2, tags : ['cat'] } );
     * > db.things.insert( { _id : 3, tags : ['mouse', 'cat', 'dog'] } );
     * > db.things.insert( { _id : 4, tags : []  } );
     * 
     * > // map function
     * > m = function(){
     * ...    this.tags.forEach(
     * ...        function(z){
     * ...            emit( z , { count : 1 } );
     * ...        }
     * ...    );
     * ...};
     * 
     * > // reduce function
     * > r = function( key , values ){
     * ...    var total = 0;
     * ...    for ( var i=0; i<values.length; i++ )
     * ...        total += values[i].count;
     * ...    return { count : total };
     * ...};
     * 
     * > res = db.things.mapReduce(m, r, { out : "myoutput" } );
     * > res
     * {
     *     "result" : "myoutput",
     *     "timeMillis" : 12,
     *     "counts" : {
     *         "input" : 4,
     *         "emit" : 6,
     *         "output" : 3
     *     },
     *     "ok" : 1,
     * }
     * > db.myoutput.find()
     * {"_id" : "cat" , "value" : {"count" : 3}}
     * {"_id" : "dog" , "value" : {"count" : 2}}
     * {"_id" : "mouse" , "value" : {"count" : 1}}
     * </code>
     * </pre>
     * 
     * </blockquote>
     */
    @Test
    public void testMapReduce() {
        final MongoCollection mr = myDb.getCollection("mr");

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addInteger("_id", 1);
        doc1.pushArray("tags").addString("dog").addString("cat");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addInteger("_id", 2);
        doc2.pushArray("tags").addString("cat");

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addInteger("_id", 3);
        doc3.pushArray("tags").addString("mouse").addString("dog")
                .addString("cat");

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addInteger("_id", 4);
        doc4.pushArray("tags");

        myConfig.setDefaultDurability(Durability.ACK);
        mr.insert(doc1.build(), doc2.build(), doc3.build(), doc4.build());

        final MapReduce.Builder mrBuilder = new MapReduce.Builder();
        mrBuilder.setMapFunction("function() {                              "
                + "    this.tags.forEach(                                   "
                + "               function(z){                              "
                + "                       emit( z , { count : 1 } );        "
                + "               }                                         "
                + "    );                                                   "
                + "};");
        mrBuilder.setReduceFunction("function( key , values ){              "
                + "    var total = 0;                                       "
                + "    for ( var i=0; i<values.length; i++ ) {              "
                + "        total += values[i].count;                        "
                + "    }                                                    "
                + "    return { count : total };                            "
                + "};");
        mrBuilder.setOutputName("myoutput");
        mrBuilder.setOutputType(MapReduce.OutputType.REPLACE);

        mr.mapReduce(mrBuilder.build());

        final DocumentBuilder expected1 = BuilderFactory.start();
        expected1.addString("_id", "cat");
        expected1.push("value").addDouble("count", 3);

        final DocumentBuilder expected2 = BuilderFactory.start();
        expected2.addString("_id", "dog");
        expected2.push("value").addDouble("count", 2);

        final DocumentBuilder expected3 = BuilderFactory.start();
        expected3.addString("_id", "mouse");
        expected3.push("value").addDouble("count", 1);

        final Set<Document> expected = new HashSet<Document>();
        expected.add(expected1.build());
        expected.add(expected2.build());
        expected.add(expected3.build());

        final Set<Document> actual = new HashSet<Document>();
        final MongoCollection out = myDb.getCollection("myoutput");
        for (final Document doc : out.find(MongoCollection.ALL)) {
            actual.add(doc);
        }

        assertEquals(expected, actual);
    }

    /**
     * Verifies that a {@link MapReduce} will timeout.
     */
    @Test
    public void testMapReduceTimeout() {
        final MongoCollection collection = largeCollection(myMongo);

        final MapReduce.Builder mrBuilder = new MapReduce.Builder();
        mrBuilder.setMapFunction("function() {                              "
                + "    this.tags.forEach(                                   "
                + "               function(z){                              "
                + "                       emit( z , { count : 1 } );        "
                + "               }                                         "
                + "    );                                                   "
                + "};");
        mrBuilder.setReduceFunction("function( key , values ){              "
                + "    var total = 0;                                       "
                + "    for ( var i=0; i<values.length; i++ ) {              "
                + "        total += values[i].count;                        "
                + "    }                                                    "
                + "    return { count : total };                            "
                + "};");
        mrBuilder.setOutputName("myoutput");
        mrBuilder.setOutputType(MapReduce.OutputType.REPLACE);

        // Shortest possible timeout.
        mrBuilder.maximumTime(1, TimeUnit.MILLISECONDS);

        try {
            collection.mapReduce(mrBuilder);
            fail("Should have thrown a timeout exception.");
        }
        catch (final MaximumTimeLimitExceededException expected) {
            // Good.
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the maximum time attribute.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(MapReduce.MAX_TIMEOUT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies that the MongoDB iteration over a large collection works as
     * expected.
     */
    @Test
    public void testMultiFetchIterator() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(10);

        final MongoIterator<Document> iter = collection.find(findBuilder
                .build());
        int count = 0;
        for (final Document found : iter) {

            assertNotNull(found);

            count += 1;
        }

        assertEquals(LARGE_COLLECTION_COUNT, count);
    }

    /**
     * Verifies that the MongoDB iteration over a large collection works as
     * expected.
     */
    @Test
    public void testMultiFetchIteratorWithLimit() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(10);
        findBuilder.setLimit(123);

        final MongoIterator<Document> iter = collection.find(findBuilder
                .build());
        int count = 0;
        for (final Document found : iter) {

            assertNotNull(found);

            count += 1;
        }

        assertEquals(findBuilder.build().getLimit(), count);
    }

    /**
     * Verifies that the MongoDB iteration over a large collection works as
     * expected.
     */
    @Test
    public void testMultiFetchIteratorWithLimitRestart() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(10);
        findBuilder.setLimit(123);

        MongoIterator<Document> iter = collection.find(findBuilder.build());
        int count = 0;
        for (final Document found : iter) {

            assertNotNull(found);

            count += 1;

            // Only read a few documents and then stop.
            iter.stop();
        }
        // Should not have read all of the documents, yet.
        assertNotEquals(findBuilder.build().getLimit(), count);

        // Restart the connections.
        IOUtils.close(myMongo);
        connect();

        // Restart the iterator.
        iter = myMongo.restart(iter.asDocument());
        for (final Document found : iter) {

            assertNotNull(found);

            count += 1;
        }

        // Now we have read all of the documents.
        assertEquals(findBuilder.build().getLimit(), count);
    }

    /**
     * Verifies running a {@code parallelCollectionScan} command.
     */
    @Test
    public void testParallelScan() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        final int cores = Runtime.getRuntime().availableProcessors();
        final ParallelScan.Builder scan = ParallelScan.builder()
                .requestedIteratorCount(cores).batchSize(10);
        try {
            int count = 0;
            final Collection<MongoIterator<Document>> iterators = collection
                    .parallelScan(scan);
            for (final MongoIterator<Document> iter : iterators) {
                for (final Document found : iter) {

                    assertNotNull(found);

                    count += 1;
                }
            }

            assertThat(iterators.size(), Matchers.allOf(
                    greaterThanOrEqualTo(1), lessThanOrEqualTo(cores)));
            assertEquals(LARGE_COLLECTION_COUNT, count);
        }
        catch (final ReplyException shardedMaybe) {
            if (isShardedConfiguration()) {
                assertThat(shardedMaybe.getMessage(),
                        containsString("no such cmd: parallelCollectionScan"));
            }
            else {
                throw shardedMaybe;
            }
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the parallelCollectionScan.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(ParallelScan.REQUIRED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies running a {@code parallelCollectionScan} command will timeout.
     */
    @Test
    public void testParallelScanOnSecondary() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        final int cores = Runtime.getRuntime().availableProcessors();
        final ParallelScan.Builder scan = ParallelScan.builder()
                .requestedIteratorCount(cores).batchSize(10)
                .readPreference(ReadPreference.PREFER_SECONDARY);
        try {
            int count = 0;
            final Collection<MongoIterator<Document>> iterators = collection
                    .parallelScan(scan);
            for (final MongoIterator<Document> iter : iterators) {
                for (final Document found : iter) {

                    assertNotNull(found);

                    count += 1;
                }
            }

            assertThat(iterators.size(), Matchers.allOf(
                    greaterThanOrEqualTo(1), lessThanOrEqualTo(cores)));
            assertEquals(LARGE_COLLECTION_COUNT, count);
        }
        catch (final ReplyException shardedMaybe) {
            if (isShardedConfiguration()) {
                assertThat(shardedMaybe.getMessage(),
                        containsString("no such cmd: parallelCollectionScan"));
            }
            else {
                throw shardedMaybe;
            }
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance
            // That supports the parallelCollectionScan.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(ParallelScan.REQUIRED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
    }

    /**
     * Verifies the ability to adjust the profiling status.
     */
    @Test
    public void testProfilingStatus() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        if (!isShardedConfiguration()) {
            assertEquals(ProfilingStatus.OFF, myDb.getProfilingStatus());

            for (final ProfilingStatus status : Arrays.asList(
                    ProfilingStatus.ON, ProfilingStatus.slow(100),
                    ProfilingStatus.OFF, ProfilingStatus.slow(1000))) {
                assertTrue(myDb.setProfilingStatus(status));
                assertFalse(myDb.setProfilingStatus(status));

                assertEquals(status, myDb.getProfilingStatus());
            }
        }
    }

    /**
     * Verifies that we cannot send a query document that is over the maximum
     * size.
     */
    @Test
    public void testQueryDocumentToLarge() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 1);
        builder.add("bytes", new byte[Client.MAX_DOCUMENT_SIZE]);

        try {
            // Should not get to the point of being submitted.
            myCollection.findAsync(builder.build());
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertEquals(Client.MAX_DOCUMENT_SIZE, dtle.getMaximumSize());
            assertEquals(builder.build(), dtle.getDocument());
        }
    }

    /**
     * Test method for {@link ConditionBuilder#all}.
     */
    @Test
    public void testQueryWithAll() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("a").addInteger(1).addString("b").addBoolean(true);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("a").addInteger(1).addString("c").addBoolean(true);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a").all(
                constant(true), constant("b")));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#and}.
     */
    @Test
    public void testQueryWithAnd() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", 1);
        doc1.addInteger("b", 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", 1);
        doc2.addInteger("b", 2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(and(
                where("a").equals(1), where("b").equals(1)));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        iter = myCollection.find(where("a").equals(1).and("b").equals(2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#elementMatches}.
     */
    @Test
    public void testQueryWithElementMatches() {
        ArrayBuilder ab = null;
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        ab = doc1.pushArray("a");
        ab.push().addInteger("b", -1);
        ab.push().addInteger("c", 1);
        ab.push().addInteger("b", 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        ab = doc2.pushArray("a");
        ab.push().addInteger("b", -1);
        ab.push().addInteger("c", 1);
        ab.push().addInteger("d", 1);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .elementMatches(where("b").equals(1)));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(boolean)}.
     */
    @Test
    public void testQueryWithEqualsBoolean() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBoolean("a", true);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBoolean("a", false);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(true));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(byte[])}.
     */
    @Test
    public void testQueryWithEqualsByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[myRandom.nextInt(100) + 1];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", bytes2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(byte, byte[])}.
     */
    @Test
    public void testQueryWithEqualsByteByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[myRandom.nextInt(100) + 1];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", (byte) 12, bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", (byte) 12, bytes2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addBinary("a", (byte) 13, bytes1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals((byte) 12, bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(DocumentAssignable)} .
     */
    @Test
    public void testQueryWithEqualsDocumentAssignable() {

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.push("a").addInteger("b", 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.push("a").addInteger("b", 2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(BuilderFactory.start().addInteger("b", 1)));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(double)}.
     */
    @Test
    public void testQueryWithEqualsDoubleCloseToInteger() {
        final double d1 = myRandom.nextInt();
        final double d2 = Double.longBitsToDouble(myRandom.nextLong());

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addDouble("a", d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addDouble("a", d2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", (int) d1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addLong("a", (long) d1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(d1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc3.build());
            expected.add(doc4.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(double)}.
     */
    @Test
    public void testQueryWithEqualsDoubleNotCloseToInt() {
        final double d1 = myRandom.nextInt() + 0.5;
        final double d2 = Double.longBitsToDouble(myRandom.nextLong());

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addDouble("a", d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addDouble("a", d2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", (int) d1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addLong("a", (long) d1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(d1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(int)}.
     */
    @Test
    public void testQueryWithEqualsInt() {
        final int v1 = myRandom.nextInt();
        final int v2 = myRandom.nextInt();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addLong("a", v1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addDouble("a", v1);

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addDouble("a", 0.1 + v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(v1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc3.build());
            expected.add(doc4.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsJavaScript(String)}.
     */
    @Test
    public void testQueryWithEqualsJavaScriptString() {
        final String v1 = "a == 1";
        final String v2 = "b == 1";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addJavaScript("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addJavaScript("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addJavaScript("a", v1, BuilderFactory.start().asDocument());

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addString("a", v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsJavaScript(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#equalsJavaScript(String, DocumentAssignable)} .
     */
    @Test
    public void testQueryWithEqualsJavaScriptStringDocument() {
        final String v1 = "a == 1";
        final String v2 = "b == 1";
        final Document d1 = BuilderFactory.start().addInteger("a", 1)
                .asDocument();
        final Document d2 = BuilderFactory.start().addInteger("a", 2)
                .asDocument();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addJavaScript("a", v1, d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addJavaScript("a", v2, d1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addJavaScript("a", v1, d2);

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addString("a", v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsJavaScript(v1, d1));
        try {
            // Bug in MongoDB? - Scope is being ignored.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc4.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(long)}.
     */
    @Test
    public void testQueryWithEqualsLong() {
        final long v1 = myRandom.nextInt(); // Keep on integer scale.
        final long v2 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addLong("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", (int) v1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addDouble("a", v1);

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addDouble("a", 0.1 + v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(v1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc3.build());
            expected.add(doc4.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsMaxKey()}.
     */
    @Test
    public void testQueryWithEqualsMaxKey() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMaxKey("a");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMinKey("a");

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addNull("a");

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addInteger("b", 1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsMaxKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());

            // MongoDB 2.0.7 does not return more documents. 2.2.0 does.
            if (iter.hasNext()) {
                assertTrue(iter.hasNext());
                received.add(iter.next());
                assertTrue(iter.hasNext());
                received.add(iter.next());
                assertTrue(iter.hasNext());
                received.add(iter.next());
                assertTrue(iter.hasNext());
                received.add(iter.next());
                assertFalse(iter.hasNext());

                expected.add(doc2.build());
                expected.add(doc3.build());
                expected.add(doc4.build());
                expected.add(doc5.build());
            }

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsMinKey()}.
     */
    @Test
    public void testQueryWithEqualsMinKey() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMaxKey("a");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMinKey("a");

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addNull("a");

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addInteger("b", 1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsMinKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc2.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());

            // MongoDB 2.0.7 does not return more documents. 2.2.0 does.
            if (iter.hasNext()) {
                received.add(iter.next());
                assertTrue(iter.hasNext());
                received.add(iter.next());
                assertTrue(iter.hasNext());
                received.add(iter.next());
                assertTrue(iter.hasNext());
                received.add(iter.next());
                assertFalse(iter.hasNext());

                expected.add(doc1.build());
                expected.add(doc3.build());
                expected.add(doc4.build());
                expected.add(doc5.build());
            }

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsMongoTimestamp(long)}.
     */
    @Test
    public void testQueryWithEqualsMongoTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMongoTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addLong("a", v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsMongoTimestamp(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsMongoTimestamp(long)}.
     */
    @Test
    public void testQueryWithEqualsMongoTimestampFailsWhenEncountersATimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMongoTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addLong("a", v1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addTimestamp("a", v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4);

        MongoIterator<Document> iter = null;
        try {
            iter = myCollection.find(where("a").equalsMongoTimestamp(v1));
            iter.hasNext();
            fail("Expected to throw."); // But not in 1.8.
        }
        catch (final QueryFailedException expected) {
            // Bug in MongoDB 2.2.0
            assertThat(expected.getMessage(),
                    containsString("wrong type for field (a) 17 != 9"));
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsNull()}.
     */
    @Test
    public void testQueryWithEqualsNull() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addNull("a");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMinKey("a");

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addMaxKey("a");

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addInteger("b", 1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsNull());
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc5.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(ObjectId)} .
     */
    @Test
    public void testQueryWithEqualsObjectId() {
        final ObjectId v1 = new ObjectId();
        final ObjectId v2 = new ObjectId();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addObjectId("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addObjectId("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(java.util.regex.Pattern)}.
     */
    @Test
    public void testQueryWithEqualsPattern() {
        final Pattern v1 = Pattern.compile("abc.*f");
        final Pattern v2 = Pattern.compile("abc.*def");

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addRegularExpression("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addRegularExpression("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addString("a", "abcdef"); // Chosen to match!

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addString("a", "hello"); // Chosen to not match!

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addSymbol("a", "abcdef"); // Chosen to match!

        final DocumentBuilder doc6 = BuilderFactory.start();
        doc6.addObjectId("_id", new ObjectId());
        doc6.addSymbol("a", "hello"); // Chosen to not match!

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5, doc6);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(v1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc3.build());
            expected.add(doc5.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equals(String)}.
     */
    @Test
    public void testQueryWithEqualsString() {
        final String v1 = "v1";
        final String v2 = "v2";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addString("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addSymbol("a", v1);

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addJavaScript("a", v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equals(v1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc4.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsSymbol(String)}.
     */
    @Test
    public void testQueryWithEqualsSymbol() {
        final String v1 = "v1";
        final String v2 = "v2";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addSymbol("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addString("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addString("a", v1);

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addJavaScript("a", v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsSymbol(v1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc4.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#equalsTimestamp(long)}.
     */
    @Test
    public void testQueryWithEqualsTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addLong("a", v1);

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addMongoTimestamp("a", v1);

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .equalsTimestamp(v1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc4.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }

    }

    /**
     * Test method for {@link ConditionBuilder#exists}.
     */
    @Test
    public void testQueryWithExists() {
        final ObjectId v1 = new ObjectId();
        final ObjectId v2 = new ObjectId();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addObjectId("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addObjectId("b", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(where("a").exists());
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        iter = myCollection.find(where("b").exists(false));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#geoWithin(DocumentAssignable)}.
     */
    @Test
    public void testQueryWithGeoWithinDocumentAssignable() {
        final Document doc1 = Json
                .parse("{_id: 'P1', p: {type: 'Point', coordinates: [2,2] } } )");
        final Document doc2 = Json
                .parse("{_id: 'P2', p: {type: 'Point', coordinates: [3,6] } } )");
        final Document doc3 = Json
                .parse("{_id: 'Poly1', p: {type: 'Polygon', coordinates: ["
                        + "[ [3,1], [1,2], [5,6], [9,2], [4,3], [3,1] ]] } } )");
        final Document doc4 = Json
                .parse("{_id: 'LS1', p: {type: 'LineString', "
                        + "coordinates: [ [5,2], [7,3], [7,5], [9,4] ] } } )");

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3,
                    doc4);

            iter = getGeoSphereCollection().find(
                    where("p").geoWithin(
                            GeoJson.polygon(Arrays.asList(p(0, 0), p(3, 0),
                                    p(3, 3), p(0, 3), p(0, 0)))));

            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1);

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
            iter.close();
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#geoWithin(DocumentAssignable)}.
     */
    @Deprecated
    @Test
    public void testQueryWithGeoWithinUniqueDocsFalse() {
        final Document doc2 = Json
                .parse("{_id: 'P2', p: {type: 'Point', coordinates: [1,1] } } )");
        final Document doc3 = Json
                .parse("{_id: 'Poly1', p: {type: 'Polygon', coordinates: ["
                        + "[ [3,1], [1,2], [5,6], [9,2], [4,3], [3,1] ]] } } )");
        final Document doc4 = Json
                .parse("{_id: 'LS1', p: {type: 'LineString', "
                        + "coordinates: [ [5,2], [7,3], [7,5], [9,4] ] } } )");

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc2, doc3, doc4);

            iter = getGeoSphereCollection().find(
                    where("p").geoWithin(
                            GeoJson.polygon(Arrays.asList(p(0, 0), p(3, 0),
                                    p(3, 3), p(0, 3), p(0, 0))), false));

            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc2);

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
            iter.close();
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#geoWithin(DocumentAssignable)}.
     */
    @Test
    public void testQueryWithGeoWithinWithMultiPointField() {
        final Document doc1 = BuilderFactory.start().add("_id", "P1")
                .add("p", GeoJson.multiPoint(p(2, 2), p(1, 1))).build();
        final Document doc2 = Json
                .parse("{_id: 'P2', p: {type: 'Point', coordinates: [3,6] } } )");
        final Document doc3 = Json
                .parse("{_id: 'Poly1', p: {type: 'Polygon', coordinates: ["
                        + "[ [3,1], [1,2], [5,6], [9,2], [4,3], [3,1] ]] } } )");
        final Document doc4 = Json
                .parse("{_id: 'LS1', p: {type: 'LineString', "
                        + "coordinates: [ [5,2], [7,3], [7,5], [9,4] ] } } )");

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3,
                    doc4);

            iter = getGeoSphereCollection().find(
                    where("p").geoWithin(
                            GeoJson.polygon(Arrays.asList(p(0, 0), p(3, 0),
                                    p(3, 3), p(0, 3), p(0, 0)))));

            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1);

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
            iter.close();
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.6 (for Multi-Point GeoJSON support)

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(GeoJson.MULTI_SUPPORT_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        catch (final ReplyException re) {
            // See if we failed to insert the MultiPoint document.
            assumeThat(re.getMessage(),
                    not(containsString("Can't extract geo keys from object, "
                            + "malformed geometry?:{ type: \"MultiPoint\"")));

            // Humm - Should have worked. Rethrow the error.
            throw re;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(byte[])}.
     */
    @Test
    public void testQueryWithGreaterThanByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 1;
        bytes2[0] = 0;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", bytes2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThan(bytes2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(byte, byte[])}.
     */
    @Test
    public void testQueryWithGreaterThanByteByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", (byte) 12, bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", (byte) 11, bytes1);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThan((byte) 11, bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        bytes2[0] = 0;
        iter = myCollection.find(where("a").greaterThan((byte) 12, bytes2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(double)}.
     */
    @Test
    public void testQueryWithGreaterThanDouble() {
        final double d1 = myRandom.nextInt();
        final double d2 = d1 - 0.1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addDouble("a", d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addDouble("a", d2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThan(d2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(int)}.
     */
    @Test
    public void testQueryWithGreaterThanInt() {
        final int v1 = myRandom.nextInt();
        final int v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(long)}.
     */
    @Test
    public void testQueryWithGreaterThanLong() {
        final long v1 = myRandom.nextInt();
        final long v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addLong("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanMongoTimestamp(long)}.
     */
    @Test
    public void testQueryWithGreaterThanMongoTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMongoTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanMongoTimestamp(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(ObjectId)} .
     */
    @Test
    public void testQueryWithGreaterThanObjectId() {
        // ObjectId's increase in time.
        final ObjectId v2 = new ObjectId();
        final ObjectId v1 = new ObjectId();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addObjectId("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addObjectId("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(byte[])}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 2;
        bytes2[0] = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", bytes2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualTo(bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualTo(byte, byte[])}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToByteByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 1;
        bytes2[0] = 0;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", (byte) 12, bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", (byte) 11, bytes2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualTo((byte) 11, bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        iter = myCollection.find(where("a").greaterThanOrEqualTo((byte) 12,
                bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(double)}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToDouble() {
        final double d1 = myRandom.nextInt();
        final double d2 = d1 - 0.1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addDouble("a", d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addDouble("a", d2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualTo(d1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(int)}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToInt() {
        final int v1 = myRandom.nextInt();
        final int v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(long)}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToLong() {
        final long v1 = myRandom.nextInt();
        final long v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addLong("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualToMongoTimestamp(long)}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToMongoTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMongoTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualToMongoTimestamp(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(ObjectId)} .
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToObjectId() {
        // ObjectId's increase in time.
        final ObjectId v2 = new ObjectId();
        final ObjectId v1 = new ObjectId();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addObjectId("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addObjectId("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanOrEqualTo(String)}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToString() {
        final String v1 = "b";
        final String v2 = "a";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addString("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualToSymbol(String)}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToSymbol() {
        final String v1 = "b";
        final String v2 = "a";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addSymbol("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addSymbol("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualToSymbol(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#greaterThanOrEqualToTimestamp(long)}.
     */
    @Test
    public void testQueryWithGreaterThanOrEqualToTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanOrEqualToTimestamp(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThan(String)}.
     */
    @Test
    public void testQueryWithGreaterThanString() {
        final String v1 = "b";
        final String v2 = "a";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addString("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanSymbol(String)}.
     */
    @Test
    public void testQueryWithGreaterThanSymbol() {
        final String v1 = "b";
        final String v2 = "a";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addSymbol("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addSymbol("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanSymbol(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#greaterThanTimestamp(long)}.
     */
    @Test
    public void testQueryWithGreaterThanTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 - 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .greaterThanTimestamp(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#in}.
     */
    @Test
    public void testQueryWithIn() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", "b");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBoolean("a", false);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addInteger("a", 1);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a").in(
                constant(true), constant("b")));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#instanceOf(ElementType)}.
     */
    @Test
    public void testQueryWithInstanceOf() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", "b");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBoolean("a", false);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addSymbol("a", "a");

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .instanceOf(ElementType.STRING));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#intersects(DocumentAssignable)}.
     */
    @Test
    public void testQueryWithIntersectsDocumentAssignable() {
        final Document doc1 = Json
                .parse("{_id: 'P1', p: {type: 'Point', coordinates: [2,2] } } )");
        final Document doc2 = Json
                .parse("{_id: 'P2', p: {type: 'Point', coordinates: [3,6] } } )");
        final Document doc3 = Json
                .parse("{_id: 'Poly1', p: {type: 'Polygon', coordinates: ["
                        + "[ [3,1], [1,2], [5,6], [9,2], [4,3], [3,1] ]] } } )");
        final Document doc4 = Json
                .parse("{_id: 'LS1', p: {type: 'LineString', "
                        + "coordinates: [ [5,2], [7,3], [7,5], [9,4] ] } } )");

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3,
                    doc4);

            iter = getGeoSphereCollection().find(
                    where("p").intersects(
                            GeoJson.polygon(Arrays.asList(p(0, 0), p(3, 0),
                                    p(3, 3), p(0, 3), p(0, 0)))));

            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1);
            expected.add(doc3);

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
            iter.close();

            iter = getGeoSphereCollection()
                    .find(where("p").intersects(
                            GeoJson.lineString(p(1, 4), p(8, 4))));

            expected.clear();
            expected.add(doc3);
            expected.add(doc4);

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
            iter.close();

        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(byte[])}.
     */
    @Test
    public void testQueryWithLessThanByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 1;
        bytes2[0] = 2;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", bytes2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThan(bytes2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(byte, byte[])}.
     */
    @Test
    public void testQueryWithLessThanByteByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", (byte) 12, bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", (byte) 13, bytes1);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(where("a").lessThan(
                (byte) 13, bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        bytes2[0] = 2;
        iter = myCollection.find(where("a").lessThan((byte) 12, bytes2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(double)}.
     */
    @Test
    public void testQueryWithLessThanDouble() {
        final double d1 = myRandom.nextInt();
        final double d2 = d1 + 0.1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addDouble("a", d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addDouble("a", d2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThan(d2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(int)}.
     */
    @Test
    public void testQueryWithLessThanInt() {
        final int v1 = myRandom.nextInt();
        final int v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(long)}.
     */
    @Test
    public void testQueryWithLessThanLong() {
        final long v1 = myRandom.nextInt();
        final long v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addLong("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanMongoTimestamp(long)}.
     */
    @Test
    public void testQueryWithLessThanMongoTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMongoTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanMongoTimestamp(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(ObjectId)} .
     */
    @Test
    public void testQueryWithLessThanObjectId() {
        // ObjectId's increase in time.
        final ObjectId v1 = new ObjectId();
        final ObjectId v2 = new ObjectId();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addObjectId("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addObjectId("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(byte[])}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 2;
        bytes2[0] = 3;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", bytes2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualTo(bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(byte, byte[])}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToByteByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[bytes1.length];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        bytes1[0] = 1;
        bytes2[0] = 2;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", (byte) 12, bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", (byte) 13, bytes2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualTo((byte) 13, bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        iter = myCollection.find(where("a")
                .lessThanOrEqualTo((byte) 12, bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(double)}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToDouble() {
        final double d1 = myRandom.nextInt();
        final double d2 = d1 + 0.1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addDouble("a", d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addDouble("a", d2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualTo(d1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(int)}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToInt() {
        final int v1 = myRandom.nextInt();
        final int v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(long)}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToLong() {
        final long v1 = myRandom.nextInt();
        final long v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addLong("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#lessThanOrEqualToMongoTimestamp(long)}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToMongoTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMongoTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualToMongoTimestamp(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(ObjectId)} .
     */
    @Test
    public void testQueryWithLessThanOrEqualToObjectId() {
        // ObjectId's increase in time.
        final ObjectId v1 = new ObjectId();
        final ObjectId v2 = new ObjectId();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addObjectId("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addObjectId("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualTo(String)}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToString() {
        final String v1 = "b";
        final String v2 = "c";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addString("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualTo(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualToSymbol(String)}.
     */
    @Test
    public void testQueryWithLessThanOrEqualToSymbol() {
        final String v1 = "b";
        final String v2 = "c";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addSymbol("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addSymbol("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualToSymbol(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanOrEqualToTimestamp(long)}
     * .
     */
    @Test
    public void testQueryWithLessThanOrEqualToTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanOrEqualToTimestamp(v1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThan(String)}.
     */
    @Test
    public void testQueryWithLessThanString() {
        final String v1 = "b";
        final String v2 = "c";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addString("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThan(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanSymbol(String)} .
     */
    @Test
    public void testQueryWithLessThanSymbol() {
        final String v1 = "b";
        final String v2 = "c";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addSymbol("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addSymbol("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanSymbol(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#lessThanTimestamp(long)}.
     */
    @Test
    public void testQueryWithLessThanTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .lessThanTimestamp(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#matches(java.util.regex.Pattern)}
     * .
     */
    @Test
    public void testQueryWithMatches() {
        final Pattern v1 = Pattern.compile("abc.*f");
        final Pattern v2 = Pattern.compile("abc.*def");

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addRegularExpression("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addRegularExpression("a", v2);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addString("a", "abcdef"); // Chosen to match!

        final DocumentBuilder doc4 = BuilderFactory.start();
        doc4.addObjectId("_id", new ObjectId());
        doc4.addString("a", "hello"); // Chosen to not match!

        final DocumentBuilder doc5 = BuilderFactory.start();
        doc5.addObjectId("_id", new ObjectId());
        doc5.addSymbol("a", "abcdef"); // Chosen to match!

        final DocumentBuilder doc6 = BuilderFactory.start();
        doc6.addObjectId("_id", new ObjectId());
        doc6.addSymbol("a", "hello"); // Chosen to not match!

        myCollection.insert(Durability.ACK, doc1, doc2, doc3, doc4, doc5, doc6);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .matches(v1));
        try {
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc3.build());
            expected.add(doc5.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertFalse(iter.hasNext());

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#mod(int, int)}.
     */
    @Test
    public void testQueryWithModWithInt() {
        final int v1 = myRandom.nextInt();
        final int v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a").mod(
                10, v1 % 10));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#mod(long, long)}.
     */
    @Test
    public void testQueryWithModWithLong() {
        final long v1 = myRandom.nextInt();
        final long v2 = v1 + 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addLong("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a").mod(
                100, v1 % 100));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(DocumentAssignable)}.
     */
    @Test
    public void testQueryWithNearDocumentAssignable() {
        final double x = 2.456;
        final double y = 5.234;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addDouble(x + 1).addDouble(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 2).addDouble(y + 2);

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3);

            iter = getGeoSphereCollection().find(
                    where("p").near(GeoJson.point(p(x, y))));

            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(DocumentAssignable, double)}
     * .
     */
    @Test
    public void testQueryWithNearDocumentAssignableDouble() {

        final double x = 5.33;
        final double y = 6.14;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.add("p", GeoJson.point(p(x, y)));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.add("p", GeoJson.point(p(x - 1, y - 1)));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.add("p", GeoJson.point(p(x + 20, y + 20)));

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3);

            iter = getGeoSphereCollection().find(
                    where("p").near(GeoJson.point(p(x, y)),
                            distance(x, y, x + 5, y + 5) + 1));

            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(double, double)}.
     */
    @Test
    public void testQueryWithNearDoubleDouble() {
        final double x = 5.1;
        final double y = 5.1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addDouble(x + 1).addDouble(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 2).addDouble(y + 2);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").near(x, y));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(double, double, double)}.
     */
    @Test
    public void testQueryWithNearDoubleDoubleDouble() {
        final double x = 5.1;
        final double y = 5.1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addDouble(x + 1).addDouble(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 2).addDouble(y + 2);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").near(x, y, 2.3));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(int, int)}.
     */
    @Test
    public void testQueryWithNearIntInt() {
        final int x = 45;
        final int y = 32;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addInteger(x + 1).addInteger(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 2).addInteger(y + 2);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").near(x, y));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(int, int, int)}.
     */
    @Test
    public void testQueryWithNearIntIntInt() {
        final int x = 23;
        final int y = 23;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addInteger(x + 1).addInteger(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 2).addInteger(y + 3);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").near(x, y, 3));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(long, long)}.
     */
    @Test
    public void testQueryWithNearLongLong() {
        final long x = 38;
        final long y = 42;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addLong(x + 1).addLong(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 2).addLong(y + 2);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").near(x, y));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(long, long, long)}.
     */
    @Test
    public void testQueryWithNearLongLongLong() {
        final long x = 17;
        final long y = 23;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addLong(x + 1).addLong(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 2).addLong(y + 3);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").near(x, y, 3));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(DocumentAssignable)}.
     */
    @Test
    public void testQueryWithNearSphereDocumentAssignable() {
        final double x = 5.1;
        final double y = 32.56;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addDouble(x + 1).addDouble(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 2).addDouble(y + 2);

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3);

            iter = getGeoSphereCollection().find(
                    where("p").nearSphere(GeoJson.point(p(x, y))));

            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#nearSphere(DocumentAssignable, double)}.
     */
    @Test
    public void testQueryWithNearSphereDocumentAssignableDouble() {
        final double x = 34.768;
        final double y = 3.456;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.add("p", GeoJson.point(p(x + 1, y + 1)));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.add("p", GeoJson.point(p(x + 2, y + 1)));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.add("p", GeoJson.point(p(x + 20.0, y + 20)));

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3);

            iter = getGeoSphereCollection().find(
                    where("p").nearSphere(GeoJson.point(p(x, y)),
                            distance(x, y, x + 2, y + 2) + 1));

            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(double, double)}.
     */
    @Test
    public void testQueryWithNearSphereDoubleDouble() {
        final double x = 5.1;
        final double y = 23.42;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addDouble(x + 1).addDouble(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 2).addDouble(y + 2);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").nearSphere(x, y));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#nearSphere(double, double, double)}.
     */
    @Test
    public void testQueryWithNearSphereDoubleDoubleDouble() {
        final double x = 23.546;
        final double y = 1.89;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addDouble(x + 1).addDouble(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 20.0).addDouble(y + 20);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").nearSphere(x, y, 0.1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(int, int)}.
     */
    @Test
    public void testQueryWithNearSphereIntInt() {
        final int x = 23;
        final int y = 33;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addInteger(x + 1).addInteger(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 2).addInteger(y + 2);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").nearSphere(x, y));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(int, int, int)}.
     */
    @Test
    public void testQueryWithNearSphereIntIntInt() {
        final int x = 13;
        final int y = 2;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addInteger(x + 1).addInteger(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 50).addInteger(y + 50);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").nearSphere(x, y, 1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(long, long)}.
     */
    @Test
    public void testQueryWithNearSphereLongLong() {
        final long x = 32;
        final long y = 51;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addLong(x + 1).addLong(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 10).addLong(y + 10);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").nearSphere(x, y));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc3.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(long, long, long)}.
     */
    @Test
    public void testQueryWithNearSphereLongLongLong() {
        final long x = 15;
        final long y = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("p").addLong(x + 1).addLong(y + 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 50).addLong(y + 50);

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").nearSphere(x, y, 1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(doc2.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(boolean)}.
     */
    @Test
    public void testQueryWithNotEqualToBoolean() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBoolean("a", true);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBoolean("a", false);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(false));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(byte[])}.
     */
    @Test
    public void testQueryWithNotEqualToByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[myRandom.nextInt(100) + 1];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", bytes2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(bytes2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(byte, byte[])}.
     */
    @Test
    public void testQueryWithNotEqualToByteByteArray() {
        final byte[] bytes1 = new byte[myRandom.nextInt(100) + 1];
        final byte[] bytes2 = new byte[myRandom.nextInt(100) + 1];
        myRandom.nextBytes(bytes1);
        myRandom.nextBytes(bytes2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addBinary("a", (byte) 12, bytes1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", (byte) 13, bytes1);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(where("a").notEqualTo(
                (byte) 13, bytes1));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        myCollection.delete(doc2);
        doc2.reset();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBinary("a", (byte) 12, bytes2);
        myCollection.insert(Durability.ACK, doc2);

        iter = myCollection.find(where("a").notEqualTo((byte) 12, bytes2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(DocumentAssignable)}.
     */
    @Test
    public void testQueryWithNotEqualToDocumentAssignable() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.push("a").addInteger("b", 1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.push("a").addInteger("b", 2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(BuilderFactory.start().addInteger("b", 2)));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(double)}.
     */
    @Test
    public void testQueryWithNotEqualToDouble() {
        final double v1 = Double.longBitsToDouble(myRandom.nextLong());
        final double v2 = Double.longBitsToDouble(myRandom.nextLong());

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addDouble("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addDouble("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(int)}.
     */
    @Test
    public void testQueryWithNotEqualToInt() {
        final int v1 = myRandom.nextInt();
        final int v2 = myRandom.nextInt();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addInteger("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addInteger("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToJavaScript(String)}.
     */
    @Test
    public void testQueryWithNotEqualToJavaScriptString() {
        final String v1 = "a == 1";
        final String v2 = "b == 1";

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addJavaScript("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addJavaScript("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToJavaScript(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#notEqualToJavaScript(String, DocumentAssignable)}
     * .
     */
    @Test
    public void testQueryWithNotEqualToJavaScriptStringDocument() {
        final String v1 = "a == 1";
        final String v2 = "b == 1";
        final Document d1 = BuilderFactory.start().addInteger("a", 1)
                .asDocument();
        final Document d2 = BuilderFactory.start().addInteger("a", 2)
                .asDocument();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addJavaScript("a", v1, d1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addJavaScript("a", v2, d1);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToJavaScript(v2, d1));
        try {
            // Bug in MongoDB? - Scope is being ignored.
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }

        myCollection.delete(doc2);
        doc2.reset();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addJavaScript("a", v1, d2);
        myCollection.insert(Durability.ACK, doc2);

        iter = myCollection.find(where("a").notEqualToJavaScript(v1, d2));
        try {
            // Bug in MongoDB? - Scope is being ignored.
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(long)}.
     */
    @Test
    public void testQueryWithNotEqualToLong() {
        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addLong("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToMaxKey()}.
     */
    @Test
    public void testQueryWithNotEqualToMaxKey() {
        final long v1 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMaxKey("a");

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToMaxKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());

            final Set<Document> received = new HashSet<Document>();

            assertTrue(iter.hasNext());
            received.add(iter.next());

            // For 2.5/2.6 this behavior was fixed and now return just the
            // matching document.
            if (!iter.hasNext()) {
                expected.clear();
                expected.add(doc1.build());
            }
            else {
                received.add(iter.next());
                assertFalse(iter.hasNext());
            }

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToMinKey()}.
     */
    @Test
    public void testQueryWithNotEqualToMinKey() {
        final long v1 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMinKey("a");

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToMinKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());

            // For 2.5/2.6 this behavior was fixed and now return just the
            // matching document.
            if (!iter.hasNext()) {
                expected.clear();
                expected.add(doc1.build());
            }
            else {
                received.add(iter.next());
                assertFalse(iter.hasNext());
            }

            assertEquals(expected, received);
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToMongoTimestamp(long)}.
     */
    @Test
    public void testQueryWithNotEqualToMongoTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addMongoTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addMongoTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToMongoTimestamp(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToNull()}.
     */
    @Test
    public void testQueryWithNotEqualToNull() {
        final long v1 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addLong("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addNull("a");

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToNull());
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(ObjectId)} .
     */
    @Test
    public void testQueryWithNotEqualToObjectId() {
        final ObjectId v1 = new ObjectId();
        final ObjectId v2 = new ObjectId();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addObjectId("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addObjectId("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(Pattern)}.
     */
    @Test
    public void testQueryWithNotEqualToPattern() {
        final Pattern v1 = Pattern.compile("abc.*def");
        final Pattern v2 = Pattern.compile("abc.def");

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addRegularExpression("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addRegularExpression("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        MongoIterator<Document> iter = null;
        try {
            iter = myCollection.find(where("a").notEqualTo(v2));
            // fail("Expect a QueryFailedException."); // Up to 2.5.5
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        catch (final QueryFailedException qfe) {
            // Bug in MongoDB - Discovered fixed in 2.5.5
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualTo(String)}.
     */
    @Test
    public void testQueryWithNotEqualToString() {
        final String v1 = String.valueOf(myRandom.nextDouble());
        final String v2 = String.valueOf(myRandom.nextDouble());

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addString("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToSymbol(String)}.
     */
    @Test
    public void testQueryWithNotEqualToSymbol() {
        final String v1 = String.valueOf(myRandom.nextDouble());
        final String v2 = String.valueOf(myRandom.nextDouble());

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addSymbol("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addSymbol("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToSymbol(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notEqualToTimestamp(long)}.
     */
    @Test
    public void testQueryWithNotEqualToTimestamp() {
        final long v1 = myRandom.nextLong();
        final long v2 = myRandom.nextLong();

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addTimestamp("a", v1);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addTimestamp("a", v2);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notEqualToTimestamp(v2));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#notIn}.
     */
    @Test
    public void testQueryWithNotIn() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", "c");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBoolean("a", true);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addString("a", "b");

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .notIn(constant(true), constant("b")));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#size(int)}.
     */
    @Test
    public void testQueryWithSize() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.pushArray("a").addInteger(1).addString("b").addBoolean(true);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("a").addInteger(1).addBoolean(true);

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a").size(
                3));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#where(String)}.
     */
    @Test
    public void testQueryWithWhere() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.addString("a", "c");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.addBoolean("a", true);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.addString("a", "b");

        myCollection.insert(Durability.ACK, doc1, doc2);

        final MongoIterator<Document> iter = myCollection.find(where("a")
                .where("this.a == 'c'"));
        try {
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(boolean, Point2D, Point2D, Point2D, Point2D[])}
     * .
     */
    @Deprecated
    @Test
    public void testQueryWithWithinBooleanPoint2DPoint2DPoint2DPoint2DArray() {
        final double x1 = 5.1;
        final double y1 = 5.1;
        final double x2 = 4.2;
        final double y2 = 8.2;

        final double minx = Math.min(x1, x2);
        final double maxx = Math.max(x1, x2);
        final double deltax = Math.abs(x1 - x2);
        final double miny = Math.min(y1, y2);
        final double maxy = Math.max(y1, y2);
        final double deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(minx).addDouble(miny);
        ab.pushArray().addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(minx - 1).addDouble(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        // Find on a slightly deformed square
        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(false,
                            new Point2D.Double(minx - 0.5, miny),
                            new Point2D.Double(maxx, miny),
                            new Point2D.Double(maxx, maxy + 0.75),
                            new Point2D.Double(minx, maxy)));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(DocumentAssignable)}.
     */
    @Test
    public void testQueryWithWithinDocumentAssignable() {
        final double x1 = 5.4;
        final double y1 = 3.2;

        final double deltax = 5.234;
        final double x2 = x1 + deltax;
        final double minx = Math.min(x1, x2);
        final double maxx = Math.max(x1, x2);

        final double deltay = 6.238;
        final double y2 = y1 + deltay;
        final double miny = Math.min(y1, y2);
        final double maxy = Math.max(y1, y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.add("p", GeoJson.point(p(minx, miny)));
        // doc1.pushArray("p")
        // .add(GeoJson.point(p(minx, miny)))
        // .add(GeoJson.point(p(minx + (deltax / 2), miny + (deltay / 2))));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.add("p",
                GeoJson.point(p(minx + (deltax / 2.0), miny + (deltay / 2.0))));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.add("p", GeoJson.point(p(minx - 1, miny - 1)));

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3);

            iter = getGeoSphereCollection().find(
                    where("p").within(
                            GeoJson.polygon(Arrays.asList(p(minx, miny),
                                    p(minx, maxy), p(maxx, maxy),
                                    p(maxx, miny), p(minx, miny)))));

            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, double, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinDocumentAssignableBoolean() {
        final double x1 = 16.8;
        final double y1 = 44.1;

        final double deltax = 4.4656;
        final double x2 = x1 + deltax;
        final double minx = Math.min(x1, x2);
        final double maxx = Math.max(x1, x2);

        final double deltay = 7.2343;
        final double y2 = y1 + deltay;
        final double miny = Math.min(y1, y2);
        final double maxy = Math.max(y1, y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        doc1.add("p", GeoJson.point(p(minx, miny)));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.add("p",
                GeoJson.point(p(minx + (deltax / 2.0), miny + (deltay / 2.0))));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.add("p", GeoJson.point(p(minx - 1, miny - 1)));

        MongoIterator<Document> iter = null;
        try {
            // Will create and index the collection if it does not exist.
            getGeoSphereCollection().insert(Durability.ACK, doc1, doc2, doc3);

            iter = getGeoSphereCollection().find(
                    where("p").within(
                            GeoJson.polygon(Arrays.asList(p(minx, miny),
                                    p(minx, maxy), p(maxx, maxy),
                                    p(maxx, miny), p(minx, miny))), false));

            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version prior to 2.4 (no GeoJson support)
            // or after 2.6 (no $uniqueDocs support)

            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(Version.VERSION_2_4));
            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(double, double, double)}.
     */
    @Test
    public void testQueryWithWithinDoubleDoubleDouble() {
        final double x = 12.3;
        final double y = 22.1;
        final double radius = Math.sqrt(4.0 + 4.0);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(x + 1).addDouble(y + 1);
        ab.pushArray().addDouble(x + 2).addDouble(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 3).addDouble(y + 3);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinDoubleDoubleDoubleBoolean() {
        final double x = 5.1;
        final double y = 5.1;
        final double radius = Math.sqrt(4.0 + 4.0) + .1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(x + 1).addDouble(y + 1);
        ab.pushArray().addDouble(x + 2).addDouble(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 3).addDouble(y + 3);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(x, y, radius, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, double)}.
     */
    @Test
    public void testQueryWithWithinDoubleDoubleDoubleDouble() {
        final double x1 = 5.1;
        final double y1 = 5.1;
        final double x2 = 3.4;
        final double y2 = 12.9;

        final double minx = Math.min(x1, x2);
        final double deltax = Math.abs(x1 - x2);
        final double miny = Math.min(y1, y2);
        final double deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(minx).addDouble(miny);
        ab.pushArray().addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(minx - 1).addDouble(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, double, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinDoubleDoubleDoubleDoubleBoolean() {
        final double x1 = 5.1;
        final double y1 = 5.1;
        final double x2 = 16.2;
        final double y2 = 1.4;

        final double minx = Math.min(x1, x2);
        final double deltax = Math.abs(x1 - x2);
        final double miny = Math.min(y1, y2);
        final double deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(minx).addDouble(miny);
        ab.pushArray().addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(minx - 1).addDouble(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(x1, y1, x2, y2, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int)}.
     */
    @Test
    public void testQueryWithWithinIntIntInt() {
        final int x = 3;
        final int y = 3;
        final int radius = (int) Math.ceil(Math.sqrt(4.0 + 4.0));

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addInteger(x + 1).addInteger(y + 1);
        ab.pushArray().addInteger(x + 2).addInteger(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 3).addInteger(y + 3);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinIntIntIntBoolean() {
        final int x = 3;
        final int y = 3;
        final int radius = (int) Math.ceil(Math.sqrt(4.0 + 4.0));

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addInteger(x + 1).addInteger(y + 1);
        ab.pushArray().addInteger(x + 2).addInteger(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 3).addInteger(y + 3);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(x, y, radius, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int, int)}.
     */
    @Test
    public void testQueryWithWithinIntIntIntInt() {
        final int x1 = 3;
        final int y1 = 3;
        final int x2 = 17;
        final int y2 = 32;

        final int minx = Math.min(x1, x2);
        final int deltax = Math.abs(x1 - x2);
        final int miny = Math.min(y1, y2);
        final int deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addInteger(minx).addInteger(miny);
        ab.pushArray().addInteger(minx + (deltax / 2))
                .addInteger(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(minx + (deltax / 2))
                .addInteger(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(minx - 1).addInteger(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(int, int, int, int, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinIntIntIntIntBoolean() {
        final int x1 = 3;
        final int y1 = 3;
        final int x2 = 12;
        final int y2 = 11;

        final int minx = Math.min(x1, x2);
        final int deltax = Math.abs(x1 - x2);
        final int miny = Math.min(y1, y2);
        final int deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addInteger(minx).addInteger(miny);
        ab.pushArray().addInteger(minx + (deltax / 2))
                .addInteger(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(minx + (deltax / 2))
                .addInteger(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(minx - 1).addInteger(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(x1, y1, x2, y2, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(long, long, long)}.
     */
    @Test
    public void testQueryWithWithinLongLongLong() {
        final long x = 5;
        final long y = 5;
        final long radius = (long) Math.ceil(Math.sqrt(4.0 + 4.0));

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addLong(x + 1).addLong(y + 1);
        ab.pushArray().addLong(x + 2).addLong(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 3).addLong(y + 3);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(long, long, long, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinLongLongLongBoolean() {
        final long x = 5;
        final long y = 5;
        final long radius = (long) Math.ceil(Math.sqrt(4.0 + 4.0));

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addLong(x + 1).addLong(y + 1);
        ab.pushArray().addLong(x + 2).addLong(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 3).addLong(y + 3);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(x, y, radius, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(long, long, long, long)}.
     */
    @Test
    public void testQueryWithWithinLongLongLongLong() {
        final long x1 = 5;
        final long y1 = 5;
        final long x2 = 12;
        final long y2 = 13;

        final long minx = Math.min(x1, x2);
        final long deltax = Math.abs(x1 - x2);
        final long miny = Math.min(y1, y2);
        final long deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addLong(minx).addLong(miny);
        ab.pushArray().addLong(minx + (deltax / 2))
                .addLong(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(minx + (deltax / 2))
                .addLong(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(minx - 1).addLong(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(long, long, long, long, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinLongLongLongLongBoolean() {
        final long x1 = 5;
        final long y1 = 5;
        final long x2 = 17;
        final long y2 = 23;

        final long minx = Math.min(x1, x2);
        final long deltax = Math.abs(x1 - x2);
        final long miny = Math.min(y1, y2);
        final long deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addLong(minx).addLong(miny);
        ab.pushArray().addLong(minx + (deltax / 2))
                .addLong(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(minx + (deltax / 2))
                .addLong(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(minx - 1).addLong(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(x1, y1, x2, y2, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(double, double, double)}.
     */
    @Test
    public void testQueryWithWithinOnSphereDoubleDoubleDouble() {
        final double x = 3.546;
        final double y = 3.141;
        final double radius = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(x + 1).addDouble(y + 1);
        ab.pushArray().addDouble(x + 2).addDouble(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 50).addDouble(y + 50);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(double, double, double, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinOnSphereDoubleDoubleDoubleBoolean() {
        final double x = 0.34;
        final double y = 9.98;
        final double radius = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(x + 1).addDouble(y + 1);
        ab.pushArray().addDouble(x + 2).addDouble(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 50).addDouble(y + 50);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").withinOnSphere(x, y, radius, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#withinOnSphere(int, int, int)}.
     */
    @Test
    public void testQueryWithWithinOnSphereIntIntInt() {
        final int x = 3;
        final int y = 7;
        final int radius = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addInteger(x + 1).addInteger(y + 1);
        ab.pushArray().addInteger(x + 2).addInteger(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 50).addInteger(y + 50);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(int, int, int, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinOnSphereIntIntIntBoolean() {
        final int x = 1;
        final int y = 7;
        final int radius = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addInteger(x + 1).addInteger(y + 1);
        ab.pushArray().addInteger(x + 2).addInteger(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addInteger(x + 2).addInteger(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addInteger(x + 50).addInteger(y + 50);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").withinOnSphere(x, y, radius, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for {@link ConditionBuilder#withinOnSphere(long, long, long)}
     * .
     */
    @Test
    public void testQueryWithWithinOnSphereLongLongLong() {
        final long x = 8;
        final long y = 3;
        final long radius = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addLong(x + 1).addLong(y + 1);
        ab.pushArray().addLong(x + 2).addLong(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 50).addLong(y + 50);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        final MongoIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius));
        try {
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(long, long, long, boolean)}.
     */
    @Deprecated
    @Test
    public void testQueryWithWithinOnSphereLongLongLongBoolean() {
        final long x = 1;
        final long y = 2;
        final long radius = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addLong(x + 1).addLong(y + 1);
        ab.pushArray().addLong(x + 2).addLong(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addLong(x + 2).addLong(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addLong(x + 50).addLong(y + 50);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").withinOnSphere(x, y, radius, false));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.5 which removed $uniqueDocs support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    lessThan(GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(double, double, double)}.
     */
    @Test
    public void testQueryWithWithinOnSphereWrapDoesNotWork() {
        final double x = 100;
        final double y = 70;
        final double radius = 1;

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(x + 1).addDouble(y + 1);
        ab.pushArray().addDouble(x + 2).addDouble(y + 2);

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(x + 2).addDouble(y + 1);

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(x + 50).addDouble(y + 50);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").withinOnSphere(x, y, radius));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final QueryFailedException ok) {
            // OK, I guess. Would fail pre-2.5.5.
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(Point2D, Point2D, Point2D, Point2D[])} .
     */
    @Test
    public void testQueryWithWithinPoint2DPoint2DPoint2DPoint2DArray() {
        final double x1 = 15.6;
        final double y1 = 5.1;
        final double x2 = 5.1;
        final double y2 = 17.6;

        final double minx = Math.min(x1, x2);
        final double maxx = Math.max(x1, x2);
        final double deltax = Math.abs(x1 - x2);
        final double miny = Math.min(y1, y2);
        final double maxy = Math.max(y1, y2);
        final double deltay = Math.abs(y1 - y2);

        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addObjectId("_id", new ObjectId());
        final ArrayBuilder ab = doc1.pushArray("p");
        ab.pushArray().addDouble(minx).addDouble(miny);
        ab.pushArray().addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addObjectId("_id", new ObjectId());
        doc2.pushArray("p").addDouble(minx + (deltax / 2))
                .addDouble(miny + (deltay / 2));

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addObjectId("_id", new ObjectId());
        doc3.pushArray("p").addDouble(minx - 1).addDouble(miny - 1);

        final List<Document> expected = new ArrayList<Document>();
        try {
            getGeoCollection().insert(Durability.ACK, doc1);
            expected.add(doc1.build());
        }
        catch (final ReplyException re) {
            // Check if we are talking to a older MongoDB instance
            // That does not support arrays of points (e.g., 1.8.X and before).
            if (!re.getMessage().contains("geo values have to be numbers")) {
                // Humm - Should have worked. Rethrow the error.
                throw re;
            }
        }
        getGeoCollection().insert(Durability.ACK, doc2, doc3);
        expected.add(doc2.build());

        // Find on a slightly deformed square
        MongoIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").within(new Point2D.Double(minx - 0.5, miny),
                            new Point2D.Double(maxx, miny),
                            new Point2D.Double(maxx, maxy + 0.5),
                            new Point2D.Double(minx, maxy)));

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            if (!expected.isEmpty()) {
                assertTrue(iter.hasNext());
                assertTrue(expected.remove(iter.next()));
            }
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        catch (final ServerVersionException sve) {
            // See if a version after to 2.0 which added $polygon support.

            // Check if we are talking to a older MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThanOrEqualTo(GeospatialOperator.POLYGON_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Verifies that the MongoDB iteration over a large collection works as
     * expected.
     */
    @Test
    public void testRestartWithBadCursorIdFails() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(10);
        findBuilder.setLimit(123);

        MongoIterator<Document> iter = collection.find(findBuilder.build());
        int count = 0;
        for (final Document found : iter) {

            assertNotNull(found);

            count += 1;

            // Only read a few documents and then stop.
            iter.stop();
        }
        // Should not have read all of the documents, yet.
        assertNotEquals(findBuilder.build().getLimit(), count);

        // Restart the connections.
        IOUtils.close(myMongo);
        connect();

        final Document goodDoc = iter.asDocument();
        final DocumentBuilder builder = BuilderFactory.start(goodDoc);
        builder.remove(MongoCursorControl.CURSOR_ID_FIELD);
        builder.add(MongoCursorControl.CURSOR_ID_FIELD, 12345678L);

        // Restart the bad iterator.
        iter = myMongo.restart(builder.asDocument());
        try {
            iter.hasNext();
            fail("Should not have found the bogus cursor id.");
        }
        catch (final CursorNotFoundException good) {
            assertThat(good.getMessage(), containsString("12345678"));
        }
        finally {
            // Now cleanup the iterator.
            iter = myMongo.restart(goodDoc);
            for (final Document found : iter) {

                assertNotNull(found);

                count += 1;
            }

            // Now we have read all of the documents.
            assertEquals(findBuilder.build().getLimit(), count);
        }
    }

    /**
     * Verifies that we can save a document without an _id field. This becomes
     * an upsert.
     */
    @Test
    public void testSaveWithId() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("_id", 1).add("f", true);

        // Insert a doc.
        myCollection.save(builder.build());

        assertEquals(builder.build(), myCollection.findOne(builder.build()));
    }

    /**
     * Verifies that we can save a document without an _id field. This becomes
     * an insert.
     */
    @Test
    public void testSaveWithoutId() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("f", true);

        // Insert a doc.
        final Document doc = builder.build();
        myCollection.save(doc); // <== Will inject an _id!

        assertEquals(doc, myCollection.findOne(doc));
    }

    /**
     * Verifies doing a streaming find.
     */
    @Test
    public void testStreamingFind() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(50);
        findBuilder.setLimit((LARGE_COLLECTION_COUNT / 10) + 4);
        final Find find = findBuilder.build();

        final DocumentCallback callback = new DocumentCallback();
        collection.stream(callback, find);

        callback.waitFor(TimeUnit.SECONDS.toMillis(60));

        assertTrue(callback.isTerminated());
        assertFalse(callback.isTerminatedByNull());
        assertFalse(callback.isTerminatedByException());
        assertEquals(find.getLimit(), callback.getCount());
        assertNull(callback.getException());
    }

    /**
     * Verifies doing a streaming find.
     */
    @Test
    @Deprecated
    public void testStreamingFindLegacy() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final MongoCollection collection = largeCollection(myMongo);

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setProjection(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(50);
        findBuilder.setLimit((LARGE_COLLECTION_COUNT / 10) + 4);
        final Find find = findBuilder.build();

        final DocumentCallback callback = new DocumentCallback();
        collection.streamingFind((Callback<Document>) callback, find);

        callback.waitFor(TimeUnit.SECONDS.toMillis(60));

        assertTrue(callback.isTerminated());
        assertTrue(callback.isTerminatedByNull());
        assertFalse(callback.isTerminatedByException());
        assertEquals(find.getLimit(), callback.getCount());
        assertNull(callback.getException());
    }

    /**
     * Verifies the function of the {@link com.allanbank.mongodb.builder.Text
     * text} command.
     * 
     * <pre>
     * <code>
     * > db.collection.find()
     * { "_id" : ObjectId("51376a80602c316554cfe246"), "content" : "Now is the time to drink all of the coffee." }
     * { "_id" : ObjectId("51376a89602c316554cfe247"), "content" : "Now is the time to drink all of the tea!" }
     * { "_id" : ObjectId("51376ab8602c316554cfe248"), "content" : "Coffee is full of magical powers." }
     * > db.collection.runCommand( { "text": "collection" , search: "coffee magic" } )
     * {
     *     "queryDebugString" : "coffe|magic||||||",
     *     "language" : "english",
     *     "results" : [
     *         {
     *             "score" : 2.25,
     *             "obj" : {
     *                 "_id" : ObjectId("51376ab8602c316554cfe248"),
     *                 "content" : "Coffee is full of magical powers."
     *             }
     *         },
     *         {
     *             "score" : 0.625,
     *             "obj" : {
     *                 "_id" : ObjectId("51376a80602c316554cfe246"),
     *                 "content" : "Now is the time to drink all of the coffee."
     *             }
     *         }
     *     ],
     *     "stats" : {
     *         "nscanned" : 3,
     *         "nscannedObjects" : 0,
     *         "n" : 2,
     *         "nfound" : 2,
     *         "timeMicros" : 97
     *     },
     *     "ok" : 1
     * }
     * </code>
     * </pre>
     * 
     * @deprecated Support for the {@code text} command was deprecated in the
     *             2.6 version of MongoDB. Use the
     *             {@link ConditionBuilder#text(String) $text} query operator
     *             instead. This test will not be removed until two releases
     *             after the MongoDB 2.6 release (e.g. 2.10 if the releases are
     *             2.8 and 2.10).
     */
    @Deprecated
    @SuppressWarnings("boxing")
    @Test
    public void testTextSearch() {
        final DocumentBuilder builder = BuilderFactory.start();

        // Some content.
        myCollection.insert(
                Durability.ACK,
                builder.reset().add("content",
                        "Now is the time to drink all of the coffee."));
        myCollection.insert(
                Durability.ACK,
                builder.reset().add("content",
                        "Now is the time to drink all of the tea!"));
        myCollection.insert(
                Durability.ACK,
                builder.reset().add("content",
                        "Coffee is full of magical powers."));

        List<com.allanbank.mongodb.builder.TextResult> results = Collections
                .emptyList();
        try {
            // Need the text index.
            myCollection.createIndex(Index.text("content"));

            results = myCollection.textSearch(
                    com.allanbank.mongodb.builder.Text.builder().searchTerm(
                            "coffee magic")).toList();
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance.
            assumeThat(
                    sve.getActualVersion(),
                    greaterThanOrEqualTo(com.allanbank.mongodb.builder.Text.REQUIRED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }

        assertThat(results.size(), is(2));

        final com.allanbank.mongodb.builder.TextResult first = results.get(0);
        assertThat(first.getDocument().get(StringElement.class, "content"),
                is(new StringElement("content",
                        "Coffee is full of magical powers.")));
        final com.allanbank.mongodb.builder.TextResult second = results.get(1);
        assertThat(second.getDocument().get(StringElement.class, "content"),
                is(new StringElement("content",
                        "Now is the time to drink all of the coffee.")));
    }

    /**
     * Verifies performing updates on documents.
     */
    @Test
    public void testUpdate() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        // Insert (tiny?) documents.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);
            builder.addInteger("i", i);

            myCollection.insert(builder.build());
        }

        // Decrement each documents id.
        final DocumentBuilder update = BuilderFactory.start();
        update.push("$inc").addInteger("i", 1);
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.update(builder.build(), update.build());
        }

        // Now go find each one.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            final Document found = myCollection.findOne(builder.build());
            assertNotNull("" + i, found);
            assertTrue(found.contains("i"));
            assertEquals(new IntegerElement("i", i + 1), found.get("i"));
        }
    }

    /**
     * Verifies that we cannot send a command (count in this case) with a query
     * that is over the maximum size.
     */
    @Test
    public void testUpdateDocumentToLarge() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 1);
        builder.add("bytes", new byte[Client.MAX_DOCUMENT_SIZE]);

        try {
            // Should not get to the point of being submitted.
            myCollection.updateAsync(BuilderFactory.start(), builder);
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertEquals(Client.MAX_DOCUMENT_SIZE, dtle.getMaximumSize());
            assertEquals(builder.build(), dtle.getDocument());
        }
    }

    /**
     * Verifies performing a mass update on all documents with a short write
     * timeout fails.
     */
    @Test
    public void testUpdateDurabilityFails() {
        if (isReplicaSetConfiguration()) {
            // Adjust the configuration to keep the connection count down
            // and let the inserts happen asynchronously.
            myConfig.setDefaultDurability(Durability.ACK);
            myConfig.setMaxConnectionCount(1);

            final Document doc = BuilderFactory.start()
                    .add("_id", new ObjectId()).build();
            myCollection.insert(doc);

            // Increment a field.
            final DocumentBuilder update = BuilderFactory.start();
            update.push("$inc").addInteger("i", 1);

            try {
                myCollection.update(doc, update.build(), true, false,
                        Durability.replicaDurable(
                                15/* replicas we do not have */, 1 /* ms */));
                fail("Durability should have failed.");
            }
            catch (final DurabilityException error) {
                // Good there was a timeout.
                assertThat(
                        error.getMessage(),
                        anyOf(containsString("timeout"),
                                containsString("timed out waiting for slaves"),
                                containsString("waiting for replication timed out")));

                // But the update should have happened.
                final Document found = myCollection.findOne(doc);
                assertThat(found.get("i"), notNullValue());
            }
        }
    }

    /**
     * Verifies the ability to update the collection options.
     */
    @Test
    public void testUpdateOptions() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        myCollection.insert(Durability.ACK, BuilderFactory.start());

        if (!isShardedConfiguration()) {
            try {
                Document result = myCollection.updateOptions(BuilderFactory
                        .start().add("usePowerOf2Sizes", true));

                // 2.6 just returns { ok : 1.0 }
                assertThat(
                        result.get("usePowerOf2Sizes_old"),
                        anyOf(is((Element) new BooleanElement(
                                "usePowerOf2Sizes_old", false)),
                                nullValue(Element.class)));

                result = myCollection.updateOptions(BuilderFactory.start().add(
                        "usePowerOf2Sizes", true));

                // 2.4 returns null.
                assertThat(
                        result.get("usePowerOf2Sizes_old"),
                        anyOf(is((Element) new BooleanElement(
                                "usePowerOf2Sizes_old", true)),
                                nullValue(Element.class)));

                result = myCollection.updateOptions(BuilderFactory.start().add(
                        "usePowerOf2Sizes", false));
                assertEquals(new BooleanElement("usePowerOf2Sizes_old", true),
                        result.get("usePowerOf2Sizes_old"));
            }
            catch (final ServerVersionException sve) {
                // Check for before-2.2 servers.

                // Check if we are talking to a recent MongoDB instance.
                assumeThat(sve.getActualVersion(),
                        greaterThanOrEqualTo(Version.VERSION_2_2));

                // Humm - Should have worked. Rethrow the error.
                throw sve;

            }
        }
    }

    /**
     * Verifies that we cannot send a command (count in this case) with a query
     * that is over the maximum size.
     */
    @Test
    public void testUpdateQueryToLarge() {

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
        myConfig.setMaxConnectionCount(1);

        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("_id", 1);
        builder.add("bytes", new byte[Client.MAX_DOCUMENT_SIZE]);

        try {
            // Should not get to the point of being submitted.
            myCollection.updateAsync(builder, BuilderFactory.start());
            fail("Should have thrown a DocumentToLargeException");
        }
        catch (final DocumentToLargeException dtle) {
            // Good.
            assertEquals(Client.MAX_DOCUMENT_SIZE, dtle.getMaximumSize());
            assertEquals(builder.build(), dtle.getDocument());
        }
    }

    /**
     * Verifies performing updates with $sets and $unsets.
     */
    @Test
    public void testUpdateWithSetAndUnset() {
        // Adjust the configuration to keep the connection count down
        // and get acks for each operation.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        // Insert (tiny?) document.
        final DocumentBuilder builder = BuilderFactory.start();
        builder.addInteger("_id", 1);
        builder.addInteger("i", 2);
        builder.addInteger("j", 3);
        builder.addInteger("k", 4);

        myCollection.insert(builder.build());

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$unset").add("j", 1).add("k", 1);
        update.push("$set").add("i", 999).add("l", 5);

        assertEquals(1L, myCollection.update(where("_id").equals(1), update,
                false, false));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.addInteger("_id", 1);
        expected.addInteger("i", 999);
        expected.addInteger("l", 5);

        assertEquals(expected.build(),
                myCollection.findOne(where("_id").equals(1)));
    }

    /**
     * Verifies the ability to validate a collection.
     */
    @Test
    public void testValidate() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        myCollection.insert(Durability.ACK, BuilderFactory.start());

        Document result = myCollection
                .validate(MongoCollection.ValidateMode.INDEX_ONLY);

        // Pre 2.0 returns a different document format.
        if (result.contains("valid")) {
            assertEquals(new BooleanElement("valid", true), result.get("valid"));
            Element element = result.get("warning");
            if (isShardedConfiguration()) {
                element = result.findFirst("raw", ".*", "warning");
            }
            assertNotNull(element);
        }

        result = myCollection.validate(MongoCollection.ValidateMode.NORMAL);
        // Pre 2.0 returns a different document format.
        if (result.contains("valid")) {
            assertEquals(new BooleanElement("valid", true), result.get("valid"));
            Element element = result.get("warning");
            if (isShardedConfiguration()) {
                element = result.findFirst("raw", ".*", "warning");
            }
            assertNotNull(element);
        }

        result = myCollection.validate(MongoCollection.ValidateMode.FULL);
        // Pre 2.0 returns a different document format.
        if (result.contains("valid")) {
            assertEquals(new BooleanElement("valid", true), result.get("valid"));
            Element element = result.get("warning");
            if (isShardedConfiguration()) {
                element = result.findFirst("raw", ".*", "warning");
            }
            assertNull(element);
        }
    }

    /**
     * Calculates the distance between the two point in km.
     * 
     * @param x1
     *            The first x coordinate.
     * @param y1
     *            The first y coordinate.
     * @param x2
     *            The second x coordinate.
     * @param y2
     *            The second y coordinate.
     * @return The distance in meters.
     */
    protected double distance(final double x1, final double y1,
            final double x2, final double y2) {
        final double R = 6378.137 * 1000; // m - Distance is in meters w/out a
        // datum
        final double dLat = Math.toRadians(x2 - x1);
        final double dLon = Math.toRadians(y2 - y1);
        final double lat1 = Math.toRadians(x1);
        final double lat2 = Math.toRadians(x2);

        final double a = (Math.sin(dLat / 2) * Math.sin(dLat / 2))
                + (Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math
                        .cos(lat2));
        final double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        final double d = R * c;

        return d;
    }

    /**
     * Fails the test.
     * 
     * @param t
     *            The cause of the failure.
     */
    protected void fatal(final Throwable t) {
        final AssertionError error = new AssertionError(t.getMessage());
        error.initCause(t);

        throw error;
    }

    /**
     * Returns a collection with a geospatial 2D index on the 'p' field.
     * 
     * @return The collection with a geospatial 2D index on the 'p' field.
     */
    protected MongoCollection getGeoCollection() {
        if (myGeoCollection == null) {
            myGeoCollection = myDb.getCollection(GEO_TEST_COLLECTION_NAME + "_"
                    + (++ourUniqueId));
            myGeoCollection.createIndex(Index.geo2d("p"));
        }
        return myGeoCollection;
    }

    /**
     * Returns a collection with a geospatial 2Dshpere index on the 'p' field.
     * 
     * @return The collection with a geospatial 2Dshpere index on the 'p' field.
     */
    protected MongoCollection getGeoSphereCollection() {
        if (myGeoSphereCollection == null) {
            myGeoSphereCollection = myDb.getCollection(GEO_TEST_COLLECTION_NAME
                    + "_" + (++ourUniqueId));
            try {
                myGeoSphereCollection.createIndex(Index.geo2dSphere("p"));
            }
            catch (final ServerVersionException sve) {
                // Check if we are talking to a recent MongoDB instance.
                assumeThat(sve.getActualVersion(),
                        greaterThanOrEqualTo(Version.VERSION_2_4));

                // Humm - Should have worked. Rethrow the error.
                throw sve;
            }
        }
        return myGeoSphereCollection;
    }

    /**
     * Returns true when running against a replica set configuration (may be
     * shards of replica sets.
     * 
     * @return True when connecting to a replica set.
     */
    protected boolean isReplicaSetConfiguration() {
        return false;
    }

    /**
     * Returns true when running against a sharded configuration. Not all
     * commands are supported in shared environments, e.g., when connected to a
     * mongos.
     * 
     * @return True when connecting to a mongos.
     */
    protected boolean isShardedConfiguration() {
        return false;
    }

    /**
     * Shards the collection with the specified name.
     * 
     * @param collectionName
     *            The name of the collection to shard.
     */
    protected void shardCollection(final String collectionName) {

        shardCollection(collectionName, Index.asc("_id"));
    }

    /**
     * Shards the collection with the specified name.
     * 
     * @param collectionName
     *            The name of the collection to shard.
     * @param shardKey
     *            The shard key to use.
     */
    protected void shardCollection(final String collectionName,
            final Element shardKey) {

        if (isShardedConfiguration()) {
            myDb.createCollection(collectionName, null);
            myDb.runAdminCommand("enableSharding", myDb.getName(), null);
            final DocumentBuilder options = BuilderFactory.start();

            myDb.getCollection(collectionName).createIndex(shardKey);

            final String fullName = myDb.getName() + "." + collectionName;
            options.push("key").add(shardKey);
            myDb.runAdminCommand("shardCollection", fullName, options);

            if (!Index.hashed(shardKey.getName()).equals(shardKey)) {
                // Add some splits/chunks.
                options.reset().push("middle")
                        .add(shardKey.getName(), new ObjectId());
                myDb.runAdminCommand("split", fullName, options);
                options.reset().push("middle").add(shardKey.getName(), "a");
                myDb.runAdminCommand("split", fullName, options);

                // Add some more chunks and move around the shards.
                int index = 0;
                final MongoCollection shards = myMongo.getDatabase("config")
                        .getCollection("shards");
                for (final Document shard : shards.find(BuilderFactory.start())) {
                    options.reset().push("middle")
                            .add(shardKey.getName(), index);
                    myDb.runAdminCommand("split", fullName, options);

                    options.reset();
                    options.push("find").add(shardKey.getName(), index);
                    options.add(shard.get("_id").withName("to"));
                    myDb.runAdminCommand("moveChunk", fullName, options);

                    index += 1;
                }
            }
        }
    }

    /**
     * DocumentCallback provides a simple callback for testing streaming finds.
     * 
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static final class DocumentCallback implements
            StreamCallback<Document> {

        /** The number of documents received. */
        private int myCount = 0;

        /** The exception if seen. */
        private Throwable myException = null;

        /** True if the callback has been terminated. */
        private boolean myTerminated = false;

        /** True if the callback has been terminated. */
        private boolean myTerminatedByException = false;

        /** True if the callback has been terminated. */
        private boolean myTerminatedByNull = false;

        /**
         * {@inheritDoc}
         */
        @Override
        public synchronized void callback(final Document result) {
            if (result != null) {
                myCount += 1;
            }
            else {
                myTerminatedByNull = true;
                myTerminated = true;
            }
            notifyAll();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to mark the callback as terminated.
         * </p>
         */
        @Override
        public synchronized void done() {
            myTerminated = true;
            notifyAll();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public synchronized void exception(final Throwable thrown) {
            myTerminatedByException = true;
            myTerminated = true;
            myException = thrown;
            notifyAll();
        }

        /**
         * Returns the number of documents received.
         * 
         * @return The number of documents received.
         */
        public synchronized int getCount() {
            return myCount;
        }

        /**
         * Returns the exception if seen.
         * 
         * @return The exception if seen.
         */
        public synchronized Throwable getException() {
            return myException;
        }

        /**
         * Returns true if the callback has been terminated.
         * 
         * @return True if the callback has been terminated.
         */
        public synchronized boolean isTerminated() {
            return myTerminated;
        }

        /**
         * Returns the terminatedByException value.
         * 
         * @return The terminatedByException value.
         */
        public boolean isTerminatedByException() {
            return myTerminatedByException;
        }

        /**
         * Returns the terminatedByNull value.
         * 
         * @return The terminatedByNull value.
         */
        public boolean isTerminatedByNull() {
            return myTerminatedByNull;
        }

        /**
         * Waits for the specified number of documents to be received or the
         * callback to be termined or the timeout.
         * 
         * @param timeMs
         *            The maximum number of milliseconds to wait.
         */
        public void waitFor(final long timeMs) {
            long now = System.currentTimeMillis();
            final long deadline = now + timeMs;

            synchronized (this) {
                while ((now < deadline) && !myTerminated) {
                    try {
                        wait(deadline - now);
                    }
                    catch (final InterruptedException e) {
                        // Handled by while loop.
                    }
                    now = System.currentTimeMillis();
                }
            }
        }
    }

    /**
     * TestIteratorAsyncCallback provides a test callback.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    static class TestIterateInAsyncCallback implements
            Callback<MongoIterator<Document>> {

        /** The number of times the callback methods have been invoked. */
        private int myCalls = 0;

        /** The number of documents received from the iterator. */
        private int myCount = 0;

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to save the iterator and increment the call count.
         * </p>
         */
        @Override
        public synchronized void callback(final MongoIterator<Document> result) {
            while (result.hasNext()) {
                result.next();
                myCount += 1;
            }
            myCalls += 1;
            notifyAll();
        }

        /**
         * Checks the number of times the callback is invoked. Will wait for the
         * first call.
         * 
         * @throws InterruptedException
         *             On a failure to wait.
         */
        public void check() throws InterruptedException {
            synchronized (this) {
                while (myCalls <= 0) {
                    this.wait();
                }
            }

            Thread.sleep(500);

            synchronized (this) {
                if (myCalls > 1) {
                    throw new IllegalArgumentException(
                            "Called more than once: " + myCalls);
                }
            }
        }

        /**
         * Returns number of documents returned by the iterator.
         * 
         * @return The number of documents returned by the iterator.
         */
        public synchronized int count() {
            return myCount;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to increment the called count.
         * </p>
         */
        @Override
        public synchronized void exception(final Throwable thrown) {
            myCalls += 1;
            this.notifyAll();
        }
    }

    /**
     * TestIteratorAsyncCallback provides a test callback.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    static class TestIteratorAsyncCallback implements
            Callback<MongoIterator<Document>> {

        /** The number of times the callback methods have been invoked. */
        private int myCalls = 0;

        /** The iterator provided to the callback. */
        private MongoIterator<Document> myIter;

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to save the iterator and increment the call count.
         * </p>
         */
        @Override
        public void callback(final MongoIterator<Document> result) {
            myIter = result;
            synchronized (this) {
                myCalls += 1;
                this.notifyAll();
            }

        }

        /**
         * Checks the number of times the callback is invoked. Will wait for the
         * first call.
         * 
         * @throws InterruptedException
         *             On a failure to wait.
         */
        public void check() throws InterruptedException {
            synchronized (this) {
                while (myCalls <= 0) {
                    this.wait(TimeUnit.MINUTES.toMillis(1));
                }
            }

            Thread.sleep(500);

            synchronized (this) {
                if (myCalls > 1) {
                    throw new IllegalArgumentException(
                            "Called more than once: " + myCalls);
                }
            }
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to increment the called count.
         * </p>
         */
        @Override
        public void exception(final Throwable thrown) {
            synchronized (this) {
                myCalls += 1;
                this.notifyAll();
            }

        }

        /**
         * Returns the iterator returned.
         * 
         * @return The iterator provided to the callback.
         */
        public MongoIterator<Document> iter() {
            return myIter;
        }
    }
}
