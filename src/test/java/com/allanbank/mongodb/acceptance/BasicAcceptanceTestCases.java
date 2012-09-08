/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.id;
import static com.allanbank.mongodb.builder.AggregationProjectFields.includeWithoutId;
import static com.allanbank.mongodb.builder.QueryBuilder.and;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.Sort.asc;
import static com.allanbank.mongodb.builder.Sort.desc;
import static com.allanbank.mongodb.builder.expression.Expressions.constant;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.set;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.Mongo;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.ElementType;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.builder.Sort;
import com.allanbank.mongodb.error.QueryFailedException;
import com.allanbank.mongodb.error.ReplyException;

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
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class BasicAcceptanceTestCases extends ServerTestDriverSupport {

    /** The name of the test collection to use. */
    public static final String GEO_TEST_COLLECTION_NAME = "geo";

    /** One million - used when we want a large collection of document. */
    public static final int LARGE_COLLECTION_COUNT = 1000000;

    /** One hundred - used when we only need a small collection. */
    public static final int SMALL_COLLECTION_COUNT = 100;

    /** The name of the test collection to use. */
    public static final String TEST_COLLECTION_NAME = "acceptance";

    /** The name of the test database to use. */
    public static final String TEST_DB_NAME = "acceptance_test";

    /** The default collection for the test. */
    protected MongoCollection myCollection = null;

    /** The configuration for the test. */
    protected MongoDbConfiguration myConfig = null;

    /** The default database to use for the test. */
    protected MongoDatabase myDb = null;

    /** The Geospatial collection for the test. */
    protected MongoCollection myGeoCollection = null;

    /** The connection to MongoDB for the test. */
    protected Mongo myMongo = null;

    /** A source of random for the tests. */
    protected Random myRandom = null;

    /**
     * Sets up to create a connection to MongoDB.
     */
    @Before
    public void connect() {
        myConfig = new MongoDbConfiguration();
        myConfig.addServer(new InetSocketAddress("127.0.0.1", DEFAULT_PORT));

        myMongo = MongoFactory.create(myConfig);
        myDb = myMongo.getDatabase(TEST_DB_NAME);
        myCollection = myDb.getCollection(TEST_COLLECTION_NAME);

        myRandom = new Random(System.currentTimeMillis());
    }

    /**
     * Disconnects from MongoDB.
     */
    @After
    public void disconnect() {
        try {
            if (myCollection != null) {
                myCollection.delete(BuilderFactory.start().build(),
                        Durability.ACK);
            }
            if (myGeoCollection != null) {
                myGeoCollection.delete(BuilderFactory.start().build(),
                        Durability.ACK);
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
            myConfig = null;
            myRandom = null;

        }
    }

    /**
     * Verifies the function of Aggregation framework.
     * <p>
     * Using the drivers support classes: <blockquote>
     * 
     * <pre>
     * <code>
     * import static {@link com.allanbank.mongodb.builder.AggregationGroupField#set com.allanbank.mongodb.builder.AggregationGroupField.set};
     * import static {@link com.allanbank.mongodb.builder.AggregationGroupId#id com.allanbank.mongodb.builder.AggregationGroupId.id};
     * import static {@link com.allanbank.mongodb.builder.AggregationProjectFields#includeWithoutId com.allanbank.mongodb.builder.AggregationProjectFields.includeWithoutId};
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

        try {
            final List<Document> results = aggregate.aggregate(builder.build());

            assertEquals(expected, results);
        }
        catch (final ReplyException re) {
            // Check if we are talking to a recent MongoDB instance.
            final String message = re.getMessage();

            assumeTrue(!message.contains("no such cmd: aggregate")
                    && !message.contains("unrecognized command: aggregate"));

            throw re;
        }
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

            assertEquals(i + 1,
                    myCollection.count(BuilderFactory.start().build()));
        }
    }

    /**
     * Tests that an index is successfully created.
     */
    @Test
    public void testCreateIndex() {

        myCollection.createIndex(Sort.asc("foo"), Sort.asc("bar"));

        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.NONE);
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
        findBuilder.setReturnFields(BuilderFactory.start()
                .addBoolean("_id", false).addBoolean("foo", true)
                .addBoolean("bar", true).build());
        final ClosableIterator<Document> iter = myCollection.find(findBuilder
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
                myCollection.count(BuilderFactory.start().build()));

        myConfig.setDefaultDurability(Durability.ACK);
        assertEquals(SMALL_COLLECTION_COUNT,
                myCollection.delete(BuilderFactory.start().build()));

        assertEquals(0, myCollection.count(BuilderFactory.start().build()));

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
        final ArrayElement items = myCollection.distinct(builder.build());

        final Set<String> actual = new HashSet<String>();
        for (final StringElement element : items.queryPath(StringElement.class,
                ".*")) {
            actual.add(element.getValue());
        }

        assertEquals(expected, actual);
    }

    /**
     * Verifies that a collection is removed from the database on a drop.
     */
    @Test
    public void testDropCollection() {
        // Make sure the collection/db exist.
        myCollection.insert(BuilderFactory.start().build());

        assertTrue(myDb.listCollections().contains(TEST_COLLECTION_NAME));

        myCollection.drop();

        assertFalse(myDb.listCollections().contains(TEST_COLLECTION_NAME));
    }

    /**
     * Verifies that a database is removed from the server on a drop.
     */
    @Test
    public void testDropDatabase() {
        // Make sure the collection/db exist.
        myCollection.insert(BuilderFactory.start().build());

        assertTrue(myMongo.listDatabases().contains(TEST_DB_NAME));

        myDb.drop();

        assertFalse(myMongo.listDatabases().contains(TEST_DB_NAME));
    }

    /**
     * Verifies that indexes are properly dropped from the system indexes.
     */
    @Test
    public void testDropIndex() {
        myCollection.createIndex(Sort.asc("foo"), Sort.asc("bar"));

        Document found = myDb.getCollection("system.indexes").findOne(
                BuilderFactory.start()
                        .addRegularExpression("name", ".*foo.*", "").build());
        assertNotNull(found);

        myCollection.dropIndex(Sort.asc("foo"), Sort.asc("bar"));
        found = myDb.getCollection("system.indexes").findOne(
                BuilderFactory.start()
                        .addRegularExpression("name", ".*foo.*", "").build());
        assertNull(found);
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

        final ArrayElement results = myCollection.groupBy(builder.build());

        assertEquals(1, results.getEntries().size());
        final Element entry = results.getEntries().get(0);
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
     * Verifies the ability to list the collections for a database on the
     * server.
     */
    @Test
    public void testListCollections() {
        // Make sure the collection/db exist.
        myCollection.insert(Durability.ACK, BuilderFactory.start().build());

        final Collection<String> names = myDb.listCollections();

        assertTrue(names.contains(TEST_COLLECTION_NAME));
        assertTrue(names.contains("system.indexes"));
    }

    /**
     * Verifies the ability to list the databases on the server.
     */
    @Test
    public void testListDatabases() {
        // Make sure the collection/db exist.
        myCollection.insert(BuilderFactory.start().build());

        assertTrue(myMongo.listDatabases().contains(TEST_DB_NAME));
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
        for (final Document doc : out.find(BuilderFactory.start().build())) {
            actual.add(doc);
        }

        assertEquals(expected, actual);
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

        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.insert(builder.build());
        }

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setReturnFields(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(10);

        final ClosableIterator<Document> iter = myCollection.find(findBuilder
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

        for (int i = 0; i < LARGE_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.insert(builder.build());
        }

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().build());
        findBuilder.setReturnFields(BuilderFactory.start()
                .addBoolean("_id", true).build());
        // Fetch a lot.
        findBuilder.setBatchSize(10);
        findBuilder.setLimit(123);

        final ClosableIterator<Document> iter = myCollection.find(findBuilder
                .build());
        int count = 0;
        for (final Document found : iter) {

            assertNotNull(found);

            count += 1;
        }

        assertEquals(findBuilder.build().getLimit(), count);
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .all(constant(true), constant("b")));
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

        ClosableIterator<Document> iter = myCollection.find(and(where("a")
                .equals(1), where("b").equals(1)));
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .equalsMaxKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());
            expected.add(doc3.build());
            expected.add(doc4.build());
            expected.add(doc5.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .equalsMinKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());
            expected.add(doc3.build());
            expected.add(doc4.build());
            expected.add(doc5.build());

            final Set<Document> received = new HashSet<Document>();
            assertTrue(iter.hasNext());
            received.add(iter.next());
            assertTrue(iter.hasNext());
            received.add(iter.next());
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = null;
        try {
            iter = myCollection.find(where("a").equalsMongoTimestamp(v1));
            iter.hasNext();
            fail("Expected to throw.");
        }
        catch (final QueryFailedException expected) {
            // Bug in MongoDB!
            assertEquals("wrong type for field (a) 17 != 9",
                    expected.getMessage());
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = myCollection
                .find(where("a").exists());
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .in(constant(true), constant("b")));
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = myCollection.find(where("a")
                .lessThan((byte) 13, bytes1));
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .mod(10, v1 % 10));
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .mod(100, v1 % 100));
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
     * Test method for {@link ConditionBuilder#near(double, double)}.
     */
    @Test
    public void testQueryWithNearDoubleDouble() {
        final double x = myRandom.nextDouble() * 170.0;
        final double y = myRandom.nextDouble() * 170.0;

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final double x = myRandom.nextDouble() * 170.0;
        final double y = myRandom.nextDouble() * 170.0;

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final int x = (int) (myRandom.nextDouble() * 170);
        final int y = (int) (myRandom.nextDouble() * 170);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final int x = (int) (myRandom.nextDouble() * 170);
        final int y = (int) (myRandom.nextDouble() * 170);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final long x = (long) (myRandom.nextDouble() * 170);
        final long y = (long) (myRandom.nextDouble() * 170);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final long x = (long) (myRandom.nextDouble() * 170);
        final long y = (long) (myRandom.nextDouble() * 170);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
     * Test method for {@link ConditionBuilder#nearSphere(double, double)}.
     */
    @Test
    public void testQueryWithNearSphereDoubleDouble() {
        final double x = myRandom.nextDouble() * 170.0;
        final double y = myRandom.nextDouble() * 80.0;

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
     * Test method for
     * {@link ConditionBuilder#nearSphere(double, double, double)}.
     */
    @Test
    public void testQueryWithNearSphereDoubleDoubleDouble() {
        final double x = myRandom.nextDouble() * 160.0;
        final double y = myRandom.nextDouble() * 70.0;

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final int x = (int) (myRandom.nextDouble() * 170);
        final int y = (int) (myRandom.nextDouble() * 80);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final int x = (int) (myRandom.nextDouble() * 20);
        final int y = (int) (myRandom.nextDouble() * 20);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final long x = (long) (myRandom.nextDouble() * 170);
        final long y = (long) (myRandom.nextDouble() * 80);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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
        final long x = (long) (myRandom.nextDouble() * 20);
        final long y = (long) (myRandom.nextDouble() * 20);

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

        final ClosableIterator<Document> iter = getGeoCollection().find(
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = myCollection.find(where("a")
                .notEqualTo((byte) 13, bytes1));
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .notEqualToMaxKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());

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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .notEqualToMinKey());
        try {
            // Bug in MongoDB? - Matching all documents.
            final Set<Document> expected = new HashSet<Document>();
            expected.add(doc1.build());
            expected.add(doc2.build());

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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        ClosableIterator<Document> iter = null;
        try {
            iter = myCollection.find(where("a").notEqualTo(v2));
            fail("Expect a QueryFailedException.");
            assertTrue(iter.hasNext());
            assertEquals(doc1.build(), iter.next());
            assertFalse(iter.hasNext());
        }
        catch (final QueryFailedException qfe) {
            // Bug in MongoDB?
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
                .size(3));
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

        final ClosableIterator<Document> iter = myCollection.find(where("a")
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
    @Test
    public void testQueryWithWithinBooleanPoint2DPoint2DPoint2DPoint2DArray() {
        final double x1 = myRandom.nextDouble() * 170.0;
        final double y1 = myRandom.nextDouble() * 170.0;
        final double x2 = myRandom.nextDouble() * 170.0;
        final double y2 = myRandom.nextDouble() * 170.0;

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        // Find on a slightly deformed square
        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(false, new Point2D.Double(minx - 0.5, miny),
                        new Point2D.Double(maxx, miny),
                        new Point2D.Double(maxx, maxy + 0.75),
                        new Point2D.Double(minx, maxy)));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(double, double, double)}.
     */
    @Test
    public void testQueryWithWithinDoubleDoubleDouble() {
        final double x = myRandom.nextDouble() * 170.0;
        final double y = myRandom.nextDouble() * 170.0;
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, boolean)}.
     */
    @Test
    public void testQueryWithWithinDoubleDoubleDoubleBoolean() {
        final double x = myRandom.nextDouble() * 170.0;
        final double y = myRandom.nextDouble() * 170.0;
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, double)}.
     */
    @Test
    public void testQueryWithWithinDoubleDoubleDoubleDouble() {
        final double x1 = myRandom.nextDouble() * 170.0;
        final double y1 = myRandom.nextDouble() * 170.0;
        final double x2 = myRandom.nextDouble() * 170.0;
        final double y2 = myRandom.nextDouble() * 170.0;

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(double, double, double, double, boolean)}.
     */
    @Test
    public void testQueryWithWithinDoubleDoubleDoubleDoubleBoolean() {
        final double x1 = myRandom.nextDouble() * 170.0;
        final double y1 = myRandom.nextDouble() * 170.0;
        final double x2 = myRandom.nextDouble() * 170.0;
        final double y2 = myRandom.nextDouble() * 170.0;

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int)}.
     */
    @Test
    public void testQueryWithWithinIntIntInt() {
        final int x = (int) (myRandom.nextDouble() * 170.0);
        final int y = (int) (myRandom.nextDouble() * 170.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int, boolean)}.
     */
    @Test
    public void testQueryWithWithinIntIntIntBoolean() {
        final int x = (int) (myRandom.nextDouble() * 170.0);
        final int y = (int) (myRandom.nextDouble() * 170.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(int, int, int, int)}.
     */
    @Test
    public void testQueryWithWithinIntIntIntInt() {
        final int x1 = (int) (myRandom.nextDouble() * 170.0);
        final int y1 = (int) (myRandom.nextDouble() * 170.0);
        final int x2 = (int) (myRandom.nextDouble() * 170.0);
        final int y2 = (int) (myRandom.nextDouble() * 170.0);

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(int, int, int, int, boolean)}.
     */
    @Test
    public void testQueryWithWithinIntIntIntIntBoolean() {
        final int x1 = (int) (myRandom.nextDouble() * 170.0);
        final int y1 = (int) (myRandom.nextDouble() * 170.0);
        final int x2 = (int) (myRandom.nextDouble() * 170.0);
        final int y2 = (int) (myRandom.nextDouble() * 170.0);

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(long, long, long)}.
     */
    @Test
    public void testQueryWithWithinLongLongLong() {
        final long x = (long) (myRandom.nextDouble() * 170.0);
        final long y = (long) (myRandom.nextDouble() * 170.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(long, long, long, boolean)}.
     */
    @Test
    public void testQueryWithWithinLongLongLongBoolean() {
        final long x = (long) (myRandom.nextDouble() * 170.0);
        final long y = (long) (myRandom.nextDouble() * 170.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x, y, radius, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#within(long, long, long, long)}.
     */
    @Test
    public void testQueryWithWithinLongLongLongLong() {
        final long x1 = (long) (myRandom.nextDouble() * 170.0);
        final long y1 = (long) (myRandom.nextDouble() * 170.0);
        final long x2 = (long) (myRandom.nextDouble() * 170.0);
        final long y2 = (long) (myRandom.nextDouble() * 170.0);

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#within(long, long, long, long, boolean)}.
     */
    @Test
    public void testQueryWithWithinLongLongLongLongBoolean() {
        final long x1 = (long) (myRandom.nextDouble() * 170.0);
        final long y1 = (long) (myRandom.nextDouble() * 170.0);
        final long x2 = (long) (myRandom.nextDouble() * 170.0);
        final long y2 = (long) (myRandom.nextDouble() * 170.0);

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(x1, y1, x2, y2, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(double, double, double)}.
     */
    @Test
    public void testQueryWithWithinOnSphereDoubleDoubleDouble() {
        final double x = myRandom.nextDouble() * 10.0;
        final double y = myRandom.nextDouble() * 10.0;
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(double, double, double, boolean)}.
     */
    @Test
    public void testQueryWithWithinOnSphereDoubleDoubleDoubleBoolean() {
        final double x = myRandom.nextDouble() * 10.0;
        final double y = myRandom.nextDouble() * 10.0;
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#withinOnSphere(int, int, int)}.
     */
    @Test
    public void testQueryWithWithinOnSphereIntIntInt() {
        final int x = (int) (myRandom.nextDouble() * 10.0);
        final int y = (int) (myRandom.nextDouble() * 10.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(int, int, int, boolean)}.
     */
    @Test
    public void testQueryWithWithinOnSphereIntIntIntBoolean() {
        final int x = (int) (myRandom.nextDouble() * 10.0);
        final int y = (int) (myRandom.nextDouble() * 10.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
        }
    }

    /**
     * Test method for {@link ConditionBuilder#withinOnSphere(long, long, long)}
     * .
     */
    @Test
    public void testQueryWithWithinOnSphereLongLongLong() {
        final long x = (long) (myRandom.nextDouble() * 10.0);
        final long y = (long) (myRandom.nextDouble() * 10.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius));
        try {
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
        finally {
            iter.close();
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#withinOnSphere(long, long, long, boolean)}.
     */
    @Test
    public void testQueryWithWithinOnSphereLongLongLongBoolean() {
        final long x = (long) (myRandom.nextDouble() * 10.0);
        final long y = (long) (myRandom.nextDouble() * 10.0);
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").withinOnSphere(x, y, radius, false));
        try {
            final List<Document> expected = new ArrayList<Document>();
            expected.add(doc1.build());
            expected.add(doc1.build());
            expected.add(doc2.build());

            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertTrue(iter.hasNext());
            assertTrue(expected.remove(iter.next()));
            assertFalse(iter.hasNext());
            assertEquals(0, expected.size());
        }
        finally {
            iter.close();
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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        ClosableIterator<Document> iter = null;
        try {
            iter = getGeoCollection().find(
                    where("p").withinOnSphere(x, y, radius));

            fail("$withinSphere wrapping now works!");
        }
        catch (final QueryFailedException expected) {
            // OK, I guess.
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
        final double x1 = myRandom.nextDouble() * 170.0;
        final double y1 = myRandom.nextDouble() * 170.0;
        final double x2 = myRandom.nextDouble() * 170.0;
        final double y2 = myRandom.nextDouble() * 170.0;

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

        getGeoCollection().insert(Durability.ACK, doc1, doc2, doc3);

        // Find on a slightly deformed square
        final ClosableIterator<Document> iter = getGeoCollection().find(
                where("p").within(new Point2D.Double(minx - 0.5, miny),
                        new Point2D.Double(maxx, miny),
                        new Point2D.Double(maxx, maxy + 0.5),
                        new Point2D.Double(minx, maxy)));
        try {
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
        finally {
            iter.close();
        }
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
     * Returns a collection with a geospatial 2D index on the 'p' field.
     * 
     * @return The collection with a geospatial 2D index on the 'p' field.
     */
    protected MongoCollection getGeoCollection() {
        if (myGeoCollection == null) {
            myGeoCollection = myDb.getCollection(GEO_TEST_COLLECTION_NAME);
            myGeoCollection.createIndex(Sort.geo2d("p"));
        }
        return myGeoCollection;
    }
}
