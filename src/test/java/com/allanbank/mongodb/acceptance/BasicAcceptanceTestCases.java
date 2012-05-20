/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.acceptance;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

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
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.client.MongoImpl;
import com.allanbank.mongodb.commands.Distinct;
import com.allanbank.mongodb.commands.Find;
import com.allanbank.mongodb.commands.FindAndModify;
import com.allanbank.mongodb.commands.GroupBy;
import com.allanbank.mongodb.commands.MapReduce;

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

    /** The connection to MongoDB for the test. */
    protected Mongo myMongo = null;

    /**
     * Sets up to create a connection to MongoDB.
     */
    @Before
    public void connect() {
        myConfig = new MongoDbConfiguration();
        myConfig.addServer(new InetSocketAddress("127.0.0.1", DEFAULT_PORT));

        myMongo = new MongoImpl(myConfig);
        myDb = myMongo.getDatabase(TEST_DB_NAME);
        myCollection = myDb.getCollection(TEST_COLLECTION_NAME);
    }

    /**
     * Disconnects from MongoDB.
     */
    @After
    public void disconnect() {
        try {
            if (myCollection != null) {
                myCollection.delete(BuilderFactory.start().get(),
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
            myConfig = null;
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

            myCollection.insert(builder.get());

            assertEquals(i + 1,
                    myCollection.count(BuilderFactory.start().get()));
        }
    }

    /**
     * Tests that an index is successfully created.
     */
    @Test
    public void testCreateIndex() {
        final LinkedHashMap<String, Integer> keys = new LinkedHashMap<String, Integer>();
        keys.put("foo", Integer.valueOf(1));
        keys.put("bar", Integer.valueOf(1));

        myCollection.createIndex(keys);

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

            myCollection.insert(builder.get());
        }

        // Now go find all of them by the covering index.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().get());
        findBuilder.setReturnFields(BuilderFactory.start()
                .addBoolean("_id", false).addBoolean("foo", true)
                .addBoolean("bar", true).get());
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

            myCollection.insert(builder.get());
        }

        assertEquals(SMALL_COLLECTION_COUNT,
                myCollection.count(BuilderFactory.start().get()));

        myConfig.setDefaultDurability(Durability.ACK);
        assertEquals(SMALL_COLLECTION_COUNT,
                myCollection.delete(BuilderFactory.start().get()));

        assertEquals(0, myCollection.count(BuilderFactory.start().get()));

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

        myCollection.insert(Durability.ACK, doc1.get(), doc2.get(), doc3.get());

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
        myCollection.insert(BuilderFactory.start().get());

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
        myCollection.insert(BuilderFactory.start().get());

        assertTrue(myMongo.listDatabases().contains(TEST_DB_NAME));

        myDb.drop();

        assertFalse(myMongo.listDatabases().contains(TEST_DB_NAME));
    }

    /**
     * Verifies that indexes are properly dropped from the system indexes.
     */
    @Test
    public void testDropIndex() {
        final LinkedHashMap<String, Integer> keys = new LinkedHashMap<String, Integer>();
        keys.put("foo", Integer.valueOf(1));
        keys.put("bar", Integer.valueOf(1));

        myCollection.createIndex(keys);

        Document found = myDb.getCollection("system.indexes").findOne(
                BuilderFactory.start()
                        .addRegularExpression("name", ".*foo.*", "").get());
        assertNotNull(found);

        myCollection.dropIndex(keys);
        found = myDb.getCollection("system.indexes").findOne(
                BuilderFactory.start()
                        .addRegularExpression("name", ".*foo.*", "").get());
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

        myCollection.insert(doc.get());

        final DocumentBuilder query = BuilderFactory.start();
        query.addInteger("_id", 0);

        final DocumentBuilder update = BuilderFactory.start();
        update.push("$inc").addInteger("i", 1);

        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(query.get());
        builder.setUpdate(update.get());
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
     * , http_action: "GET /display/DOCS/Aggregation"
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
     *     "http_action" : "GET /display/DOCS/Aggregation",
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
        doc.addString("http_action", "GET /display/DOCS/Aggregation");

        myCollection.insert(Durability.ACK, doc.get());

        final DocumentBuilder query = BuilderFactory.start();
        query.push("invoked_at.d").addString("$gte", "2009-11")
                .addString("$lt", "2009-12");

        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("http_action"));
        builder.setInitialValue(BuilderFactory.start().addInteger("count", 0)
                .addDouble("total_time", 0.0).get());
        builder.setQuery(query.get());
        builder.setReduceFunction("function(doc, out){ out.count++; out.total_time+=doc.response_time }");
        builder.setFinalizeFunction("function(out){ out.avg_time = out.total_time / out.count }");

        final ArrayElement results = myCollection.groupBy(builder.build());

        assertEquals(1, results.getEntries().size());
        final Element entry = results.getEntries().get(0);
        assertThat(entry, instanceOf(DocumentElement.class));

        final DocumentElement result = (DocumentElement) entry;
        assertEquals(new StringElement("http_action",
                "GET /display/DOCS/Aggregation"), result.get("http_action"));
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

            myCollection.insert(builder.get());
        }

        // Now go find each one.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            final Document found = myCollection.findOne(builder.get());
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
        myCollection.insert(Durability.ACK, BuilderFactory.start().get());

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
        myCollection.insert(BuilderFactory.start().get());

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
        mr.insert(doc1.get(), doc2.get(), doc3.get(), doc4.get());

        final MapReduce.Builder mrBuilder = new MapReduce.Builder();
        mrBuilder.setMapFunction("function() { " + "this.tags.forEach( "
                + "function(z){ " + "emit( z , { count : 1 } ); " + "} ); };");
        mrBuilder.setReduceFunction("function( key , values ){ "
                + "var total = 0; "
                + "for ( var i=0; i<values.length; i++ ) { "
                + "total += values[i].count; } "
                + "return { count : total }; };");
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
        expected.add(expected1.get());
        expected.add(expected2.get());
        expected.add(expected3.get());

        final Set<Document> actual = new HashSet<Document>();
        final MongoCollection out = myDb.getCollection("myoutput");
        for (final Document doc : out.find(BuilderFactory.start().get())) {
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

            myCollection.insert(builder.get());
        }

        // Now go find all of them.
        final Find.Builder findBuilder = new Find.Builder(BuilderFactory
                .start().get());
        findBuilder.setReturnFields(BuilderFactory.start()
                .addBoolean("_id", true).get());

        final ClosableIterator<Document> iter = myCollection.find(findBuilder
                .build());
        int expectedId = 0;
        for (final Document found : iter) {

            assertNotNull(found);

            expectedId += 1;
        }

        assertEquals(LARGE_COLLECTION_COUNT, expectedId);
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

            myCollection.insert(builder.get());
        }

        // Decrement each documents id.
        final DocumentBuilder update = BuilderFactory.start();
        update.push("$inc").addInteger("i", 1);
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.update(builder.get(), update.get());
        }

        // Now go find each one.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            final Document found = myCollection.findOne(builder.get());
            assertNotNull("" + i, found);
            assertTrue(found.contains("i"));
            assertEquals(new IntegerElement("i", i + 1), found.get("i"));
        }
    }
}
