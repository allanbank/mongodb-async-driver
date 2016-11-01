/*
 * #%L
 * ShardedReplicaSetsAcceptanceTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static com.allanbank.mongodb.builder.AggregationGroupField.set;
import static com.allanbank.mongodb.builder.AggregationGroupId.id;
import static com.allanbank.mongodb.builder.AggregationProjectFields.includeWithoutId;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.Sort.asc;
import static com.allanbank.mongodb.builder.Sort.desc;
import static com.allanbank.mongodb.builder.expression.Expressions.field;
import static com.allanbank.mongodb.builder.expression.Expressions.set;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.Index;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.callback.FutureReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.ServerStatus;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.error.QueryFailedException;
import com.allanbank.mongodb.error.ReplyException;
import com.allanbank.mongodb.error.ServerVersionException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * BasicAcceptanceTestCases performs acceptance tests for the driver against a
 * sharded MongoDB configuration.
 * <p>
 * These are not meant to be exhaustive tests of the driver but instead attempt
 * to demonstrate that interactions with the MongoDB processes work.
 * </p>
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedReplicaSetsAcceptanceTest
        extends BasicAcceptanceTestCases {

    /**
     * Starts the sharded server before the tests.
     */
    @BeforeClass
    public static void startServer() {
        startShardedReplicaSets();
        disableBalancer();
        buildLargeCollection();
    }

    /**
     * Stops the servers running in a sharded mode.
     */
    @AfterClass
    public static void stopServer() {
        System.out.println("@AfterClass " + ShardedReplicaSetsAcceptanceTest.class);
        stopShardedReplicaSets();
    }

    /**
     * Creates a connection to MongoDB.
     */
    @Override
    @Before
    public void connect() {
        super.connect();

        shardCollection(myCollection.getName());
    }

    /**
     * Test that a aggregate command for a secondary runs on the secondary.
     */
    @Test
    public void testAggregationOnSecondaries() {
        myConfig.setMaxConnectionCount(1);
        myConfig.setDefaultDurability(Durability.replicaDurable(2, 1000));

        shardCollection("aggregate");

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
        final Aggregate.Builder builder = Aggregate.builder();

        builder.setReadPreference(ReadPreference.SECONDARY);
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

            // Now verify the routing by running the command rapidly and looking
            // for increases in the command counts.
            final int count = 1000;
            final int before = countSecondaryCommands();
            for (int i = 0; i < count; ++i) {
                aggregate.aggregate(builder.build());
            }
            final int after = countSecondaryCommands();
            assertTrue("Should have more than " + count + " commands: "
                    + (after - before), count < (after - before));
        }
        catch (final ServerVersionException sve) {
            // Check if we are talking to a recent MongoDB instance.
            assumeThat(sve.getActualVersion(),
                    greaterThan(Aggregate.REQUIRED_VERSION));

            // Humm - Should have worked. Rethrow the error.
            throw sve;
        }
        finally {
            IOUtils.close(iter);
        }
    }

    /**
     * Verifies counting the number of documents in the collection.
     */
    @Test
    public void testCountOnSecondaries() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.replicaDurable(2, 1000));
        myConfig.setMaxConnectionCount(1);

        // Insert a million (tiny?) documents.
        for (int i = 0; i < SMALL_COLLECTION_COUNT; ++i) {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("_id", i);

            myCollection.insert(builder.build());

            assertEquals(i + 1, myCollection.count(BuilderFactory.start()
                    .build(), ReadPreference.SECONDARY));
        }

        // Now verify the routing by running the command rapidly and looking for
        // increases in the command counts.
        final int count = 1000;
        final int before = countSecondaryCommands();
        for (int i = 0; i < count; ++i) {
            myCollection.count(MongoCollection.ALL, ReadPreference.SECONDARY);
        }
        final int after = countSecondaryCommands();
        assertTrue("Should have more than " + count + " commands: "
                + (after - before), count < (after - before));
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
    public void testDistinctOnSecondaries() {
        final DocumentBuilder doc1 = BuilderFactory.start();
        doc1.addString("zip-code", "10010");

        final DocumentBuilder doc2 = BuilderFactory.start();
        doc2.addString("zip-code", "10010");

        final DocumentBuilder doc3 = BuilderFactory.start();
        doc3.addString("zip-code", "99701");

        myCollection.insert(Durability.replicaDurable(2, 10000), doc1.build(),
                doc2.build(), doc3.build());

        final Set<String> expected = new HashSet<String>();
        expected.add("10010");
        expected.add("99701");

        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("zip-code");
        builder.setReadPreference(ReadPreference.SECONDARY);

        final List<Element> items = myCollection.distinct(builder.build())
                .toList();

        final Set<String> actual = new HashSet<String>();
        for (final Element element : items) {
            actual.add(element.getValueAsString());
        }

        assertEquals(expected, actual);

        // Now verify the routing by running the command rapidly and looking for
        // increases in the command counts.
        final int count = 1000;
        final int before = countSecondaryCommands();
        for (int i = 0; i < count; ++i) {
            myCollection.distinct(builder.build());
        }
        final int after = countSecondaryCommands();
        assertTrue("Should have more than " + count + " commands: "
                + (after - before), count < (after - before));
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to adjust the expectations for the command due to delta's in
     * responses for sharded collections.
     * </p>
     */
    @Test
    @Override
    public void testExplain() {
        // Adjust the configuration to keep the connection count down
        // and let the inserts happen asynchronously.
        myConfig.setDefaultDurability(Durability.ACK);
        myConfig.setMaxConnectionCount(1);

        myCollection.createIndex(Index.asc("a"), Index.asc("b"));

        Document result = myCollection.explain(QueryBuilder.where("a")
                .equals(3).and("b").equals(5));

        List<Element> elements = result.find("queryPlanner", "winningPlan", "shards", ".*", "winningPlan", "inputStage", "inputStage", "indexName");
        assertFalse(elements.isEmpty());
        for (final Element element : elements) {
            assertEquals(new StringElement("indexName", "a_1_b_1"),
                    element);
        }

        result = myCollection.explain(QueryBuilder.where("f").equals(42));

        elements = result.find("queryPlanner", "winningPlan", "shards", ".*", "winningPlan", "inputStage", "stage");
        assertFalse(elements.isEmpty());
        for (final Element element : elements) {
            assertEquals(new StringElement("stage", "COLLSCAN"), element);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to adjust the expectations for the command to fail due to the
     * group command not being supported on sharded collections.
     * </p>
     */
    @Test
    @Override
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

        try {
            myCollection.groupBy(builder.build());
            fail("Not expecting to be able to run 'group' command on sharded collection.");
        }
        catch (final ReplyException expected) {
            assertThat(
                    expected.getMessage(),
                    containsString("can't do command: group on sharded collection"));
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
    public void testGroupByOnSecondaries() {

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
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setKeys(Collections.singleton("http_action"));
        builder.setInitialValue(BuilderFactory.start().addInteger("count", 0)
                .addDouble("total_time", 0.0).build());
        builder.setQuery(query.build());
        builder.setReduceFunction("function(doc, out){ out.count++; out.total_time+=doc.response_time }");
        builder.setFinalizeFunction("function(out){ out.avg_time = out.total_time / out.count }");

        try {
            myCollection.groupBy(builder.build());
            fail("Not expecting to be able to run 'group' command on sharded collection.");
        }
        catch (final ReplyException expected) {
            assertThat(
                    expected.getMessage(),
                    containsString("can't do command: group on sharded collection"));
        }
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
     * > res = db.things.mapReduce(m, r, { out : "inline" } );
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
    public void testMapReduceOnSecondaries() {
        shardCollection("mr");
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

        myConfig.setDefaultDurability(Durability.replicaDurable(2, 10000));
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
        mrBuilder.setOutputType(MapReduce.OutputType.INLINE);
        mrBuilder.setReadPreference(ReadPreference.SECONDARY);

        final Set<Document> actual = new HashSet<Document>(mr.mapReduce(
                mrBuilder.build()).toList());

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

        assertEquals(expected, actual);

        // Now verify the routing by running the command rapidly and looking for
        // increases in the command counts.
        final int count = 1000;
        final int before = countSecondaryCommands();
        final int beforePrimry = countPrimaryCommands();
        for (int i = 0; i < count; ++i) {
            mr.mapReduce(mrBuilder.build());
        }
        final int after = countSecondaryCommands();
        final int afterPrimary = countPrimaryCommands();

        // NOTE: M/R is not routed to secondary via mongos.
        assertTrue("M/R count on secondaries expected to be less than " + count
                + " commands due to mongos limitation: " + (after - before),
                (after - before) < count);
        assertTrue("M/R count on primaries expected to be more than " + count
                + " commands due to mongos limitation: "
                + (afterPrimary - beforePrimry),
                count <= (afterPrimary - beforePrimry));
    }

    /**
     * Test method for {@link ConditionBuilder#near(double, double)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearDoubleDouble() {
        try {
            super.testQueryWithNearDoubleDouble();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(double, double, double)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearDoubleDoubleDouble() {
        try {
            super.testQueryWithNearDoubleDoubleDouble();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(int, int)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearIntInt() {
        try {
            super.testQueryWithNearIntInt();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(int, int, int)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearIntIntInt() {
        try {
            super.testQueryWithNearIntIntInt();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(long, long)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearLongLong() {
        try {
            super.testQueryWithNearLongLong();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#near(long, long, long)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearLongLongLong() {
        try {
            super.testQueryWithNearLongLongLong();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(double, double)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearSphereDoubleDouble() {
        try {
            super.testQueryWithNearSphereDoubleDouble();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for
     * {@link ConditionBuilder#nearSphere(double, double, double)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearSphereDoubleDoubleDouble() {
        try {
            super.testQueryWithNearSphereDoubleDoubleDouble();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(int, int)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearSphereIntInt() {
        try {
            super.testQueryWithNearSphereIntInt();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(int, int, int)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearSphereIntIntInt() {
        try {
            super.testQueryWithNearSphereIntIntInt();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(long, long)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearSphereLongLong() {
        try {
            super.testQueryWithNearSphereLongLong();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Test method for {@link ConditionBuilder#nearSphere(long, long, long)}.
     * <p>
     * Overridden to expect the query to fail on the sharded collection.
     * </p>
     */
    @Test
    @Override
    public void testQueryWithNearSphereLongLongLong() {
        try {
            super.testQueryWithNearSphereLongLongLong();
            fail("Expected $near to fail on a sharded collection.");
        }
        catch (final QueryFailedException qfe) {
            assertThat(
                    qfe.getMessage(),
                    containsString("use geoNear command rather than $near query"));
        }
    }

    /**
     * Returns the number of commands seen on secondaries.
     *
     * @return The number of commands seen on secondaries.
     */
    protected int countPrimaryCommands() {
        int count = 0;
        final SocketConnectionFactory factory = new SocketConnectionFactory(
                myConfig);
        try {
            final InetSocketAddress defaultAddr = createAddress();
            final Cluster cluster = new Cluster(myConfig, ClusterType.SHARDED);
            for (int port = DEFAULT_PORT; port < (DEFAULT_PORT + 50); ++port) {
                Connection conn = null;
                try {
                    conn = factory.connect(cluster.add(new InetSocketAddress(
                            defaultAddr.getHostName(), port)), myConfig);

                    final FutureReplyCallback replyFuture = new FutureReplyCallback();
                    conn.send(new ServerStatus(), replyFuture);

                    final Reply reply = replyFuture.get();
                    assertEquals(1, reply.getResults().size());

                    final Document doc = reply.getResults().get(0);
                    final StringElement process = doc.get(StringElement.class,
                            "process");
                    if ((process != null)
                            && (process.getValue().endsWith("mongod"))) {

                        final BooleanElement primary = doc.findFirst(
                                BooleanElement.class, "repl", "ismaster");

                        if ((primary != null) && primary.getValue()) {
                            final NumericElement countElement = doc.findFirst(
                                    NumericElement.class, "opcounters",
                                    "command");
                            if (countElement != null) {
                                count += countElement.getIntValue();
                            }
                        }

                    }
                }
                catch (final InterruptedException e) {
                    fail(e.getMessage());
                }
                catch (final ExecutionException e) {
                    fail(e.getMessage());
                }
                finally {
                    IOUtils.close(conn);
                }
            }
        }
        catch (final IOException ignore) {
            // OK.
        }
        finally {
            factory.close();
        }

        return count;
    }

    /**
     * Returns the number of commands seen on secondaries.
     *
     * @return The number of commands seen on secondaries.
     */
    protected int countSecondaryCommands() {
        int count = 0;
        final SocketConnectionFactory factory = new SocketConnectionFactory(
                myConfig);
        try {
            final InetSocketAddress defaultAddr = createAddress();
            final Cluster cluster = new Cluster(myConfig, ClusterType.SHARDED);
            for (int port = DEFAULT_PORT; port < (DEFAULT_PORT + 50); ++port) {
                Connection conn = null;
                try {
                    conn = factory.connect(cluster.add(new InetSocketAddress(
                            defaultAddr.getHostName(), port)), myConfig);

                    final FutureReplyCallback replyFuture = new FutureReplyCallback();
                    conn.send(new ServerStatus(), replyFuture);

                    final Reply reply = replyFuture.get();
                    assertEquals(1, reply.getResults().size());

                    final Document doc = reply.getResults().get(0);
                    final StringElement process = doc.get(StringElement.class,
                            "process");
                    if ((process != null)
                            && (process.getValue().endsWith("mongod"))) {

                        final BooleanElement secondary = doc.findFirst(
                                BooleanElement.class, "repl", "secondary");

                        if ((secondary != null) && secondary.getValue()) {
                            final NumericElement countElement = doc.findFirst(
                                    NumericElement.class, "opcounters",
                                    "command");
                            if (countElement != null) {
                                count += countElement.getIntValue();
                            }
                        }

                    }
                }
                catch (final InterruptedException e) {
                    fail(e.getMessage());
                }
                catch (final ExecutionException e) {
                    fail(e.getMessage());
                }
                finally {
                    IOUtils.close(conn);
                }
            }
        }
        catch (final IOException ignore) {
            // OK.
        }
        finally {
            factory.close();
        }

        return count;
    }

    /**
     * Returns a collection with a geospatial 2D index on the 'p' field.
     *
     * @return The collection with a geospatial 2D index on the 'p' field.
     */
    @Override
    protected MongoCollection getGeoCollection() {
        if (myGeoCollection == null) {
            final MongoCollection collection = super.getGeoCollection();
            shardCollection(collection.getName());

            return collection;
        }

        return super.getGeoCollection();
    }

    /**
     * Sets up to create a connection to MongoDB.
     */
    @Override
    protected MongoClientConfiguration initConfig() {
        super.initConfig();

        myConfig.addServer(createAddress());
        myConfig.setAutoDiscoverServers(true);
        myConfig.setMaxConnectionCount(1);
        myConfig.setReconnectTimeout(60000);

        return myConfig;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true.
     * </p>
     */
    @Override
    protected boolean isReplicaSetConfiguration() {
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true.
     * </p>
     */
    @Override
    protected boolean isShardedConfiguration() {
        return true;
    }
}
