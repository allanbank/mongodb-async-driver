/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.bson.impl.ImmutableDocument;
import com.allanbank.mongodb.builder.Aggregation;
import com.allanbank.mongodb.builder.Count;
import com.allanbank.mongodb.builder.Distinct;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.FindAndModify;
import com.allanbank.mongodb.builder.GroupBy;
import com.allanbank.mongodb.builder.Index;
import com.allanbank.mongodb.builder.MapReduce;
import com.allanbank.mongodb.builder.Sort;
import com.allanbank.mongodb.builder.Text;
import com.allanbank.mongodb.builder.TextResult;
import com.allanbank.mongodb.client.callback.CursorCallback;
import com.allanbank.mongodb.client.callback.ReplyArrayCallback;
import com.allanbank.mongodb.client.callback.ReplyDocumentCallback;
import com.allanbank.mongodb.client.callback.ReplyLongCallback;
import com.allanbank.mongodb.client.callback.ReplyResultCallback;
import com.allanbank.mongodb.client.callback.SingleDocumentCallback;
import com.allanbank.mongodb.client.message.AggregationCommand;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.message.Update;
import com.allanbank.mongodb.error.ReplyException;

/**
 * MongoCollectionImplTest provides tests for the {@link MongoCollectionImpl}
 * class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("unchecked")
public class MongoCollectionImplTest {

    /** The address for the test. */
    private String myAddress = null;

    /** The client the collection interacts with. */
    private Client myMockClient = null;

    /** The parent database for the collection. */
    private MongoDatabase myMockDatabase = null;

    /** The instance under test. */
    private AbstractMongoCollection myTestInstance = null;

    /**
     * Creates the base set of objects for the test.
     */
    @Before
    public void setUp() {
        myMockClient = EasyMock.createMock(Client.class);
        myMockDatabase = EasyMock.createMock(MongoDatabase.class);

        myTestInstance = new MongoCollectionImpl(myMockClient, myMockDatabase,
                "test");
        myAddress = "localhost:21017";

        expect(myMockClient.getConfig()).andReturn(
                new MongoClientConfiguration()).anyTimes();
    }

    /**
     * Cleans up the base set of objects for the test.
     */
    @After
    public void tearDown() {
        myMockClient = null;
        myMockDatabase = null;

        myTestInstance = null;
        myAddress = null;
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#aggregate(com.allanbank.mongodb.builder.Aggregate)}
     * .
     */
    @Test
    @Deprecated
    public void testAggregate() {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Collections.singletonList(value.build()),
                myTestInstance.aggregate(builder));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#aggregateAsync(com.allanbank.mongodb.builder.Aggregate)}
     * .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    @Deprecated
    public void testAggregateAsyncAggregate() throws Exception {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Collections.singletonList(value.build()), myTestInstance
                .aggregateAsync(builder).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#aggregateAsync(Callback, com.allanbank.mongodb.builder.Aggregate)}
     * .
     */
    @Test
    @Deprecated
    public void testAggregateAsyncCallbackOfListOfDocumentAggregate() {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#aggregateAsync(LambdaCallback, com.allanbank.mongodb.builder.Aggregate)}
     * method.
     */
    @Deprecated
    @Test
    public void testAggregateAsyncLambdaCallbackAggregate() {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<List<Document>> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#aggregateAsync(LambdaCallback, com.allanbank.mongodb.builder.Aggregate.Builder)}
     * method.
     */
    @Deprecated
    @Test
    public void testAggregateAsyncLambdaCallbackAggregateBuilder() {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<List<Document>> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#aggregateAsync(LambdaCallback, Aggregation)}
     * method.
     */
    @Test
    public void testAggregateAsyncLambdaCallbackAggregation() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<MongoIterator<Document>> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#aggregateAsync(LambdaCallback, Aggregation.Builder)}
     * method.
     */
    @Test
    public void testAggregateAsyncLambdaCallbackAggregationBuilder() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<MongoIterator<Document>> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#aggregateAsync(com.allanbank.mongodb.builder.Aggregate)}
     * .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    @Deprecated
    public void testAggregateWithMaxTime() throws Exception {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);
        builder.maximumTime(10, TimeUnit.SECONDS);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add("maxTimeMS", 10000L);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Collections.singletonList(value.build()), myTestInstance
                .aggregateAsync(builder).get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#aggregateAsync(Callback, com.allanbank.mongodb.builder.Aggregate)}
     * .
     */
    @Test
    @Deprecated
    public void testAggregateWithReadPreference() {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);
        builder.setReadPreference(ReadPreference.PREFER_PRIMARY);

        final com.allanbank.mongodb.builder.Aggregate request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder queryBuilder = expectedCommand.push("$query");
        queryBuilder.addString("aggregate", "test");
        queryBuilder.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_PRIMARY);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PREFER_PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#aggregateAsync(Callback, com.allanbank.mongodb.builder.Aggregate)}
     * .
     */
    @Test
    @Deprecated
    public void testAggregateWithReadPreferenceNonSharded() {
        final com.allanbank.mongodb.builder.Aggregate.Builder builder = new com.allanbank.mongodb.builder.Aggregate.Builder();
        builder.limit(5);
        builder.setReadPreference(ReadPreference.PREFER_PRIMARY);

        final com.allanbank.mongodb.builder.Aggregate request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(builder
                .build().asAggregation(), "test", "test",
                expectedCommand.build(), ReadPreference.PREFER_PRIMARY,
                Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#aggregate(Aggregation)} .
     */
    @Test
    public void testAggregation() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        final MongoIterator<Document> docs = myTestInstance.aggregate(builder);
        assertThat(docs.hasNext(), is(true));
        assertThat(docs.next(), is(value.build()));
        assertThat(docs.hasNext(), is(false));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#aggregateAsync(Aggregation)} .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testAggregationAsyncAggregation() throws Exception {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        final MongoIterator<Document> docs = myTestInstance.aggregateAsync(
                builder).get();
        assertThat(docs.hasNext(), is(true));
        assertThat(docs.next(), is(value.build()));
        assertThat(docs.hasNext(), is(false));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#aggregateAsync(Callback, Aggregation)} .
     */
    @Test
    public void testAggregationAsyncCallbackOfListOfDocumentAggregation() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#aggregateAsync(Aggregation)} .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testAggregationWithMaxTime() throws Exception {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);
        builder.maximumTime(10, TimeUnit.SECONDS);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add("maxTimeMS", 10000L);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_6);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        final MongoIterator<Document> docs = myTestInstance.aggregateAsync(
                builder).get();
        assertThat(docs.hasNext(), is(true));
        assertThat(docs.next(), is(value.build()));
        assertThat(docs.hasNext(), is(false));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#aggregateAsync(Callback, Aggregation)} .
     */
    @Test
    public void testAggregationWithReadPreference() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);
        builder.setReadPreference(ReadPreference.PREFER_PRIMARY);

        final Aggregation request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder queryBuilder = expectedCommand.push("$query");
        queryBuilder.addString("aggregate", "test");
        queryBuilder.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_PRIMARY);

        final AggregationCommand message = new AggregationCommand(request,
                "test", "test", expectedCommand.build(),
                ReadPreference.PREFER_PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#aggregateAsync(Callback, Aggregation)} .
     */
    @Test
    public void testAggregationWithReadPreferenceNonSharded() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);
        builder.setReadPreference(ReadPreference.PREFER_PRIMARY);

        final Aggregation request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PREFER_PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.aggregateAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#buildIndexName(Element...)} .
     */
    @Test
    public void testBuildIndexName() {
        String name = myTestInstance
                .buildIndexName(new DoubleElement("l", 1.2));
        assertEquals("l_1", name);

        name = myTestInstance.buildIndexName(new StringElement("l", "true"));
        assertEquals("l_true", name);

        name = myTestInstance.buildIndexName(new SymbolElement("l", "true"));
        assertEquals("l_true", name);
    }

    /**
     * Test method for {@link AbstractMongoCollection#count()} .
     */
    @Test
    public void testCount() {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.count());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#countAsync()} .
     */
    @Test
    public void testCountAsync() {
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay();

        assertNotNull(myTestInstance.countAsync());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#countAsync(Callback)} .
     */
    @Test
    public void testCountAsyncCallbackOfLong() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(Callback, DocumentAssignable)}
     * .
     */
    @Test
    public void testCountAsyncCallbackOfLongDocument() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#countAsync(Callback, DocumentAssignable, ReadPreference)}
     * .
     */
    @Test
    public void testCountAsyncCallbackOfLongDocumentBoolean() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, doc,
                ReadPreference.SECONDARY);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#countAsync(Callback, ReadPreference)} .
     */
    @Test
    public void testCountAsyncCallbackOfLongReadPreference() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, ReadPreference.SECONDARY);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(DocumentAssignable)} .
     */
    @Test
    public void testCountAsyncDocument() {
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay();

        assertNotNull(myTestInstance.countAsync(doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(DocumentAssignable, ReadPreference)}
     * .
     * 
     * @throws Exception
     *             On an error
     */
    @Test
    public void testCountAsyncDocumentBoolean() throws Exception {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1),
                myTestInstance.countAsync(doc, ReadPreference.SECONDARY).get());

        verify();
    }

    /**
     * Test for the {@link AbstractMongoCollection#countAsync(LambdaCallback)}
     * method.
     */
    @Test
    public void testCountAsyncLambdaCallback() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#countAsync(LambdaCallback, Count)} method.
     */
    @Test
    public void testCountAsyncLambdaCallbackCount() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, Count.builder()
                .readPreference(ReadPreference.SECONDARY).query(doc).build());

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#countAsync(LambdaCallback, Count.Builder)}
     * method.
     */
    @Test
    public void testCountAsyncLambdaCallbackCountBuilder() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, Count.builder()
                .readPreference(ReadPreference.SECONDARY).query(doc));

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#countAsync(LambdaCallback, DocumentAssignable)}
     * method.
     */
    @Test
    public void testCountAsyncLambdaCallbackDocumentAssignable() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#countAsync(LambdaCallback, DocumentAssignable, ReadPreference)}
     * method.
     */
    @Test
    public void testCountAsyncLambdaCallbackDocumentAssignableReadPreference() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, doc,
                ReadPreference.SECONDARY);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#countAsync(LambdaCallback, ReadPreference)}
     * method.
     */
    @Test
    public void testCountAsyncLambdaCallbackReadPreference() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, ReadPreference.SECONDARY);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#countAsync(Count.Builder)}
     * .
     * 
     * @throws Exception
     *             On an error
     */
    @Test
    public void testCountAsyncWithMaxTime() throws Exception {

        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        final Count.Builder builder = Count.builder();
        builder.setQuery(doc);
        builder.setReadPreference(ReadPreference.SECONDARY);
        builder.setMaximumTimeMilliseconds(1234L);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .add("maxTimeMS", 1234L).build(),
                        ReadPreference.SECONDARY)), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1), myTestInstance.countAsync(builder).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(ReadPreference)} .
     * 
     * @throws Exception
     *             On an error
     */
    @Test
    public void testCountAsyncWithOnlyReadPreference() throws Exception {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.push("$query").addString("count", "test")
                .addDocument("query", doc);
        commandDoc.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_SECONDARY);
        final Command command = new Command("test", commandDoc.build(),
                ReadPreference.PREFER_SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(myMockClient.send(eq(command), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1),
                myTestInstance.countAsync(ReadPreference.PREFER_SECONDARY)
                        .get());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#count(DocumentAssignable)}
     * .
     */
    @Test
    public void testCountDocument() {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.PRIMARY)),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.count(doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#count(DocumentAssignable, ReadPreference)}
     * .
     */
    @Test
    public void testCountDocumentBoolean() {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.count(doc, ReadPreference.SECONDARY));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#count(DocumentAssignable, ReadPreference)}
     * .
     */
    @Test
    public void testCountDocumentBooleanOnInterrupt() {
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        callback(Reply.class))).andReturn(myAddress);

        replay();

        try {
            myTestInstance.count(doc, ReadPreference.SECONDARY);
        }
        catch (final MongoDbException error) {
            // Good.
            assertTrue(error.getCause() instanceof InterruptedException);
        }
        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#count(DocumentAssignable, ReadPreference)}
     * .
     */
    @Test
    public void testCountDocumentBooleanOnIOError() {
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        callback(Reply.class, new IOException()))).andReturn(
                myAddress);

        replay();

        try {
            myTestInstance.count(doc, ReadPreference.SECONDARY);
        }
        catch (final MongoDbException error) {
            // Good.
            assertTrue(error.getCause() instanceof IOException);
        }
        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#count(DocumentAssignable, ReadPreference)}
     * .
     */
    @Test
    public void testCountDocumentBooleanOnMongoError() {
        final Document replyDoc = BuilderFactory.start().addInteger("X", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        expect(myMockDatabase.getName()).andReturn("test");

        expect(
                myMockClient.send(eq(new Command("test", BuilderFactory.start()
                        .addString("count", "test").addDocument("query", doc)
                        .build(), ReadPreference.SECONDARY)),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        try {
            myTestInstance.count(doc, ReadPreference.SECONDARY);
        }
        catch (final ReplyException error) {
            // Good.
        }
        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#count(ReadPreference)} .
     * 
     * @throws Exception
     *             On an error
     */
    @Test
    public void testCountWithOnlyReadPreference() throws Exception {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.push("$query").addString("count", "test")
                .addDocument("query", doc);
        commandDoc.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_SECONDARY);
        final Command command = new Command("test", commandDoc.build(),
                ReadPreference.PREFER_SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(myMockClient.send(eq(command), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.count(ReadPreference.PREFER_SECONDARY));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(DocumentAssignable, ReadPreference)}
     * .
     * 
     * @throws Exception
     *             On an error
     */
    @Test
    public void testCountWithReadPreference() throws Exception {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start();
        commandDoc.push("$query").addString("count", "test")
                .addDocument("query", doc);
        commandDoc.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_SECONDARY);
        final Command command = new Command("test", commandDoc.build(),
                ReadPreference.PREFER_SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(myMockClient.send(eq(command), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1),
                myTestInstance.countAsync(doc, ReadPreference.PREFER_SECONDARY)
                        .get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(DocumentAssignable, ReadPreference)}
     * .
     * 
     * @throws Exception
     *             On an error
     */
    @Test
    public void testCountWithReadPreferenceNonSharded() throws Exception {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();
        final Document doc = BuilderFactory.start().build();

        final DocumentBuilder commandDoc = BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc);
        final Command command = new Command("test", commandDoc.build(),
                ReadPreference.PREFER_SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(myMockClient.send(eq(command), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1),
                myTestInstance.countAsync(doc, ReadPreference.PREFER_SECONDARY)
                        .get());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#createIndex(Element...)} .
     */
    @Test
    public void testCreateIndexLinkedHashMapOfStringInteger() {

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "k_1_l_-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(queryMessage),
                        callback(reply(indexDocBuilder.build())))).andReturn(
                myAddress);

        replay();

        myTestInstance.createIndex(Index.asc("k"), Index.desc("l"));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#createIndex(boolean, Element...)} .
     */
    @Test
    public void testCreateIndexLinkedHashMapOfStringIntegerBoolean() {

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "k_1_l_-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);
        indexDocBuilder.addBoolean("unique", true);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(queryMessage),
                        callback(reply(indexDocBuilder.build())))).andReturn(
                myAddress);

        replay();

        myTestInstance.createIndex(true, Index.asc("k"), Index.desc("l"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(String, boolean, Element...)} .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBoolean() {

        final Durability expectedDur = Durability.ACK;
        final GetLastError expectedLastError = new GetLastError("test",
                expectedDur.isWaitForFsync(), expectedDur.isWaitForJournal(),
                expectedDur.getWaitForReplicas(),
                expectedDur.getWaitTimeoutMillis());

        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "name");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(4);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(queryMessage), callback(reply())))
                .andReturn(myAddress);
        expect(
                myMockClient.send(anyObject(Insert.class),
                        eq(expectedLastError), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay();

        myTestInstance.createIndex("name", false, Index.asc("k"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(String, boolean, Element...)} .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBooleanAlreadyExists() {

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "name");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(queryMessage),
                        callback(reply(indexDocBuilder.build())))).andReturn(
                myAddress);

        replay();

        myTestInstance.createIndex("name", false, Index.asc("k"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(String, boolean, Element...)} .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBooleanAlreadyExistsEmptyName() {

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "k_1_l_-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(queryMessage),
                        callback(reply(indexDocBuilder.build())))).andReturn(
                myAddress);

        replay();

        myTestInstance.createIndex("", false, Index.asc("k"), Index.desc("l"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(String, boolean, Element...)} .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBooleanAlreadyExistsNullName() {

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "k_1_l_-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(queryMessage),
                        callback(reply(indexDocBuilder.build())))).andReturn(
                myAddress);

        replay();

        myTestInstance
                .createIndex(null, false, Index.asc("k"), Index.desc("l"));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#createIndex(boolean, Element...)} .
     */
    @Test
    public void testCreateIndexWithOptions() {

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "k_1_l_-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);
        indexDocBuilder.addBoolean("unique", true);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(queryMessage),
                        callback(reply(indexDocBuilder.build())))).andReturn(
                myAddress);

        replay();

        myTestInstance.createIndex(
                AbstractMongoCollection.UNIQUE_INDEX_OPTIONS, Index.asc("k"),
                Index.desc("l"));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#createIndex(boolean, Element...)} .
     */
    @Test
    public void testCreateIndexWithUniqueFalse() {

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "k_1_l_-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(queryMessage),
                        callback(reply(indexDocBuilder.build())))).andReturn(
                myAddress);

        replay();

        myTestInstance.createIndex(false, Index.asc("k"), Index.desc("l"));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(String, boolean, Element...)} .
     */
    @Test
    public void testCreateIndexWithVersionFor2DShpere() {

        final Durability expectedDur = Durability.ACK;
        final GetLastError expectedLastError = new GetLastError("test",
                expectedDur.isWaitForFsync(), expectedDur.isWaitForJournal(),
                expectedDur.getWaitForReplicas(),
                expectedDur.getWaitTimeoutMillis());

        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "name");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").add("k", Index.GEO_2DSPHERE_INDEX_NAME);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(4);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(queryMessage), callback(reply())))
                .andReturn(myAddress);

        final Capture<Insert> insert = new Capture<Insert>();
        expect(
                myMockClient.send(capture(insert), eq(expectedLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        myTestInstance.createIndex("name", false, Index.geo2dSphere("k"));

        assertThat(insert.getValue().getRequiredServerVersion(),
                is(Version.VERSION_2_4));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(String, boolean, Element...)} .
     */
    @Test
    public void testCreateIndexWithVersionForHashed() {

        final Durability expectedDur = Durability.ACK;
        final GetLastError expectedLastError = new GetLastError("test",
                expectedDur.isWaitForFsync(), expectedDur.isWaitForJournal(),
                expectedDur.getWaitForReplicas(),
                expectedDur.getWaitTimeoutMillis());

        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "name");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").add("k", Index.HASHED_INDEX_NAME);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(4);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(queryMessage), callback(reply())))
                .andReturn(myAddress);

        final Capture<Insert> insert = new Capture<Insert>();
        expect(
                myMockClient.send(capture(insert), eq(expectedLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        myTestInstance.createIndex("name", false, Index.hashed("k"));

        assertThat(insert.getValue().getRequiredServerVersion(),
                is(Version.VERSION_2_4));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(String, boolean, Element...)} .
     */
    @Test
    public void testCreateIndexWithVersionForText() {

        final Durability expectedDur = Durability.ACK;
        final GetLastError expectedLastError = new GetLastError("test",
                expectedDur.isWaitForFsync(), expectedDur.isWaitForJournal(),
                expectedDur.getWaitForReplicas(),
                expectedDur.getWaitTimeoutMillis());

        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "name");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").add("k", Index.TEXT_INDEX_NAME);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.build(), null, 1 /* batchSize */,
                1 /* limit */, 0 /* skip */, false /* tailable */,
                ReadPreference.PRIMARY, false /* noCursorTimeout */,
                false /* awaitData */, false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(4);

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(queryMessage), callback(reply())))
                .andReturn(myAddress);

        final Capture<Insert> insert = new Capture<Insert>();
        expect(
                myMockClient.send(capture(insert), eq(expectedLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        myTestInstance.createIndex("name", false, Index.text("k"));

        assertThat(insert.getValue().getRequiredServerVersion(),
                is(Version.VERSION_2_4));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Callback, DocumentAssignable)}
     * .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocument() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getDurability()).andReturn(Durability.NONE);

        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        mockCountCallback.callback(Long.valueOf(-1L));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Callback, DocumentAssignable, boolean)}
     * .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocumentBoolean() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc, true);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#deleteAsync(Callback, DocumentAssignable, boolean, Durability)}
     * .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocumentBooleanDurability() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance
                .deleteAsync(mockCountCallback, doc, true, Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Callback, DocumentAssignable, Durability)}
     * .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocumentDurability() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        mockCountCallback.callback(Long.valueOf(-1L));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc, Durability.NONE);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(DocumentAssignable)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocument() throws Exception {

        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1), myTestInstance.deleteAsync(doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(DocumentAssignable, boolean)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocumentBoolean() throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1), myTestInstance.deleteAsync(doc, true)
                .get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(DocumentAssignable, boolean, Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocumentBooleanDurability() throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1),
                myTestInstance.deleteAsync(doc, true, Durability.ACK).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(DocumentAssignable, Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocumentDurability() throws Exception {
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(-1L),
                myTestInstance.deleteAsync(doc, Durability.NONE).get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#deleteAsync(LambdaCallback, DocumentAssignable)}
     * method.
     */
    @Test
    public void testDeleteAsyncLambdaCallbackDocumentAssignable() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getDurability()).andReturn(Durability.NONE);

        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        mockCountCallback.accept(null, Long.valueOf(-1L));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#deleteAsync(LambdaCallback, DocumentAssignable, boolean)}
     * method.
     */
    @Test
    public void testDeleteAsyncLambdaCallbackDocumentAssignableB() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc, true);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#deleteAsync(LambdaCallback, DocumentAssignable, boolean, Durability)}
     * method.
     */
    @Test
    public void testDeleteAsyncLambdaCallbackDocumentAssignableBDurability() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance
                .deleteAsync(mockCountCallback, doc, true, Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#deleteAsync(LambdaCallback, DocumentAssignable, Durability)}
     * method.
     */
    @Test
    public void testDeleteAsyncLambdaCallbackDocumentAssignableDurability() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        mockCountCallback.accept(null, Long.valueOf(-1L));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc, Durability.NONE);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#delete(DocumentAssignable)} .
     */
    @Test
    public void testDeleteDocument() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.delete(doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#delete(DocumentAssignable, boolean)} .
     */
    @Test
    public void testDeleteDocumentBoolean() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1, myTestInstance.delete(doc, true));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#delete(DocumentAssignable, boolean, Durability)}
     * .
     */
    @Test
    public void testDeleteDocumentBooleanDurability() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.delete(doc, true, Durability.ACK));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#delete(DocumentAssignable, Durability)} .
     */
    @Test
    public void testDeleteDocumentDurability() {
        final Document doc = BuilderFactory.start().build();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        replay();

        assertEquals(-1L, myTestInstance.delete(doc, Durability.NONE));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#distinct(Distinct)} .
     */
    @Test
    public void testDistinct() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setQuery(BuilderFactory.start().build());

        final Distinct request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder values = result.pushArray("values");
        values.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(result.build().find(ArrayElement.class, "values").get(0),
                myTestInstance.distinct(builder));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#distinctAsync(Callback, Distinct)} .
     */
    @Test
    public void testDistinctAsyncCallbackOfArrayElementDistinct() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setQuery(BuilderFactory.start().build());

        final Distinct request = builder.build();

        final Callback<ArrayElement> mockCountCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, builder);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#distinctAsync(Distinct)} .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testDistinctAsyncDistinct() throws Exception {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setQuery(BuilderFactory.start().build());

        final Distinct request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder values = result.pushArray("values");
        values.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(result.build().find(ArrayElement.class, "values").get(0),
                myTestInstance.distinctAsync(builder).get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#distinctAsync(LambdaCallback, Distinct)}
     * method.
     */
    @Test
    public void testDistinctAsyncLambdaCallbackDistinct() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setQuery(BuilderFactory.start().build());

        final Distinct request = builder.build();

        final LambdaCallback<ArrayElement> mockCountCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, builder.build());

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#distinctAsync(LambdaCallback, Distinct.Builder)}
     * method.
     */
    @Test
    public void testDistinctAsyncLambdaCallbackDistinctBuilder() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setQuery(BuilderFactory.start().build());

        final Distinct request = builder.build();

        final LambdaCallback<ArrayElement> mockCountCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, builder);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#distinctAsync(Callback, Distinct)} .
     */
    @Test
    public void testDistinctAsyncNoQuery() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");

        final Callback<ArrayElement> mockCountCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, builder);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#distinctAsync(Distinct)} .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testDistinctWithMaxTime() throws Exception {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setQuery(BuilderFactory.start().build());

        builder.maximumTime(30, TimeUnit.SECONDS);

        final Distinct request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder values = result.pushArray("values");
        values.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.add("maxTimeMS", request.getMaximumTimeMilliseconds());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(result.build().find(ArrayElement.class, "values").get(0),
                myTestInstance.distinctAsync(builder).get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#distinctAsync(Callback, Distinct)} .
     */
    @Test
    public void testDistinctWithReadPreference() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setReadPreference(ReadPreference.CLOSEST);

        final Callback<ArrayElement> mockCountCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.push("$query").addString("distinct", "test")
                .addString("key", "foo");
        expectedCommand.add(ReadPreference.FIELD_NAME, ReadPreference.CLOSEST);

        final Command message = new Command("test", expectedCommand.build(),
                ReadPreference.CLOSEST);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, builder);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#distinctAsync(Callback, Distinct)} .
     */
    @Test
    public void testDistinctWithReadPreferenceNonSharded() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");
        builder.setReadPreference(ReadPreference.CLOSEST);

        final Callback<ArrayElement> mockCountCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");

        final Command message = new Command("test", expectedCommand.build(),
                ReadPreference.CLOSEST);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.STAND_ALONE);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, builder);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link MongoCollectionImpl#drop()}.
     */
    @Test
    public void testDrop() {

        final Document goodResult = BuilderFactory.start().addDouble("ok", 1.0)
                .build();
        final Document badResult = BuilderFactory.start().addLong("ok", 0)
                .build();
        final Document missingOkResult = BuilderFactory.start().build();

        expect(myMockDatabase.runCommand("drop", "test", null)).andReturn(
                goodResult);
        expect(myMockDatabase.runCommand("drop", "test", null)).andReturn(
                badResult);
        expect(myMockDatabase.runCommand("drop", "test", null)).andReturn(
                missingOkResult);

        replay();

        assertTrue(myTestInstance.drop());
        assertFalse(myTestInstance.drop());
        assertFalse(myTestInstance.drop());

        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#dropIndex(String)} .
     */
    @Test
    public void testDropIndex() {
        final Document goodResult = BuilderFactory.start().addDouble("ok", 1.0)
                .build();
        final Document badResult = BuilderFactory.start().addLong("ok", 0)
                .build();
        final Document missingOkResult = BuilderFactory.start().build();
        final Document options = BuilderFactory.start()
                .addString("index", "foo").build();

        expect(myMockDatabase.runCommand("deleteIndexes", "test", options))
                .andReturn(goodResult);
        expect(myMockDatabase.runCommand("deleteIndexes", "test", options))
                .andReturn(badResult);
        expect(myMockDatabase.runCommand("deleteIndexes", "test", options))
                .andReturn(missingOkResult);
        expect(
                myMockDatabase.runCommand("deleteIndexes", "test",
                        BuilderFactory.start().addString("index", "f_1")
                                .build())).andReturn(goodResult);

        replay();

        assertTrue(myTestInstance.dropIndex("foo"));
        assertFalse(myTestInstance.dropIndex("foo"));
        assertFalse(myTestInstance.dropIndex("foo"));
        assertTrue(myTestInstance.dropIndex(Index.asc("f")));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#explain(Aggregation)} .
     */
    @Test
    public void testExplainAggregation() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add("explain", true);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        final Document doc = myTestInstance.explain(builder);
        assertThat(doc, is(reply(result.build()).getResults().get(0)));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#explainAsync(Aggregation)}
     * .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testExplainAggregationAsync() throws Exception {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add("explain", true);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        final Document doc = myTestInstance.explainAsync(builder).get();
        assertThat(doc, is(reply(result.build()).getResults().get(0)));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#explainAsync(Callback, Aggregation)} .
     */
    @Test
    public void testExplainAggregationAsyncCallback() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final Callback<Document> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add("explain", true);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.explainAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#explainAsync(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testExplainAsyncFind() throws Exception {
        final Document result1 = BuilderFactory.start().build();

        final Document query = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().add("$query", query)
                .add("$explain", true).build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.SECONDARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(query);
        findBuilder.setReadPreference(ReadPreference.SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result1))))
                .andReturn(myAddress);

        replay();

        final Future<Document> future = myTestInstance
                .explainAsync(findBuilder);
        assertSame(result1, future.get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#explainAsync(LambdaCallback, Aggregation)}
     * method.
     */
    @Test
    public void testExplainAsyncLambdaCallbackAggregation() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add("explain", true);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.explainAsync(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#explainAsync(LambdaCallback, Aggregation.Builder)}
     * method.
     */
    @Test
    public void testExplainAsyncLambdaCallbackAggregationBuilder() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);
        expectedCommand.add("explain", true);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.explainAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#explainAsync(LambdaCallback, Find)}
     * method.
     */
    @Test
    public void testExplainAsyncLambdaCallbackFind() {
        final Document query = BuilderFactory.start().build();

        final Find.Builder builder = new Find.Builder();
        builder.query(query);

        final Document doc = BuilderFactory.start().add("$query", query)
                .add("$explain", true).build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.SECONDARY, false, false, false, false);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.SECONDARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.explainAsync(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#explainAsync(LambdaCallback, Find.Builder)}
     * method.
     */
    @Test
    public void testExplainAsyncLambdaCallbackFindBuilder() {
        final Document query = BuilderFactory.start().build();

        final Find.Builder builder = new Find.Builder();
        builder.query(query);

        final Document doc = BuilderFactory.start().add("$query", query)
                .add("$explain", true).build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.SECONDARY, false, false, false, false);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.SECONDARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.explainAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#explain(DocumentAssignable)} .
     */
    @Test
    public void testExplainDocument() {
        final Document result1 = BuilderFactory.start().build();

        final Document query = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().add("$query", query)
                .add("$explain", true).build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result1))))
                .andReturn(myAddress);

        replay();

        final Document iter = myTestInstance.explain(query);
        assertSame(result1, iter);

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#explain(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testExplainFind() throws Exception {
        final Document result1 = BuilderFactory.start().build();

        final Document query = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().add("$query", query)
                .add("$explain", true).build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.SECONDARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(query);
        findBuilder.setReadPreference(ReadPreference.SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result1))))
                .andReturn(myAddress);

        replay();

        final Document result = myTestInstance.explain(findBuilder);
        assertSame(result1, result);

        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#explainAsync(Callback, Find)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testExplainWithNonLegacyOptions() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder.build());
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.add("$explain", true);
        qRequestBuilder.addDocument("$readPreference",
                ReadPreference.PREFER_SECONDARY.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.asDocument(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final Future<Document> future = myTestInstance.explainAsync(builder);
        assertSame(result1, future.get());

        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#explainAsync(Callback, Find)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testExplainWithNonLegacyOptionsAndNonShardedAndCallback()
            throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder.build());
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.add("$explain", true);

        final Query message = new Query("test", "test",
                qRequestBuilder.asDocument(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.STAND_ALONE);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.callback(result1);
        expectLastCall();

        replay(mockCallback);

        myTestInstance.explainAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModify() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModify(builder));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModifyAsRemove() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setRemove(true);

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.add("remove", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModify(builder));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#findAndModifyAsync(Callback, FindAndModify)} .
     */
    @Test
    public void testFindAndModifyAsyncCallbackOfDocumentFindAndModify() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());

        final FindAndModify request = builder.build();

        final Callback<Document> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.findAndModifyAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModifyAsync(FindAndModify)} .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testFindAndModifyAsyncFindAndModify() throws Exception {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModifyAsync(builder)
                .get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAndModifyAsync(LambdaCallback, FindAndModify)}
     * method.
     */
    public void testFindAndModifyAsyncLambdaCallbackFindAndModify() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());

        final FindAndModify request = builder.build();

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.findAndModifyAsync(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAndModifyAsync(LambdaCallback, FindAndModify.Builder)}
     * method.
     */
    @Test
    public void testFindAndModifyAsyncLambdaCallbackFindAndModifyBuilder() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());

        final FindAndModify request = builder.build();

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.findAndModifyAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModifyWithFields() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());
        builder.setFields(BuilderFactory.start().add("f", 1));

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());
        expectedCommand.addDocument("fields", BuilderFactory.start()
                .add("f", 1));

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModify(builder));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModifyWithMaxTime() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());

        builder.maximumTime(1, TimeUnit.DAYS);

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());
        expectedCommand.add("maxTimeMS", request.getMaximumTimeMilliseconds());

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModify(builder));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModifyWithNew() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());
        builder.setReturnNew(true);

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());
        expectedCommand.add("new", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModify(builder));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModifyWithSort() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());
        builder.setSort(Sort.asc("f"));

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());
        expectedCommand.addDocument("sort", BuilderFactory.start().add("f", 1));

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModify(builder));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModifyWithUpsert() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().build());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).build());
        builder.setUpsert(true);

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());
        expectedCommand.add("upsert", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(value.build(), myTestInstance.findAndModify(builder));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAsync(Callback, DocumentAssignable)} .
     */
    @Test
    public void testFindAsyncCallbackDocument() {
        final Callback<MongoIterator<Document>> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), anyObject(CursorCallback.class)))
                .andReturn(myAddress);

        replay(mockCountCallback);

        myTestInstance.findAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#findAsync(Callback, Find)}
     * .
     */
    @Test
    public void testFindAsyncCallbackFind() {
        final Callback<MongoIterator<Document>> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), anyObject(CursorCallback.class)))
                .andReturn(myAddress);

        replay(mockCountCallback);

        myTestInstance.findAsync(mockCountCallback, findBuilder);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAsync(DocumentAssignable)}.
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindAsyncDocument() throws Exception {

        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final Future<MongoIterator<Document>> future = myTestInstance
                .findAsync(doc);
        final MongoIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#findAsync(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindAsyncFind() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.SECONDARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);
        findBuilder.setReadPreference(ReadPreference.SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final Future<MongoIterator<Document>> future = myTestInstance
                .findAsync(findBuilder);
        final MongoIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#findAsync(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindAsyncFindTailable() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                true, ReadPreference.SECONDARY, false, true, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);
        findBuilder.setReadPreference(ReadPreference.SECONDARY);
        findBuilder.tailable();

        expect(myMockDatabase.getName()).andReturn("test");
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final Future<MongoIterator<Document>> future = myTestInstance
                .findAsync(findBuilder);
        final MongoIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAsync(LambdaCallback, DocumentAssignable)}
     * method.
     */
    @Test
    public void testFindAsyncLambdaCallbackDocumentAssignable() {
        final LambdaCallback<MongoIterator<Document>> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), anyObject(CursorCallback.class)))
                .andReturn(myAddress);

        replay(mockCountCallback);

        myTestInstance.findAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAsync(LambdaCallback, Find)} method.
     */
    @Test
    public void testFindAsyncLambdaCallbackFind() {
        final LambdaCallback<MongoIterator<Document>> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), anyObject(CursorCallback.class)))
                .andReturn(myAddress);

        replay(mockCountCallback);

        myTestInstance.findAsync(mockCountCallback, findBuilder.build());

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAsync(LambdaCallback, Find.Builder)}
     * method.
     */
    @Test
    public void testFindAsyncLambdaCallbackFindBuilder() {
        final LambdaCallback<MongoIterator<Document>> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), anyObject(CursorCallback.class)))
                .andReturn(myAddress);

        replay(mockCountCallback);

        myTestInstance.findAsync(mockCountCallback, findBuilder);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#find(DocumentAssignable)}
     * .
     */
    @Test
    public void testFindDocument() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final MongoIterator<Document> iter = myTestInstance.find(doc);
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#find(Find)} .
     */
    @Test
    public void testFindDocumentBoolean() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);
        findBuilder.setReadPreference(ReadPreference.PRIMARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final MongoIterator<Document> iter = myTestInstance.find(findBuilder);
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findOne(DocumentAssignable)} .
     */
    @Test
    public void testFindOne() {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("foo", 2)
                .build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay(mockCountCallback);

        assertSame(replyDoc, myTestInstance.findOne(doc));

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findOneAsync(Callback, Find)} .
     */
    @Test
    public void testFindOneAsyncCallbackFind() {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(SingleDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.findOneAsync(mockCountCallback, findBuilder);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#findOneAsync(Callback, DocumentAssignable)} .
     */
    @Test
    public void testFindOneAsyncCallbackOfDocumentDocument() {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(SingleDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.findOneAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findOneAsync(DocumentAssignable)}.
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindOneAsyncDocument() throws Exception {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("foo", 2)
                .build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay(mockCountCallback);

        assertSame(replyDoc, myTestInstance.findOneAsync(doc).get());

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#findOneAsync(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindOneAsyncFind() throws Exception {
        final Document result1 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.SECONDARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);
        findBuilder.setReadPreference(ReadPreference.SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.send(eq(message), callback(reply(result1))))
                .andReturn(myAddress);

        replay();

        final Future<Document> future = myTestInstance
                .findOneAsync(findBuilder);
        assertSame(result1, future.get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAsync(LambdaCallback, DocumentAssignable)}
     * method.
     */
    @Test
    public void testFindOneAsyncLambdaCallbackDocumentAssignable() {
        final LambdaCallback<Document> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(SingleDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.findOneAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAsync(LambdaCallback, Find)} method.
     */
    @Test
    public void testFindOneAsyncLambdaCallbackFind() {
        final LambdaCallback<Document> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(SingleDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.findOneAsync(mockCountCallback, findBuilder.build());

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#findAsync(LambdaCallback, Find.Builder)}
     * method.
     */
    @Test
    public void testFindOneAsyncLambdaCallbackFindBuilder() {
        final LambdaCallback<Document> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Find.Builder findBuilder = new Find.Builder(doc);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(SingleDocumentCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.findOneAsync(mockCountCallback, findBuilder);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findOne(DocumentAssignable)} .
     */
    @Test
    public void testFindOneNonLegacyOptions() {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory
                .start()
                .addDocument("$query", BuilderFactory.start())
                .addDocument(ReadPreference.FIELD_NAME,
                        ReadPreference.PREFER_SECONDARY.asDocument()).build();
        final Document replyDoc = BuilderFactory.start().addInteger("foo", 2)
                .build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PREFER_SECONDARY, false, false, false,
                false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PREFER_SECONDARY);
        expect(myMockClient.send(eq(message), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay(mockCountCallback);

        assertSame(replyDoc, myTestInstance.findOne(BuilderFactory.start()));

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findOne(DocumentAssignable)} .
     */
    @Test
    public void testFindOneNonSharded() {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("foo", 2)
                .build();

        final Query message = new Query("test", "test", doc, null, 1, 1, 0,
                false, ReadPreference.PREFER_SECONDARY, false, false, false,
                false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PREFER_SECONDARY);
        expect(myMockClient.send(eq(message), callback(reply(replyDoc))))
                .andReturn(myAddress);

        replay(mockCountCallback);

        assertSame(replyDoc, myTestInstance.findOne(BuilderFactory.start()));

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link MongoCollectionImpl#findOne(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindOneWithAllOptions() throws Exception {
        final Document result1 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());
        qRequestBuilder.addDocument("$readPreference",
                ReadPreference.PREFER_SECONDARY.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(), 1, 1,
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(myMockClient.send(eq(message), callback(reply(result1))))
                .andReturn(myAddress);

        replay();

        final Future<Document> future = myTestInstance.findOneAsync(builder);
        assertSame(result1, future.get());

        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#findAsync(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindOneWithAllOptionsNonSharded() throws Exception {
        final Document result1 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(), 1, 1,
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(myMockClient.send(eq(message), callback(reply(result1))))
                .andReturn(myAddress);

        replay();

        final Future<Document> future = myTestInstance.findOneAsync(builder);
        assertSame(result1, future.get());

        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#findOneAsync(Find)}
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindOneWithNonLegacyOptionsAndNonSharded() throws Exception {
        final Document result1 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder.build());
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        final Find request = builder.build();

        final Query message = new Query("test", "test", qBuilder.asDocument(),
                request.getProjection(), 1, 1, request.getNumberToSkip(),
                false, ReadPreference.PREFER_SECONDARY, false, false, false,
                true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(myMockClient.send(eq(message), callback(reply(result1))))
                .andReturn(myAddress);

        replay();

        assertSame(result1, myTestInstance.findOne(builder));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#findAndModifyAsync(Callback, FindAndModify)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindWithAllOptions() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());
        qRequestBuilder.addDocument("$readPreference",
                ReadPreference.PREFER_SECONDARY.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final Future<MongoIterator<Document>> future = myTestInstance
                .findAsync(builder);
        final MongoIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#findAsync(Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindWithAllOptionsNonSharded() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final Future<MongoIterator<Document>> future = myTestInstance
                .findAsync(builder);
        final MongoIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#findOneAsync(Find)}
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindWithNonLegacyOptionsAndNonSharded() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder.build());
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        final Find request = builder.build();

        final Query message = new Query("test", "test", qBuilder.asDocument(),
                request.getProjection(), request.getBatchSize(),
                request.getLimit(), request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        replay();

        final Future<MongoIterator<Document>> future = myTestInstance
                .findAsync(request);
        final MongoIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#getDatabaseName()} .
     */
    @Test
    public void testGetDatabaseName() {
        expect(myMockDatabase.getName()).andReturn("foo");
        replay();
        assertEquals("foo", myTestInstance.getDatabaseName());
        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#getDurability()}.
     */
    @Test
    public void testGetDurabilityFromDatabase() {
        final Durability defaultDurability = Durability.journalDurable(1234);

        expect(myMockDatabase.getDurability()).andReturn(defaultDurability);

        replay();

        final Durability result = myTestInstance.getDurability();
        assertSame(defaultDurability, result);

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#getDurability()}.
     */
    @Test
    public void testGetDurabilitySet() {
        final Durability defaultDurability = Durability.journalDurable(1234);
        final Durability setDurability = Durability.journalDurable(4321);

        expect(myMockDatabase.getDurability()).andReturn(defaultDurability);

        replay();

        myTestInstance.setDurability(setDurability);
        assertSame(setDurability, myTestInstance.getDurability());

        myTestInstance.setDurability(null); // Now back to client.
        assertSame(defaultDurability, myTestInstance.getDurability());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#getName()}.
     */
    @Test
    public void testGetName() {
        replay();
        assertEquals("test", myTestInstance.getName());
        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#getReadPreference()}.
     */
    @Test
    public void testGetReadPreferenceFromDatabase() {
        final ReadPreference defaultReadPreference = ReadPreference
                .preferSecondary();

        expect(myMockDatabase.getReadPreference()).andReturn(
                defaultReadPreference);

        replay();

        final ReadPreference result = myTestInstance.getReadPreference();
        assertSame(defaultReadPreference, result);

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#getReadPreference()}.
     */
    @Test
    public void testGetReadPreferenceSet() {
        final ReadPreference defaultReadPreference = ReadPreference
                .preferSecondary();
        final ReadPreference setReadPreference = ReadPreference.secondary();

        expect(myMockDatabase.getReadPreference()).andReturn(
                defaultReadPreference);

        replay();

        myTestInstance.setReadPreference(setReadPreference);
        assertSame(setReadPreference, myTestInstance.getReadPreference());

        myTestInstance.setReadPreference(null); // Now back to client.
        assertSame(defaultReadPreference, myTestInstance.getReadPreference());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#groupBy(GroupBy)} .
     */
    @Test
    public void testGroupBy() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder value = result.pushArray("retval");
        value.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(result.build().find(ArrayElement.class, "retval").get(0),
                myTestInstance.groupBy(builder));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#groupByAsync(Callback, GroupBy)} .
     */
    @Test
    public void testGroupByAsyncCallbackOfArrayElementGroupBy() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));

        final GroupBy request = builder.build();

        final Callback<ArrayElement> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.groupByAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#groupByAsync(GroupBy)}.
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testGroupByAsyncGroupBy() throws Exception {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder value = result.pushArray("retval");
        value.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(result.build().find(ArrayElement.class, "retval").get(0),
                myTestInstance.groupByAsync(builder).get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#groupByAsync(LambdaCallback, GroupBy)}
     * method.
     */
    @Test
    public void testGroupByAsyncLambdaCallbackGroupBy() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));

        final GroupBy request = builder.build();

        final LambdaCallback<ArrayElement> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.groupByAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#groupByAsync(LambdaCallback, GroupBy.Builder)}
     * method.
     */
    @Test
    public void testGroupByAsyncLambdaCallbackGroupByBuilder() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));

        final LambdaCallback<ArrayElement> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.groupByAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#groupByAsync(Callback, GroupBy)} .
     */
    @Test
    public void testGroupByAsyncWithAllOptions() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeyFunction("function f() {}");
        builder.setFinalizeFunction("finalize");
        builder.setInitialValue(BuilderFactory.start().build());
        builder.setQuery(BuilderFactory.start().addBoolean("foo", true).build());
        builder.setReduceFunction("reduce");
        builder.setMaximumTimeMilliseconds(1000);

        final GroupBy request = builder.build();

        final Callback<ArrayElement> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.addJavaScript("$keyf", request.getKeyFunction());
        group.addDocument("initial", request.getInitialValue());
        group.addJavaScript("$reduce", request.getReduceFunction());
        group.addJavaScript("finalize", request.getFinalizeFunction());
        group.addDocument("cond", request.getQuery());
        group.add("maxTimeMS", 1000L);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.groupByAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#groupByAsync(Callback, GroupBy)} .
     */
    @Test
    public void testGroupByWithReadPreference() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        final GroupBy request = builder.build();

        final Callback<ArrayElement> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);
        group.add(ReadPreference.FIELD_NAME, ReadPreference.PREFER_SECONDARY);

        final Command message = new Command("test", expectedCommand.build(),
                ReadPreference.PREFER_SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.groupByAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#groupByAsync(Callback, GroupBy)} .
     */
    @Test
    public void testGroupByWithReadPreferenceNonSharded() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        final Callback<ArrayElement> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.build(),
                ReadPreference.PREFER_SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyArrayCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.groupByAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(boolean, DocumentAssignable...)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncBooleanDocumentArray() throws Exception {
        final Document doc = BuilderFactory.start().addBoolean("_id", true)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Integer.valueOf(1), myTestInstance.insertAsync(true, doc)
                .get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(boolean, Durability, DocumentAssignable...)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncBooleanDurabilityDocumentArray()
            throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Integer.valueOf(1),
                myTestInstance.insertAsync(true, Durability.ACK, doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Callback, boolean, DocumentAssignable...)}
     * .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerBooleanDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, true, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#insertAsync(Callback, boolean, Durability, DocumentAssignable...)}
     * .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerBooleanDurabilityDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance
                .insertAsync(mockCountCallback, true, Durability.ACK, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Callback, DocumentAssignable...)}
     * .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Callback, Durability, DocumentAssignable...)}
     * .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerDurabilityDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, Durability.ACK, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(DocumentAssignable...)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncDocumentArray() throws Exception {
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getDurability()).andReturn(Durability.NONE);
        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        replay();

        assertEquals(Integer.valueOf(-1), myTestInstance.insertAsync(doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Durability, DocumentAssignable...)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncDurabilityDocumentArray() throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Integer.valueOf(1),
                myTestInstance.insertAsync(Durability.ACK, doc).get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#insertAsync(LambdaCallback, boolean, DocumentAssignable[])}
     * method.
     */
    @Test
    public void testInsertAsyncLambdaCallbackBDocumentAssignable() {
        final LambdaCallback<Integer> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, true, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#insertAsync(LambdaCallback, boolean, Durability, DocumentAssignable[])}
     * method.
     */
    @Test
    public void testInsertAsyncLambdaCallbackBDurabilityDocumentAssignable() {
        final LambdaCallback<Integer> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance
                .insertAsync(mockCountCallback, true, Durability.ACK, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#insertAsync(LambdaCallback, DocumentAssignable[])}
     * method.
     */
    @Test
    public void testInsertAsyncLambdaCallbackDocumentAssignable() {
        final LambdaCallback<Integer> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#insertAsync(LambdaCallback, Durability, DocumentAssignable[])}
     * method.
     */
    @Test
    public void testInsertAsyncLambdaCallbackDurabilityDocumentAssignable() {
        final LambdaCallback<Integer> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, Durability.ACK, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(boolean, DocumentAssignable...)} .
     */
    @Test
    public void testInsertBooleanDocumentArray() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(2, myTestInstance.insert(true, doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(boolean, Durability, DocumentAssignable...)}
     * .
     */
    @Test
    public void testInsertBooleanDurabilityDocumentArray() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(2, myTestInstance.insert(true, Durability.ACK, doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(Durability, DocumentAssignable...)}
     * .
     */
    @Test
    public void testInsertCannotInjectId() {
        final Document doc = new ImmutableDocument(BuilderFactory.start()
                .build());
        final Document replyDoc = new ImmutableDocument(BuilderFactory.start()
                .addInteger("n", 2));

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(2, myTestInstance.insert(Durability.ACK, doc));
        assertThat(doc.get("_id"), nullValue(Element.class));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(DocumentAssignable...)} .
     */
    @Test
    public void testInsertDocumentArray() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(2, myTestInstance.insert(doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(Durability, DocumentAssignable...)}
     * .
     */
    @Test
    public void testInsertDurabilityDocumentArray() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(2, myTestInstance.insert(Durability.ACK, doc));
        assertThat(doc.get("_id"), notNullValue(Element.class));

        verify();
    }

    /**
     * Test method for {@link MongoCollection#isCapped()}.
     */
    @Test
    public void testIsCapped() {
        final Document noField = BuilderFactory.start().build();
        final Document boolFieldYes = BuilderFactory.start()
                .add("capped", true).build();
        final Document boolFieldNo = BuilderFactory.start()
                .add("capped", false).build();
        final Document intFieldYes = BuilderFactory.start().add("capped", 1)
                .build();
        final Document longFieldNo = BuilderFactory.start().add("capped", 0L)
                .build();

        expect(myMockDatabase.runCommand("collStats", "test", null)).andReturn(
                noField);
        expect(myMockDatabase.runCommand("collStats", "test", null)).andReturn(
                boolFieldYes);
        expect(myMockDatabase.runCommand("collStats", "test", null)).andReturn(
                boolFieldNo);
        expect(myMockDatabase.runCommand("collStats", "test", null)).andReturn(
                intFieldYes);
        expect(myMockDatabase.runCommand("collStats", "test", null)).andReturn(
                longFieldNo);

        replay();

        assertFalse(myTestInstance.isCapped());
        assertTrue(myTestInstance.isCapped());
        assertFalse(myTestInstance.isCapped());
        assertTrue(myTestInstance.isCapped());
        assertFalse(myTestInstance.isCapped());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#mapReduce(MapReduce)} .
     */
    @Test
    public void testMapReduce() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("results").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Collections.singletonList(value.build()),
                myTestInstance.mapReduce(builder));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncCallbackOfListOfDocumentMapReduce() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#mapReduceAsync(LambdaCallback, MapReduce)}
     * method.
     */
    @Test
    public void testMapReduceAsyncLambdaCallbackMapReduce() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        final LambdaCallback<List<Document>> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#mapReduceAsync(LambdaCallback, MapReduce.Builder)}
     * method.
     */
    @Test
    public void testMapReduceAsyncLambdaCallbackMapReduceBuilder() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        final LambdaCallback<List<Document>> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#mapReduceAsync(MapReduce)}
     * .
     * 
     * @throws Exception
     *             On a failure.
     */
    @Test
    public void testMapReduceAsyncMapReduce() throws Exception {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("results").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Collections.singletonList(value.build()), myTestInstance
                .mapReduceAsync(builder).get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithAllOptions() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.INLINE);
        builder.setFinalizeFunction("finalize");
        builder.setJsMode(true);
        builder.setKeepTemp(true);
        builder.setLimit(10);
        builder.setQuery(BuilderFactory.start().addInteger("foo", 12).build());
        builder.setScope(BuilderFactory.start().addInteger("foo", 13).build());
        builder.setSort(BuilderFactory.start().addInteger("foo", 14).build());
        builder.setVerbose(true);
        builder.setMaximumTimeMilliseconds(2345L);

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.addJavaScript("finalize", "finalize");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("sort", request.getSort());
        expectedCommand.addDocument("scope", request.getScope());
        expectedCommand.addInteger("limit", 10);
        expectedCommand.addBoolean("keeptemp", true);
        expectedCommand.addBoolean("jsMode", true);
        expectedCommand.addBoolean("verbose", true);
        expectedCommand.add("maxTimeMS", request.getMaximumTimeMilliseconds());
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithOutputMerge() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.MERGE);
        builder.setOutputName("out");

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addString("merge", "out");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithOutputMergeAndDB() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.MERGE);
        builder.setOutputName("out");
        builder.setOutputDatabase("out_db");

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addString("merge", "out")
                .addString("db", "out_db");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithOutputReduce() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.REDUCE);
        builder.setOutputName("out");

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addString("reduce", "out");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithOutputReduceAndDB() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.REDUCE);
        builder.setOutputName("out");
        builder.setOutputDatabase("out_db");

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addString("reduce", "out")
                .addString("db", "out_db");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithOutputReplace() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.REPLACE);
        builder.setOutputName("out");

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addString("replace", "out");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithOutputReplaceAndDB() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.REPLACE);
        builder.setOutputName("out");
        builder.setOutputDatabase("out_db");

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addString("replace", "out")
                .addString("db", "out_db");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithReadPreference() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.REPLACE);
        builder.setOutputName("out");
        builder.setOutputDatabase("out_db");
        builder.setReadPreference(ReadPreference.PREFER_PRIMARY);

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.push("$query").addString("mapreduce", "test")
                .addJavaScript("map", "map").addJavaScript("reduce", "reduce")
                .push("out").addString("replace", "out")
                .addString("db", "out_db");
        expectedCommand.add(ReadPreference.FIELD_NAME,
                ReadPreference.PREFER_PRIMARY);

        final Command message = new Command("test", expectedCommand.build(),
                ReadPreference.PREFER_PRIMARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#mapReduceAsync(Callback, MapReduce)} .
     */
    @Test
    public void testMapReduceAsyncWithReadPreferenceNotSharded() {
        final MapReduce.Builder builder = new MapReduce.Builder();
        builder.setMapFunction("map");
        builder.setReduceFunction("reduce");
        builder.setOutputType(MapReduce.OutputType.REPLACE);
        builder.setOutputName("out");
        builder.setOutputDatabase("out_db");
        builder.setReadPreference(ReadPreference.PREFER_PRIMARY);

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addString("replace", "out")
                .addString("db", "out_db");

        final Command message = new Command("test", expectedCommand.build(),
                ReadPreference.PREFER_PRIMARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#saveAsync(Callback, DocumentAssignable)} .
     */
    @Test
    public void testSaveAsyncCallbackOfIntegerDocumentAssignable() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().add("_id", 1).build();
        final Document update = doc;

        final Update message = new Update("test", "test", doc, update, false,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.saveAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#saveAsync(Callback, DocumentAssignable, Durability)}
     * .
     */
    @Test
    public void testSaveAsyncCallbackOfIntegerDocumentAssignableDurability() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().add("_id", 1).build();
        final Document update = doc;

        final Update message = new Update("test", "test", doc, update, false,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.saveAsync(mockCountCallback, doc, Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#saveAsync(DocumentAssignable)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testSaveAsyncDocumentAssignable() throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Integer.valueOf(2), myTestInstance.saveAsync(doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#saveAsync(DocumentAssignable,Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testSaveAsyncDocumentAssignableDurability() throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Integer.valueOf(2),
                myTestInstance.saveAsync(doc, Durability.ACK).get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#saveAsync(LambdaCallback, DocumentAssignable)}
     * method.
     */
    @Test
    public void testSaveAsyncLambdaCallbackDocumentAssignable() {
        final LambdaCallback<Integer> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().add("_id", 1).build();
        final Document update = doc;

        final Update message = new Update("test", "test", doc, update, false,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.saveAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#saveAsync(LambdaCallback, DocumentAssignable, Durability)}
     * method.
     */
    @Test
    public void testSaveAsyncLambdaCallbackDocumentAssignableDurability() {
        final LambdaCallback<Integer> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().add("_id", 1).build();
        final Document update = doc;

        final Update message = new Update("test", "test", doc, update, false,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.saveAsync(mockCountCallback, doc, Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#save(DocumentAssignable)}
     * .
     */
    @Test
    public void testSaveDocumentAssignable() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(2, myTestInstance.save(doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#save(DocumentAssignable, Durability)} .
     */
    @Test
    public void testSaveDocumentAssignableDurability() {
        final Document doc = BuilderFactory.start().build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .build();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(2, myTestInstance.save(doc, Durability.ACK));

        verify();
    }

    /**
     * Test method for {@link MongoCollection#stats()}.
     */
    @Test
    public void testStats() {
        final Document result = BuilderFactory.start().build();

        expect(myMockDatabase.runCommand("collStats", "test", null)).andReturn(
                result);

        replay();

        assertSame(result, myTestInstance.stats());
        verify();
    }

    /**
     * Test method for {@link MongoCollectionImpl#streamingFind(Callback, Find)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testStreamFindWithNonLegacyOptionsAndNonSharded()
            throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder.build());
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);

        final Find request = builder.build();

        final Query message = new Query("test", "test", qBuilder.asDocument(),
                request.getProjection(), request.getBatchSize(),
                request.getLimit(), request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay(mockCallback);

        myTestInstance.stream(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#streamingFind(Callback,DocumentAssignable)}
     * .
     */
    @Deprecated
    @Test
    public void testStreamingFindDocument() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Callback<Document> mockCallback = createMock(Callback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.callback(EasyMock.isNull(Document.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.streamingFind(mockCallback, doc);

        verify(mockCallback);
    }

    /**
     * Test method for {@link MongoCollectionImpl#streamingFind(Callback,Find)}
     * .
     */
    @Deprecated
    @Test
    public void testStreamingFindFind() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final Callback<Document> mockCallback = createMock(Callback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.callback(EasyMock.isNull(Document.class));
        expectLastCall();

        replay();

        myTestInstance.streamingFind(mockCallback,
                new Find.Builder(doc).build());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#streamingFind(LambdaCallback, DocumentAssignable)}
     * method.
     */
    @Test
    public void testStreamingFindLambdaCallbackDocumentAssignable() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.accept(null, result1);
        expectLastCall();
        mockCallback.accept(null, result2);
        expectLastCall();
        mockCallback.accept(null, null);
        expectLastCall();

        replay(mockCallback);

        assertNotNull(myTestInstance.streamingFind(mockCallback, doc));

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#streamingFind(StreamCallback,DocumentAssignable)}
     * .
     */
    @Test
    public void testStreamingFindStreamCallbackDocument() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay(mockCallback);

        assertNotNull(myTestInstance.streamingFind(mockCallback, doc));

        verify(mockCallback);
    }

    /**
     * Test method for {@link MongoCollectionImpl#streamingFind(Callback, Find)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Deprecated
    @Test
    public void testStreamingFindWithAllOptions() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());
        qRequestBuilder.addDocument("$readPreference",
                ReadPreference.PREFER_SECONDARY.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        final Callback<Document> mockCallback = createMock(Callback.class);
        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.callback(null);
        expectLastCall();

        replay(mockCallback);

        myTestInstance.streamingFind(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for {@link MongoCollectionImpl#streamingFind(Callback, Find)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Deprecated
    @Test
    public void testStreamingFindWithAllOptionsNonSharded() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        final Callback<Document> mockCallback = createMock(Callback.class);
        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.callback(null);
        expectLastCall();

        replay(mockCallback);

        myTestInstance.streamingFind(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#stream(LambdaCallback, Aggregation)}
     * method.
     */
    @Test
    public void testStreamLambdaCallbackAggregation() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.stream(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#stream(LambdaCallback, Aggregation.Builder)}
     * method.
     */
    @Test
    public void testStreamLambdaCallbackAggregationBuilder() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.stream(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#stream(LambdaCallback, Find.Builder)}
     * method.
     */
    @Test
    public void testStreamLambdaCallbackFind() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.accept(null, result1);
        expectLastCall();
        mockCallback.accept(null, result2);
        expectLastCall();
        mockCallback.accept(null, null);
        expectLastCall();

        replay();

        myTestInstance.stream(mockCallback, new Find.Builder(doc).build());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#stream(LambdaCallback, Find.Builder)}
     * method.
     */
    @Test
    public void testStreamLambdaCallbackFindBuilder() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final LambdaCallback<Document> mockCallback = createMock(LambdaCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.accept(null, result1);
        expectLastCall();
        mockCallback.accept(null, result2);
        expectLastCall();
        mockCallback.accept(null, null);
        expectLastCall();

        replay();

        myTestInstance.stream(mockCallback, new Find.Builder(doc));

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#stream(StreamCallback, Aggregation)}
     * method.
     */
    @Test
    public void testStreamStreamCallbackAggregation() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.stream(mockCallback, builder.build());

        verify(mockCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#stream(StreamCallback, Aggregation.Builder)}
     * method.
     */
    @Test
    public void testStreamStreamCallbackAggregationBuilder() {
        final Aggregation.Builder builder = new Aggregation.Builder();
        builder.limit(5);

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("result").push();
        value.addInteger("foo", 1);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("aggregate", "test");
        expectedCommand.pushArray("pipeline").push().addInteger("$limit", 5);

        final AggregationCommand message = new AggregationCommand(
                builder.build(), "test", "test", expectedCommand.build(),
                ReadPreference.PRIMARY, Version.VERSION_2_4);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        anyObject(ReplyResultCallback.class))).andReturn(
                myAddress);

        replay(mockCallback);

        myTestInstance.stream(mockCallback, builder);

        verify(mockCallback);
    }

    /**
     * Test method for {@link MongoCollectionImpl#stream(StreamCallback,Find)} .
     */
    @Test
    public void testStreamStreamCallbackFind() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay();

        myTestInstance.stream(mockCallback, new Find.Builder(doc).build());

        verify();
    }

    /**
     * Test for the {@link AbstractMongoCollection#stream(StreamCallback, Find)}
     * method.
     */
    @Test
    public void testStreamStreamCallbackFindBuilder() {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final Document doc = BuilderFactory.start().build();

        final Query message = new Query("test", "test", doc, null, 0, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay();

        myTestInstance.stream(mockCallback, new Find.Builder(doc));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#streamingFind(StreamCallback, Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testStreamStreamCallbackFindWithAllOptions() throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());
        qRequestBuilder.addDocument("$readPreference",
                ReadPreference.PREFER_SECONDARY.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType()).andReturn(ClusterType.SHARDED);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay(mockCallback);

        myTestInstance.stream(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#streamingFind(StreamCallback, Find)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testStreamStreamCallbackWithAllOptionsNonSharded()
            throws Exception {
        final Document result1 = BuilderFactory.start().build();
        final Document result2 = BuilderFactory.start().build();

        final DocumentBuilder qBuilder = BuilderFactory.start().addInteger(
                "foo", 1);
        final DocumentBuilder sort = BuilderFactory.start()
                .addInteger("baz", 1);

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(qBuilder);
        builder.setProjection(BuilderFactory.start().addBoolean("_id", true)
                .build());
        builder.setBatchSize(101010);
        builder.setLimit(202020);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReadPreference(ReadPreference.PREFER_SECONDARY);
        builder.setSort(sort);

        final Find request = builder.build();

        final DocumentBuilder qRequestBuilder = BuilderFactory.start();
        qRequestBuilder.add("$query", qBuilder);
        qRequestBuilder.addDocument("$orderby", sort.asDocument());

        final Query message = new Query("test", "test",
                qRequestBuilder.build(), request.getProjection(),
                request.getBatchSize(), request.getLimit(),
                request.getNumberToSkip(), false,
                ReadPreference.PREFER_SECONDARY, false, false, false, true);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(
                myMockClient.send(eq(message),
                        callback(reply(result1, result2))))
                .andReturn(myAddress);

        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        mockCallback.callback(result1);
        expectLastCall();
        mockCallback.callback(result2);
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay(mockCallback);

        myTestInstance.stream(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#textSearch(Text)} .
     */
    @Test
    public void testTextSearchFull() {
        final Text command = Text.builder().searchTerm("bar").language("l")
                .limit(10).query(where("f").equals(false))
                .readPreference(ReadPreference.PREFER_SECONDARY)
                .returnFields(BuilderFactory.start().add("f", 1).add("_id", 0))
                .maximumTime(5432, TimeUnit.MILLISECONDS).build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("results").push();
        value.addInteger("score", 1);
        value.push("obj").add("a", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("text", "test");
        expectedCommand.addString("search", "bar");
        expectedCommand.add("filter", where("f").equals(false).build());
        expectedCommand.addInteger("limit", 10);
        expectedCommand.add("project",
                BuilderFactory.start().add("f", 1).add("_id", 0).build());
        expectedCommand.addString("language", "l");
        expectedCommand.add("maxTimeMS", 5432L);

        final Command message = new Command("test", expectedCommand.build(),
                ReadPreference.PREFER_SECONDARY);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getClusterType())
                .andReturn(ClusterType.REPLICA_SET);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Collections.singletonList(new TextResult(value)),
                myTestInstance.textSearch(command));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#textSearch(Text)} .
     */
    @Test
    public void testTextSearchMinimal() {
        final Text command = Text.builder().searchTerm("foo").build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("results").push();
        value.addInteger("score", 1);
        value.push("obj").add("a", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("text", "test");
        expectedCommand.addString("search", "foo");

        final Command message = new Command("test", expectedCommand.build());

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getReadPreference()).andReturn(
                ReadPreference.PRIMARY);
        expect(myMockClient.send(eq(message), callback(reply(result.build()))))
                .andReturn(myAddress);

        replay();

        assertEquals(Collections.singletonList(new TextResult(value)),
                myTestInstance.textSearch(command));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Callback, DocumentAssignable, DocumentAssignable)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocument() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocumentBooleanBoolean() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update, true, true);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#updateAsync(Callback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocumentBooleanBooleanDurability() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update, true, true,
                Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Callback, DocumentAssignable, DocumentAssignable, Durability)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocumentDurability() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update,
                Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(DocumentAssignable, DocumentAssignable)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocument() throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockDatabase.getDurability()).andReturn(Durability.NONE);
        expect(myMockClient.send(eq(message), isNull(Callback.class)))
                .andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(-1L), myTestInstance.updateAsync(doc, update)
                .get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(DocumentAssignable, DocumentAssignable, boolean, boolean)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocumentBooleanBoolean()
            throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1L),
                myTestInstance.updateAsync(doc, update, true, true).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocumentBooleanBooleanDurability()
            throws Exception {

        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(
                Long.valueOf(1L),
                myTestInstance.updateAsync(doc, update, true, true,
                        Durability.ACK).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(DocumentAssignable, DocumentAssignable, Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocumentDurability() throws Exception {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(Long.valueOf(1L),
                myTestInstance.updateAsync(doc, update, Durability.ACK).get());

        verify();
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#updateAsync(LambdaCallback, DocumentAssignable, DocumentAssignable)}
     * method.
     */
    @Test
    public void testUpdateAsyncLambdaCallbackDocumentAssignableDocumentAssignable() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#updateAsync(LambdaCallback, DocumentAssignable, DocumentAssignable, boolean, boolean)}
     * method.
     */
    @Test
    public void testUpdateAsyncLambdaCallbackDocumentAssignableDocumentAssignableBB() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update, true, true);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#updateAsync(LambdaCallback, DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * method.
     */
    @Test
    public void testUpdateAsyncLambdaCallbackDocumentAssignableDocumentAssignableBBDurability() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update, true, true,
                Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test for the
     * {@link AbstractMongoCollection#updateAsync(LambdaCallback, DocumentAssignable, DocumentAssignable, Durability)}
     * method.
     */
    @Test
    public void testUpdateAsyncLambdaCallbackDocumentAssignableDocumentAssignableDurability() {
        final LambdaCallback<Long> mockCountCallback = createMock(LambdaCallback.class);
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        anyObject(ReplyLongCallback.class))).andReturn(
                myAddress);

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update,
                Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(DocumentAssignable, DocumentAssignable)}
     * .
     */
    @Test
    public void testUpdateDocumentDocument() {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.update(doc, update));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(DocumentAssignable, DocumentAssignable, boolean, boolean)}
     * .
     */
    @Test
    public void testUpdateDocumentDocumentBooleanBoolean() {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockDatabase.getDurability()).andReturn(Durability.ACK);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.update(doc, update, true, true));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(DocumentAssignable, DocumentAssignable, boolean, boolean, Durability)}
     * .
     */
    @Test
    public void testUpdateDocumentDocumentBooleanBooleanDurability() {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L,
                myTestInstance.update(doc, update, true, true, Durability.ACK));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(DocumentAssignable, DocumentAssignable, Durability)}
     * .
     */
    @Test
    public void testUpdateDocumentDocumentDurability() {
        final Document doc = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .build();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .build();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(
                myMockClient.send(eq(message), eq(getLastError),
                        callback(reply(replyDoc)))).andReturn(myAddress);

        replay();

        assertEquals(1L, myTestInstance.update(doc, update, Durability.ACK));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateOptions(DocumentAssignable)}.
     */
    @Test
    public void testUpdateOptions() {
        final Document command = BuilderFactory.start().add("collMod", "test")
                .add("usePowerOf2Sizes", true).build();

        final Document options = BuilderFactory.start()
                .add("usePowerOf2Sizes", true).build();

        final Document result = BuilderFactory.start().build();

        myMockDatabase.runCommandAsync(callback(result), eq(command),
                eq(Version.VERSION_2_2));
        expectLastCall();

        replay();

        assertSame(result, myTestInstance.updateOptions(options));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#validate(MongoCollection.ValidateMode)} .
     */
    @Test
    public void testValidate() {
        final Document indexOnly = BuilderFactory.start()
                .add("scandata", false).build();
        final Document normal = null;
        final Document full = BuilderFactory.start().add("full", true).build();

        final Document result = BuilderFactory.start().build();

        expect(myMockDatabase.runCommand("validate", "test", indexOnly))
                .andReturn(result);
        expect(myMockDatabase.runCommand("validate", "test", normal))
                .andReturn(result);
        expect(myMockDatabase.runCommand("validate", "test", full)).andReturn(
                result);

        replay();

        assertSame(result,
                myTestInstance
                        .validate(MongoCollection.ValidateMode.INDEX_ONLY));
        assertSame(result,
                myTestInstance.validate(MongoCollection.ValidateMode.NORMAL));
        assertSame(result,
                myTestInstance.validate(MongoCollection.ValidateMode.FULL));

        verify();
    }

    /**
     * Performs a {@link EasyMock#replay(Object...)} on the provided mocks and
     * the {@link #myMockClient} and {@link #myMockDatabase} objects.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void replay(final Object... mocks) {
        EasyMock.replay(mocks);
        EasyMock.replay(myMockClient, myMockDatabase);
    }

    /**
     * Creates a reply around the document.
     * 
     * @param replyDoc
     *            The document to include in the reply.
     * @return The {@link Reply}
     */
    private Reply reply(final Document... replyDoc) {
        return new Reply(1, 0, 0, Arrays.asList(replyDoc), false, false, false,
                false);
    }

    /**
     * Performs a {@link EasyMock#verify(Object...)} on the provided mocks and
     * the {@link #myMockClient} and {@link #myMockDatabase} objects.
     * 
     * @param mocks
     *            The mock to replay.
     */
    private void verify(final Object... mocks) {
        EasyMock.verify(mocks);
        EasyMock.verify(myMockClient, myMockDatabase);
    }
}
