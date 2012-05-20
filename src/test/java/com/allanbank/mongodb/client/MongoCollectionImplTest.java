/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.AnswerCallback.callback;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Future;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.commands.Distinct;
import com.allanbank.mongodb.commands.Find;
import com.allanbank.mongodb.commands.FindAndModify;
import com.allanbank.mongodb.commands.GroupBy;
import com.allanbank.mongodb.commands.MapReduce;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Delete;
import com.allanbank.mongodb.connection.message.GetLastError;
import com.allanbank.mongodb.connection.message.Insert;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.Update;
import com.allanbank.mongodb.error.ReplyException;

/**
 * MongoCollectionImplTest provides tests for the {@link MongoCollectionImpl}
 * class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("unchecked")
public class MongoCollectionImplTest {

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
    }

    /**
     * Cleans up the base set of objects for the test.
     */
    @After
    public void tearDown() {
        myMockClient = null;
        myMockDatabase = null;

        myTestInstance = null;
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(Callback, Document)} .
     */
    @Test
    public void testCountAsyncCallbackOfLongDocument() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                false)), anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#countAsync(Callback, Document, boolean)} .
     */
    @Test
    public void testCountAsyncCallbackOfLongDocumentBoolean() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                true)), anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.countAsync(mockCountCallback, doc, true);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#countAsync(Document)} .
     */
    @Test
    public void testCountAsyncDocument() {
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                false)), anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay();

        assertNotNull(myTestInstance.countAsync(doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#countAsync(Document, boolean)} .
     * 
     * @throws Exception
     *             On an error
     */
    @Test
    public void testCountAsyncDocumentBoolean() throws Exception {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                true)), callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(1), myTestInstance.countAsync(doc, true)
                .get());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#count(Document)} .
     */
    @Test
    public void testCountDocument() {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                false)), callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L, myTestInstance.count(doc));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#count(Document, boolean)}
     * .
     */
    @Test
    public void testCountDocumentBoolean() {
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                true)), callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L, myTestInstance.count(doc, true));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#count(Document, boolean)}
     * .
     */
    @Test
    public void testCountDocumentBooleanOnInterrupt() {
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                true)), callback(Reply.class));
        expectLastCall();

        replay();

        try {
            myTestInstance.count(doc, true);
        }
        catch (final MongoDbException error) {
            // Good.
            assertTrue(error.getCause() instanceof InterruptedException);
        }
        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#count(Document, boolean)}
     * .
     */
    @Test
    public void testCountDocumentBooleanOnIOError() {
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                true)), callback(Reply.class, new IOException()));
        expectLastCall();

        replay();

        try {
            myTestInstance.count(doc, true);
        }
        catch (final MongoDbException error) {
            // Good.
            assertTrue(error.getCause() instanceof IOException);
        }
        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#count(Document, boolean)}
     * .
     */
    @Test
    public void testCountDocumentBooleanOnMongoError() {
        final Document replyDoc = BuilderFactory.start().addInteger("X", 1)
                .get();
        final Document doc = BuilderFactory.start().get();

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(new Command("test", BuilderFactory.start()
                .addString("count", "test").addDocument("query", doc).get(),
                true)), callback(reply(replyDoc)));
        expectLastCall();

        replay();

        try {
            myTestInstance.count(doc, true);
        }
        catch (final ReplyException error) {
            // Good.
        }
        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#createIndex(java.util.LinkedHashMap)} .
     */
    @Test
    public void testCreateIndexLinkedHashMapOfStringInteger() {
        final LinkedHashMap<String, Integer> keySet = new LinkedHashMap<String, Integer>();
        keySet.put("k", Integer.valueOf(1));
        keySet.put("l", Integer.valueOf(-1));

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "test_k1_l-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.get(), null, 1 /* numberToReturn */,
                0 /* skip */, false /* tailable */, false /* replicaOk */,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(queryMessage),
                callback(reply(indexDocBuilder.get())));
        expectLastCall();

        replay();

        myTestInstance.createIndex(keySet);

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#createIndex(java.util.LinkedHashMap, boolean)}
     * .
     */
    @Test
    public void testCreateIndexLinkedHashMapOfStringIntegerBoolean() {
        final LinkedHashMap<String, Integer> keySet = new LinkedHashMap<String, Integer>();
        keySet.put("k", Integer.valueOf(1));
        keySet.put("l", Integer.valueOf(-1));

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "test_k1_l-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.addBoolean("unique", true);
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.get(), null, 1 /* numberToReturn */,
                0 /* skip */, false /* tailable */, false /* replicaOk */,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(queryMessage),
                callback(reply(indexDocBuilder.get())));
        expectLastCall();

        replay();

        myTestInstance.createIndex(keySet, true);

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(java.lang.String, java.util.LinkedHashMap, boolean)}
     * .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBoolean() {

        final LinkedHashMap<String, Integer> keySet = new LinkedHashMap<String, Integer>();
        keySet.put("k", Integer.valueOf(1));

        final Durability expectedDur = Durability.ACK;
        final GetLastError expectedLastError = new GetLastError("test",
                expectedDur.isWaitForFsync(), expectedDur.isWaitForJournal(),
                expectedDur.getWaitForReplicas(),
                expectedDur.getWaitTimeoutMillis());

        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "name");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.get(), null, 1 /* numberToReturn */,
                0 /* skip */, false /* tailable */, false /* replicaOk */,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(4);

        myMockClient.send(eq(queryMessage), callback(reply()));
        expectLastCall();
        myMockClient.send(anyObject(Insert.class), eq(expectedLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        myTestInstance.createIndex("name", keySet, false);

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(java.lang.String, java.util.LinkedHashMap, boolean)}
     * .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBooleanAlreadyExists() {

        final LinkedHashMap<String, Integer> keySet = new LinkedHashMap<String, Integer>();
        keySet.put("k", Integer.valueOf(1));

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "name");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.get(), null, 1 /* numberToReturn */,
                0 /* skip */, false /* tailable */, false /* replicaOk */,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(queryMessage),
                callback(reply(indexDocBuilder.get())));
        expectLastCall();

        replay();

        myTestInstance.createIndex("name", keySet, false);

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(java.lang.String, java.util.LinkedHashMap, boolean)}
     * .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBooleanAlreadyExistsEmptyName() {

        final LinkedHashMap<String, Integer> keySet = new LinkedHashMap<String, Integer>();
        keySet.put("k", Integer.valueOf(1));
        keySet.put("l", Integer.valueOf(-1));

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "test_k1_l-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.get(), null, 1 /* numberToReturn */,
                0 /* skip */, false /* tailable */, false /* replicaOk */,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(queryMessage),
                callback(reply(indexDocBuilder.get())));
        expectLastCall();

        replay();

        myTestInstance.createIndex("", keySet, false);

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#createIndex(java.lang.String, java.util.LinkedHashMap, boolean)}
     * .
     */
    @Test
    public void testCreateIndexStringLinkedHashMapOfStringIntegerBooleanAlreadyExistsNullName() {

        final LinkedHashMap<String, Integer> keySet = new LinkedHashMap<String, Integer>();
        keySet.put("k", Integer.valueOf(1));
        keySet.put("l", Integer.valueOf(-1));

        final DocumentBuilder indexDocBuilder = BuilderFactory.start();
        indexDocBuilder.addString("name", "test_k1_l-1");
        indexDocBuilder.addString("ns", "test.test");
        indexDocBuilder.push("key").addInteger("k", 1).addInteger("l", -1);

        final Query queryMessage = new Query("test", "system.indexes",
                indexDocBuilder.get(), null, 1 /* numberToReturn */,
                0 /* skip */, false /* tailable */, false /* replicaOk */,
                false /* noCursorTimeout */, false /* awaitData */,
                false /* exhaust */, false /* partial */);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(queryMessage),
                callback(reply(indexDocBuilder.get())));
        expectLastCall();

        replay();

        myTestInstance.createIndex(null, keySet, false);

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Callback, Document)} .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocument() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        expect(myMockClient.getDefaultDurability()).andReturn(Durability.NONE);

        myMockClient.send(eq(message));
        expectLastCall();

        mockCountCallback.callback(Long.valueOf(-1L));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Callback, Document, boolean)}
     * .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocumentBoolean() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc, true);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#deleteAsync(Callback, Document, boolean, Durability)}
     * .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocumentBooleanDurability() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance
                .deleteAsync(mockCountCallback, doc, true, Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Callback, Document, Durability)}
     * .
     */
    @Test
    public void testDeleteAsyncCallbackOfLongDocumentDurability() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message));
        expectLastCall();

        mockCountCallback.callback(Long.valueOf(-1L));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.deleteAsync(mockCountCallback, doc, Durability.NONE);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#deleteAsync(Document)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocument() throws Exception {

        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Delete message = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(1), myTestInstance.deleteAsync(doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Document, boolean)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocumentBoolean() throws Exception {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(1), myTestInstance.deleteAsync(doc, true)
                .get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Document, boolean, Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocumentBooleanDurability() throws Exception {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(1),
                myTestInstance.deleteAsync(doc, true, Durability.ACK).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#deleteAsync(Document, Durability)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testDeleteAsyncDocumentDurability() throws Exception {
        final Document doc = BuilderFactory.start().get();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(-1L),
                myTestInstance.deleteAsync(doc, Durability.NONE).get());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#delete(Document)} .
     */
    @Test
    public void testDeleteDocument() {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Delete message = new Delete("test", "test", doc, false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L, myTestInstance.delete(doc));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#delete(Document, boolean)}
     * .
     */
    @Test
    public void testDeleteDocumentBoolean() {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1, myTestInstance.delete(doc, true));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#delete(Document, boolean, Durability)} .
     */
    @Test
    public void testDeleteDocumentBooleanDurability() {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Delete message = new Delete("test", "test", doc, true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L, myTestInstance.delete(doc, true, Durability.ACK));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#delete(Document, Durability)} .
     */
    @Test
    public void testDeleteDocumentDurability() {
        final Document doc = BuilderFactory.start().get();

        final Delete message = new Delete("test", "test", doc, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message));
        expectLastCall();

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
        builder.setQuery(BuilderFactory.start().get());

        final Distinct request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder values = result.pushArray("values");
        values.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(result.get().queryPath(ArrayElement.class, "values")
                .get(0), myTestInstance.distinct(request));

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
        builder.setQuery(BuilderFactory.start().get());

        final Distinct request = builder.build();

        final Callback<ArrayElement> mockCountCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(ReplyArrayCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, request);

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
        builder.setQuery(BuilderFactory.start().get());

        final Distinct request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder values = result.pushArray("values");
        values.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");
        expectedCommand.addDocument("query", request.getQuery());

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(result.get().queryPath(ArrayElement.class, "values")
                .get(0), myTestInstance.distinctAsync(request).get());

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#distinctAsync(Callback, Distinct)} .
     */
    @Test
    public void testDistinctAsyncNoQuery() {
        final Distinct.Builder builder = new Distinct.Builder();
        builder.setKey("foo");

        final Distinct request = builder.build();

        final Callback<ArrayElement> mockCountCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("distinct", "test");
        expectedCommand.addString("key", "foo");

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(ReplyArrayCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.distinctAsync(mockCountCallback, request);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link MongoCollectionImpl#drop()}.
     */
    @Test
    public void testDrop() {

        final Document goodResult = BuilderFactory.start().addDouble("ok", 1.0)
                .get();
        final Document badResult = BuilderFactory.start().addLong("ok", 0)
                .get();
        final Document missingOkResult = BuilderFactory.start().get();

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
                .get();
        final Document badResult = BuilderFactory.start().addLong("ok", 0)
                .get();
        final Document missingOkResult = BuilderFactory.start().get();
        final Document options = BuilderFactory.start()
                .addString("index", "foo").get();

        expect(myMockDatabase.runCommand("deleteIndexes", "test", options))
                .andReturn(goodResult);
        expect(myMockDatabase.runCommand("deleteIndexes", "test", options))
                .andReturn(badResult);
        expect(myMockDatabase.runCommand("deleteIndexes", "test", options))
                .andReturn(missingOkResult);

        replay();

        assertTrue(myTestInstance.dropIndex("foo"));
        assertFalse(myTestInstance.dropIndex("foo"));
        assertFalse(myTestInstance.dropIndex("foo"));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAndModify(FindAndModify)} .
     */
    @Test
    public void testFindAndModify() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().get());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).get());

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(value.get(), myTestInstance.findAndModify(request));

        verify();
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#findAndModifyAsync(Callback, FindAndModify)} .
     */
    @Test
    public void testFindAndModifyAsyncCallbackOfDocumentFindAndModify() {
        final FindAndModify.Builder builder = new FindAndModify.Builder();
        builder.setQuery(BuilderFactory.start().get());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).get());

        final FindAndModify request = builder.build();

        final Callback<Document> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(ReplyDocumentCallback.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.findAndModifyAsync(mockCallback, request);

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
        builder.setQuery(BuilderFactory.start().get());
        builder.setUpdate(BuilderFactory.start().addInteger("foo", 3).get());

        final FindAndModify request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.push("value");
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("findAndModify", "test");
        expectedCommand.addDocument("query", request.getQuery());
        expectedCommand.addDocument("update", request.getUpdate());

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(value.get(), myTestInstance.findAndModifyAsync(request)
                .get());

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
        final Document result1 = BuilderFactory.start().get();
        final Document result2 = BuilderFactory.start().get();

        final Find.Builder builder = new Find.Builder();
        builder.setQuery(BuilderFactory.start().addInteger("foo", 1).get());
        builder.setReturnFields(BuilderFactory.start().addBoolean("_id", true)
                .get());
        builder.setNumberToReturn(101010);
        builder.setNumberToSkip(123456);
        builder.setPartialOk(true);
        builder.setReplicaOk(true);

        final Find request = builder.build();

        final Query message = new Query("test", "test", request.getQuery(),
                request.getReturnFields(), request.getNumberToReturn(),
                request.getNumberToSkip(), false, true, false, false, false,
                true);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result1, result2)));
        expectLastCall();

        replay();

        final Future<ClosableIterator<Document>> future = myTestInstance
                .findAsync(request);
        final ClosableIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#findAsync(Callback, Document)} .
     */
    @Test
    public void testFindAsyncCallbackOfClosableIteratorOfDocumentDocument() {
        final Callback<ClosableIterator<Document>> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Query message = new Query("test", "test", doc, null, 0, 0, false,
                false, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(QueryCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.findAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#findAsync(Callback, Find)}
     * .
     */
    @Test
    public void testFindAsyncCallbackOfClosableIteratorOfDocumentFind() {
        final Callback<ClosableIterator<Document>> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Query message = new Query("test", "test", doc, null, 0, 0, false,
                false, false, false, false, false);

        Find.Builder findBuilder = new Find.Builder(doc);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(QueryCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.findAsync(mockCountCallback, findBuilder.build());

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#findAsync(Document)}.
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindAsyncDocument() throws Exception {

        final Document result1 = BuilderFactory.start().get();
        final Document result2 = BuilderFactory.start().get();

        final Document doc = BuilderFactory.start().get();

        final Query message = new Query("test", "test", doc, null, 0, 0, false,
                false, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result1, result2)));
        expectLastCall();

        replay();

        final Future<ClosableIterator<Document>> future = myTestInstance
                .findAsync(doc);
        final ClosableIterator<Document> iter = future.get();
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
        final Document result1 = BuilderFactory.start().get();
        final Document result2 = BuilderFactory.start().get();

        final Document doc = BuilderFactory.start().get();

        final Query message = new Query("test", "test", doc, null, 0, 0, false,
                true, false, false, false, false);

        Find.Builder findBuilder = new Find.Builder(doc);
        findBuilder.setReplicaOk(true);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result1, result2)));
        expectLastCall();

        replay();

        final Future<ClosableIterator<Document>> future = myTestInstance
                .findAsync(findBuilder.build());
        final ClosableIterator<Document> iter = future.get();
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#find(Document)} .
     */
    @Test
    public void testFindDocument() {
        final Document result1 = BuilderFactory.start().get();
        final Document result2 = BuilderFactory.start().get();

        final Document doc = BuilderFactory.start().get();

        final Query message = new Query("test", "test", doc, null, 0, 0, false,
                false, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result1, result2)));
        expectLastCall();

        replay();

        final ClosableIterator<Document> iter = myTestInstance.find(doc);
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
        final Document result1 = BuilderFactory.start().get();
        final Document result2 = BuilderFactory.start().get();

        final Document doc = BuilderFactory.start().get();

        final Query message = new Query("test", "test", doc, null, 0, 0, false,
                true, false, false, false, false);

        Find.Builder findBuilder = new Find.Builder(doc);
        findBuilder.setReplicaOk(true);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result1, result2)));
        expectLastCall();

        replay();

        final ClosableIterator<Document> iter = myTestInstance.find(findBuilder
                .build());
        assertTrue(iter.hasNext());
        assertSame(result1, iter.next());
        assertTrue(iter.hasNext());
        assertSame(result2, iter.next());
        assertFalse(iter.hasNext());

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#findOne(Document)} .
     */
    @Test
    public void testFindOne() {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("foo", 2)
                .get();

        final Query message = new Query("test", "test", doc, null, 1, 0, false,
                false, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(replyDoc)));
        expectLastCall();

        replay(mockCountCallback);

        assertSame(replyDoc, myTestInstance.findOne(doc));

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#findOneAsync(Callback, Document)} .
     */
    @Test
    public void testFindOneAsyncCallbackOfDocumentDocument() {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Query message = new Query("test", "test", doc, null, 1, 0, false,
                false, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(QueryOneCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.findOneAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#findOneAsync(Document)}.
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testFindOneAsyncDocument() throws Exception {
        final Callback<Document> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("foo", 2)
                .get();

        final Query message = new Query("test", "test", doc, null, 1, 0, false,
                false, false, false, false, false);

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(replyDoc)));
        expectLastCall();

        replay(mockCountCallback);

        assertSame(replyDoc, myTestInstance.findOneAsync(doc).get());

        verify(mockCountCallback);
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
     * Test method for {@link AbstractMongoCollection#getName()}.
     */
    @Test
    public void testGetName() {
        replay();
        assertEquals("test", myTestInstance.getName());
        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#groupBy(GroupBy)} .
     */
    @Test
    public void testGroupBy() {
        final GroupBy.Builder builder = new GroupBy.Builder();
        builder.setKeys(Collections.singleton("foo"));

        final GroupBy request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder value = result.pushArray("retval");
        value.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(result.get().queryPath(ArrayElement.class, "retval")
                .get(0), myTestInstance.groupBy(request));

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(ReplyArrayCallback.class));
        expectLastCall();

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

        final GroupBy request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final ArrayBuilder value = result.pushArray("retval");
        value.push().addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        final DocumentBuilder group = expectedCommand.push("group");
        group.addString("ns", "test");
        group.push("key").addBoolean("foo", true);

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(result.get().queryPath(ArrayElement.class, "retval")
                .get(0), myTestInstance.groupByAsync(request).get());

        verify();
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
        builder.setInitialValue(BuilderFactory.start().get());
        builder.setQuery(BuilderFactory.start().addBoolean("foo", true).get());
        builder.setReduceFunction("reduce");

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(ReplyArrayCallback.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.groupByAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(boolean, Document[])} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncBooleanDocumentArray() throws Exception {
        final Document doc = BuilderFactory.start().addBoolean("_id", true)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Integer.valueOf(1), myTestInstance.insertAsync(true, doc)
                .get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(boolean, Durability, Document[])}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncBooleanDurabilityDocumentArray()
            throws Exception {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Integer.valueOf(1),
                myTestInstance.insertAsync(true, Durability.ACK, doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Callback, boolean, Document[])}
     * .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerBooleanDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, true, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#insertAsync(Callback, boolean, Durability, Document[])}
     * .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerBooleanDurabilityDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance
                .insertAsync(mockCountCallback, true, Durability.ACK, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Callback, Document[])} .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Callback, Durability, Document[])}
     * .
     */
    @Test
    public void testInsertAsyncCallbackOfIntegerDurabilityDocumentArray() {
        final Callback<Integer> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.insertAsync(mockCountCallback, Durability.ACK, doc);

        verify(mockCountCallback);
    }

    /**
     * Test method for {@link AbstractMongoCollection#insertAsync(Document[])} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncDocumentArray() throws Exception {
        final Document doc = BuilderFactory.start().get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.NONE);

        myMockClient.send(eq(message));
        expectLastCall();

        replay();

        assertEquals(Integer.valueOf(-1), myTestInstance.insertAsync(doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insertAsync(Durability, Document[])} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testInsertAsyncDurabilityDocumentArray() throws Exception {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Integer.valueOf(1),
                myTestInstance.insertAsync(Durability.ACK, doc).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(boolean, Document[])} .
     */
    @Test
    public void testInsertBooleanDocumentArray() {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(2, myTestInstance.insert(true, doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(boolean, Durability, Document[])} .
     */
    @Test
    public void testInsertBooleanDurabilityDocumentArray() {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(2, myTestInstance.insert(true, Durability.ACK, doc));

        verify();
    }

    /**
     * Test method for {@link AbstractMongoCollection#insert(Document[])} .
     */
    @Test
    public void testInsertDocumentArray() {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(2, myTestInstance.insert(doc));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#insert(Durability, Document[])} .
     */
    @Test
    public void testInsertDurabilityDocumentArray() {
        final Document doc = BuilderFactory.start().get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 2)
                .get();

        final Insert message = new Insert("test", "test",
                Collections.singletonList(doc), false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(2, myTestInstance.insert(Durability.ACK, doc));

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

        final MapReduce request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("results").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(Collections.singletonList(value.get()),
                myTestInstance.mapReduce(request));

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

        final MapReduce request = builder.build();

        final Callback<List<Document>> mockCallback = createMock(Callback.class);
        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

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

        final MapReduce request = builder.build();

        final DocumentBuilder result = BuilderFactory.start();
        final DocumentBuilder value = result.pushArray("results").push();
        value.addInteger("foo", 1);

        final DocumentBuilder expectedCommand = BuilderFactory.start();
        expectedCommand.addString("mapreduce", "test");
        expectedCommand.addJavaScript("map", "map");
        expectedCommand.addJavaScript("reduce", "reduce");
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), callback(reply(result.get())));
        expectLastCall();

        replay();

        assertEquals(Collections.singletonList(value.get()), myTestInstance
                .mapReduceAsync(request).get());

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
        builder.setQuery(BuilderFactory.start().addInteger("foo", 12).get());
        builder.setScope(BuilderFactory.start().addInteger("foo", 13).get());
        builder.setSort(BuilderFactory.start().addInteger("foo", 14).get());
        builder.setVerbose(true);

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
        expectedCommand.push("out").addInteger("inline", 1);

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

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

        final Command message = new Command("test", expectedCommand.get());

        expect(myMockDatabase.getName()).andReturn("test");

        myMockClient.send(eq(message), anyObject(MapReduceReplyCallback.class));
        expectLastCall();

        replay(mockCallback);

        myTestInstance.mapReduceAsync(mockCallback, request);

        verify(mockCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Callback, Document, Document)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocument() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Callback, Document, Document, boolean, boolean)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocumentBooleanBoolean() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update, true, true);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link MongoCollectionImpl#updateAsync(Callback, Document, Document, boolean, boolean, Durability)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocumentBooleanBooleanDurability() {

        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update, true, true,
                Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Callback, Document, Document, Durability)}
     * .
     */
    @Test
    public void testUpdateAsyncCallbackOfLongDocumentDocumentDurability() {
        final Callback<Long> mockCountCallback = createMock(Callback.class);
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                anyObject(ReplyLongCallback.class));
        expectLastCall();

        replay(mockCountCallback);

        myTestInstance.updateAsync(mockCountCallback, doc, update,
                Durability.ACK);

        verify(mockCountCallback);
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Document, Document)} .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocument() throws Exception {
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, false,
                false);

        expect(myMockDatabase.getName()).andReturn("test");
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.NONE);

        myMockClient.send(eq(message));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(-1L), myTestInstance.updateAsync(doc, update)
                .get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Document, Document, boolean, boolean)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocumentBooleanBoolean()
            throws Exception {
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(1L),
                myTestInstance.updateAsync(doc, update, true, true).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Document, Document, boolean, boolean, Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocumentBooleanBooleanDurability()
            throws Exception {

        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(
                Long.valueOf(1L),
                myTestInstance.updateAsync(doc, update, true, true,
                        Durability.ACK).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#updateAsync(Document, Document, Durability)}
     * .
     * 
     * @throws Exception
     *             On an error.
     */
    @Test
    public void testUpdateAsyncDocumentDocumentDurability() throws Exception {
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(Long.valueOf(1L),
                myTestInstance.updateAsync(doc, update, Durability.ACK).get());

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(Document, Document)} .
     */
    @Test
    public void testUpdateDocumentDocument() {
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L, myTestInstance.update(doc, update));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(Document, Document, boolean, boolean)}
     * .
     */
    @Test
    public void testUpdateDocumentDocumentBooleanBoolean() {
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);
        expect(myMockClient.getDefaultDurability()).andReturn(Durability.ACK);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L, myTestInstance.update(doc, update, true, true));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(Document, Document, boolean, boolean, Durability)}
     * .
     */
    @Test
    public void testUpdateDocumentDocumentBooleanBooleanDurability() {
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, true,
                true);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L,
                myTestInstance.update(doc, update, true, true, Durability.ACK));

        verify();
    }

    /**
     * Test method for
     * {@link AbstractMongoCollection#update(Document, Document, Durability)} .
     */
    @Test
    public void testUpdateDocumentDocumentDurability() {
        final Document doc = BuilderFactory.start().get();
        final Document update = BuilderFactory.start().addInteger("foo", 1)
                .get();
        final Document replyDoc = BuilderFactory.start().addInteger("n", 1)
                .get();

        final Update message = new Update("test", "test", doc, update, false,
                false);
        final GetLastError getLastError = new GetLastError("test", false,
                false, 0, 0);

        expect(myMockDatabase.getName()).andReturn("test").times(2);

        myMockClient.send(eq(message), eq(getLastError),
                callback(reply(replyDoc)));
        expectLastCall();

        replay();

        assertEquals(1L, myTestInstance.update(doc, update, Durability.ACK));

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
