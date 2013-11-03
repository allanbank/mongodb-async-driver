/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.notNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.StreamCallback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.MongoIteratorImpl;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;

/**
 * CursorStreamingCallbackTest provides test for the
 * {@link CursorStreamingCallback} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CursorStreamingCallbackTest {

    /** The address for the test. */
    private String myAddress = null;

    /** A set of documents for the test. */
    private List<Document> myDocs = null;

    /** A set of documents for the test. */
    private Query myQuery = null;

    /**
     * Creates a set of documents for the test.
     */
    @Before
    public void setUp() {
        final DocumentBuilder b = BuilderFactory.start();
        myDocs = new ArrayList<Document>();
        myDocs.add(b.add("a", 1).build());
        myDocs.add(b.reset().add("a", 2).build());
        myDocs.add(b.reset().add("a", 3).build());
        myDocs.add(b.reset().add("a", 4).build());
        myDocs.add(b.reset().add("a", 5).build());

        myQuery = new Query("db", "c", b.reset().build(), b.build(), 5, 0, 0,
                false, ReadPreference.PRIMARY, false, false, false, false);

        myAddress = "localhost:21017";
    }

    /**
     * Cleans up after the test.
     */
    @After
    public void tearDown() {
        myDocs = null;
        myQuery = null;
        myAddress = null;
    }

    /**
     * Test method for {@link CursorStreamingCallback} getting all of the
     * documents in one batch.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAllDocsInFirstReply() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback#asDocument()} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsDocument() {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, myQuery.getDatabaseName()
                + "." + myQuery.getCollectionName());
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456L);
        b.add(MongoCursorControl.SERVER_FIELD, myAddress);
        b.add(MongoCursorControl.LIMIT_FIELD, 0);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, myQuery.getBatchSize());

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        final Reply reply = new Reply(0, 123456, 0, myDocs, false, false,
                false, false);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }

        expect(mockClient.send(anyObject(Message.class), eq(qsCallback)))
                .andReturn(myAddress);
        expect(
                mockClient.send(anyObject(KillCursors.class),
                        isNull(Callback.class))).andReturn(myAddress);

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        assertThat(qsCallback.asDocument(), is(b.build()));
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback#asDocument()} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsDocumentAlreadyClosed() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.close();
        assertThat(qsCallback.asDocument(), nullValue(Document.class));

        verify(mockClient);
    }

    /**
     * Test method for {@link CursorStreamingCallback#asDocument()} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsDocumentAlreadyExhausted() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        assertThat(qsCallback.asDocument(), nullValue(Document.class));
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} requesting the second
     * batch.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAskForMore() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        expect(mockClient.send(anyObject(GetMore.class), eq(qsCallback)))
                .andReturn(myAddress);
        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.callback(reply2);

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} requesting the second and
     * third batch both of which are empty.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAskForMoreGetNone() {
        final List<Document> empty = Collections.emptyList();

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 10, 0, empty, false, false, false,
                false);
        final Reply reply3 = new Reply(0, 0, 0, empty, false, false, false,
                false);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        expect(mockClient.send(anyObject(GetMore.class), eq(qsCallback)))
                .andReturn(myAddress);
        expect(mockClient.send(anyObject(GetMore.class), eq(qsCallback)))
                .andReturn(myAddress);
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.callback(reply2);
        qsCallback.callback(reply3);

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} getting all of the
     * documents in one batch.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCloseBeforeReply() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        expect(
                mockClient.send(anyObject(KillCursors.class),
                        isNull(Callback.class))).andReturn(myAddress);

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.close();
        qsCallback.callback(reply);

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} getting all of the
     * documents in one batch.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testErrorReply() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, true,
                false);

        mockCallback.exception(notNull(Throwable.class));
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} getting all of the
     * documents in one batch.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testErrorReplyWithMessage() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Document replyDoc = BuilderFactory.start().add("ok", 0)
                .add("$err", "This is an error").build();
        final Reply reply = new Reply(0, 0, 0,
                Collections.singletonList(replyDoc), false, false, false, false);

        mockCallback.exception(notNull(Throwable.class));
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} getting all of the
     * documents in one batch.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testLateSetAddress() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.callback(reply);
        qsCallback.setAddress(myAddress);
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback#nextBatchSize()}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testNextBatchSize() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        replay(mockClient, mockCallback);

        final int batchSize = 5;
        int limit = 100;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);
        assertEquals(batchSize, qsCallback.nextBatchSize());
        qsCallback.close();

        limit = 5;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        qsCallback = new CursorStreamingCallback(mockClient, myQuery, false,
                mockCallback);
        assertEquals(limit, qsCallback.nextBatchSize());
        qsCallback.close();

        limit = -1;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        qsCallback = new CursorStreamingCallback(mockClient, myQuery, false,
                mockCallback);
        assertEquals(batchSize, qsCallback.nextBatchSize());

        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testOverLimit() {
        final int batchSize = 5;
        final int limit = 4;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        mockCallback.callback(myDocs.get(0));
        expectLastCall();
        mockCallback.callback(myDocs.get(1));
        expectLastCall();
        mockCallback.callback(myDocs.get(2));
        expectLastCall();
        mockCallback.callback(myDocs.get(3));
        expectLastCall();
        mockCallback.done();
        expectLastCall();
        expect(
                mockClient.send(anyObject(KillCursors.class),
                        isNull(Callback.class))).andReturn(myAddress);

        replay(mockClient, mockCallback);

        assertNull(qsCallback.getAddress());
        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testOverLimitCursorAlreadyDead() {
        final int batchSize = 5;
        final int limit = 4;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        mockCallback.callback(myDocs.get(0));
        expectLastCall();
        mockCallback.callback(myDocs.get(1));
        expectLastCall();
        mockCallback.callback(myDocs.get(2));
        expectLastCall();
        mockCallback.callback(myDocs.get(3));
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback#setBatchSize(int)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testSetBatchSize() {
        final int batchSize = 5;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                0, 0, false, ReadPreference.PRIMARY, false, false, false, false);

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        replay(mockClient, mockCallback);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        assertEquals(batchSize, qsCallback.getBatchSize());
        qsCallback.setBatchSize(10);
        assertEquals(10, qsCallback.getBatchSize());

        qsCallback.close();
        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback#stop()} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testStop() {
        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, myQuery.getDatabaseName()
                + "." + myQuery.getCollectionName());
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 10L);
        b.add(MongoCursorControl.SERVER_FIELD, myAddress);
        b.add(MongoCursorControl.LIMIT_FIELD, 0);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, myQuery.getBatchSize());

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }

        replay(mockClient, mockCallback);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);
        qsCallback.setAddress(myAddress);
        qsCallback.stop();
        qsCallback.callback(reply);
        assertThat(qsCallback.asDocument(), is(b.build()));

        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} getting all of the
     * documents in one batch.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testTerminateStreamOnRuntimeExceptionInCallback() {

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);
        final RuntimeException error = new RuntimeException("Injected!");

        mockCallback.callback(myDocs.get(0));
        expectLastCall();
        mockCallback.callback(myDocs.get(1));
        expectLastCall().andThrow(error);
        mockCallback.exception(error);
        expectLastCall();

        replay(mockClient, mockCallback);

        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.close();

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for {@link CursorStreamingCallback} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testUnderLimit() {
        final int batchSize = 5;
        final int limit = 7;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        final CursorStreamingCallback qsCallback = new CursorStreamingCallback(
                mockClient, myQuery, false, mockCallback);

        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        for (final Document doc : myDocs) {
            mockCallback.callback(doc);
            expectLastCall();
        }
        expect(mockClient.send(anyObject(GetMore.class), eq(qsCallback)))
                .andReturn(myAddress);
        mockCallback.callback(myDocs.get(0));
        expectLastCall();
        mockCallback.callback(myDocs.get(1));
        expectLastCall();
        mockCallback.done();
        expectLastCall();

        replay(mockClient, mockCallback);

        assertNull(qsCallback.getAddress());
        qsCallback.setAddress(myAddress);
        qsCallback.callback(reply);
        qsCallback.callback(reply2);

        verify(mockClient, mockCallback);
    }

    /**
     * Test method for
     * {@link MongoIteratorImpl#MongoIteratorImpl(Document, Client)} .
     */
    @SuppressWarnings({ "unchecked", "boxing" })
    @Test
    public void testWithCursorDocButNoDotInName() {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "ab");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);
        final Client mockClient = createMock(Client.class);
        final StreamCallback<Document> mockCallback = createMock(StreamCallback.class);

        expect(
                mockClient.send(anyObject(KillCursors.class),
                        isNull(Callback.class))).andReturn(myAddress);

        replay(mockClient, mockCallback);

        final CursorStreamingCallback iterImpl = new CursorStreamingCallback(
                mockClient, b.build(), mockCallback);

        assertThat(iterImpl.getBatchSize(), is(23));
        assertThat(iterImpl.getLimit(), is(4321));
        assertThat(iterImpl.getCursorId(), is(123456L));
        assertThat(iterImpl.getDatabaseName(), is("ab"));
        assertThat(iterImpl.getCollectionName(), is("ab"));
        assertThat(iterImpl.getClient(), is(mockClient));
        assertThat(iterImpl.getAddress(), is("server"));

        iterImpl.close();

        verify(mockClient, mockCallback);
    }
}
