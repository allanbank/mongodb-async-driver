/*
 * #%L
 * MongoIteratorTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.client.connection.CallbackReply.cb;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.MongoCursorControl;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.message.GetMore;
import com.allanbank.mongodb.client.message.KillCursors;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.CursorNotFoundException;

/**
 * MongoIteratorTest provides tests for the {@link MongoIteratorImpl} class.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoIteratorTest {

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
        myDocs.add(b.build());
        myDocs.add(b.build());
        myDocs.add(b.build());
        myDocs.add(b.build());
        myDocs.add(b.build());

        myQuery = new Query("db", "c", b.build(), b.build(), 5, 0, 0, false,
                ReadPreference.PRIMARY, false, false, false, false);

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
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testAllDocsInFirstReply() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertFalse(iter.hasNext());

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#asDocument()} .
     */
    @Test
    public void testAsDocument() {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "a.b");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456L);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);
        final Client mockClient = createMock(Client.class);

        replay();

        final MongoIteratorImpl iterImpl = new MongoIteratorImpl(b.build(),
                mockClient);

        assertThat(iterImpl.asDocument(), is(b.build()));

        iterImpl.close();

        verify();
    }

    /**
     * Test method for {@link MongoIteratorImpl#asDocument()} .
     */
    @Test
    public void testAsDocumentAlreadyClosed() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        iter.close();
        assertThat(iter.asDocument(), nullValue(Document.class));

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#asDocument()} .
     */
    @Test
    public void testAsDocumentAlreadyExhausted() {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, myQuery.getDatabaseName()
                + "." + myQuery.getCollectionName());
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 12345L);
        b.add(MongoCursorControl.SERVER_FIELD, myAddress);
        b.add(MongoCursorControl.LIMIT_FIELD, 0);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, myQuery.getBatchSize());

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        assertTrue(iter.hasNext());
        assertThat(iter.asDocument(), nullValue(Document.class));

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#asDocument()} .
     */
    @Test
    public void testAsDocumentNotStarted() {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, myQuery.getDatabaseName()
                + "." + myQuery.getCollectionName());
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 12345L);
        b.add(MongoCursorControl.SERVER_FIELD, myAddress);
        b.add(MongoCursorControl.LIMIT_FIELD, 0);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, myQuery.getBatchSize());

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 12345, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        assertThat(iter.asDocument(), is(b.build()));

        verify(mockClient);

        // Will call kill cursor.
        reset(mockClient);
        iter.close();
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testAskForMore() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertFalse(iter.hasNext());

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testAskForMoreGetCursorNotFound() {
        final List<Document> empty = Collections.emptyList();

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 0, 0, empty, false, true, false,
                false);
        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());

        try {
            iter.hasNext();
            fail("Should have thrown a CursorNotFound.");
        }
        catch (final CursorNotFoundException good) {
            assertThat(good.getMessage(), containsString("10"));
        }

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testAskForMoreGetNone() {
        final List<Document> empty = Collections.emptyList();

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 10, 0, empty, false, false, false,
                false);
        final Reply reply3 = new Reply(0, 0, 0, empty, false, false, false,
                false);

        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();
        mockClient.send(anyObject(GetMore.class), cb(reply3));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertFalse(iter.hasNext());

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testAskForMoreGetQueryFailed() {
        final List<Document> empty = Collections.emptyList();

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 0, 0, empty, false, false, true,
                false);
        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());

        try {
            iter.hasNext();
            fail("Should have thrown a CursorNotFound.");
        }
        catch (final CursorNotFoundException good) {
            assertThat(good.getMessage(), containsString("10"));
        }

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testAskForMoreThrowsOnInterrupt() {
        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        mockClient.send(anyObject(GetMore.class),
                anyObject(ReplyCallback.class));
        expectLastCall();
        mockClient.send(anyObject(KillCursors.class),
                isNull(ReplyCallback.class));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());

        try {
            Thread.currentThread().interrupt();
            iter.hasNext();
            fail("Should have thrown a RuntimeException.");
        }
        catch (final RuntimeException good) {
            assertThat(good.getMessage(), containsString("Interrupted"));
        }

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testAskForMoreWhenNoMore() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertFalse(iter.hasNext());

        try {
            iter.next();
            fail("Should have thrown an exception.");
        }
        catch (final NoSuchElementException nsee) {
            // Good.
        }

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#close()}.
     */
    @Test
    public void testCloseWithoutReading() {
        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        mockClient.send(anyObject(KillCursors.class),
                isNull(ReplyCallback.class));
        expectLastCall();

        replay(mockClient);

        final MongoIterator<Document> iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#close()}.
     */
    @Test
    public void testCloseWithPending() {
        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();
        mockClient.send(anyObject(KillCursors.class),
                isNull(ReplyCallback.class));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#nextBatchSize()}.
     */
    @Test
    public void testNextBatchSize() {
        final int batchSize = 5;
        int limit = 100;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        MongoIteratorImpl iter = new MongoIteratorImpl(myQuery, mockClient,
                myAddress, reply);
        assertEquals(batchSize, iter.nextBatchSize());
        iter.close();

        limit = 5;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        iter = new MongoIteratorImpl(myQuery, mockClient, myAddress, reply);
        assertEquals(limit, iter.nextBatchSize());
        iter.close();

        limit = -1;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        iter = new MongoIteratorImpl(myQuery, mockClient, myAddress, reply);
        assertEquals(batchSize, iter.nextBatchSize());

        iter.close();
        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testOverLimit() {
        final int batchSize = 5;
        final int limit = 4;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        mockClient.send(anyObject(KillCursors.class),
                isNull(ReplyCallback.class));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertFalse(iter.hasNext());

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#MongoIteratorImpl} .
     */
    @Test
    public void testOverLimitCursorAlreadyDead() {
        final int batchSize = 5;
        final int limit = 4;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertFalse(iter.hasNext());

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#remove()}.
     */
    @Test
    public void testRemove() {
        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        try {
            iter.remove();
            fail("Should throw an exception.");
        }
        catch (final UnsupportedOperationException uoe) {
            // Good.
        }

        iter.close();
        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#setBatchSize(int)}.
     */
    @Test
    public void testSetBatchSize() {
        final int batchSize = 5;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                0, 0, false, ReadPreference.PRIMARY, false, false, false, false);

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        assertEquals(batchSize, iter.getBatchSize());
        iter.setBatchSize(10);
        assertEquals(10, iter.getBatchSize());

        iter.close();
        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#stop()} .
     */
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
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        iter.stop();
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertFalse(iter.hasNext());

        assertThat(iter.asDocument(), is(b.build()));

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#remove()}.
     */
    @Test
    public void testTailableCursor() {
        final DocumentBuilder b = BuilderFactory.start();

        final List<Document> empty = Collections.emptyList();

        myQuery = new Query("db", "c", b.build(), b.build(), 5, 0, 0, true,
                ReadPreference.PRIMARY, false, false, false, false);

        final Client mockClient = createStrictMock(Client.class);
        final Reply reply = new Reply(0, 1234, 0, myDocs, false, false, false,
                false);
        final Reply reply0 = new Reply(0, 1234, 0, empty, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 1234, 0, myDocs, false, false, false,
                false);

        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall().times(100);
        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();
        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall().times(100);
        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall(); // To load data for the last assertTrue
        // hasNext.
        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall(); // Request for more data after reading
        // the last batch.
        mockClient.send(anyObject(KillCursors.class),
                isNull(ReplyCallback.class));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertTrue(iter.hasNext());

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#remove()}.
     */
    @Test
    public void testTailableCursorDoesNotExhaustTheStack() {
        final DocumentBuilder b = BuilderFactory.start();

        final List<Document> empty = Collections.emptyList();

        myQuery = new Query("db", "c", b.build(), b.build(), 5, 0, 0, true,
                ReadPreference.PRIMARY, false, false, false, false);

        final Client mockClient = createStrictMock(Client.class);
        final Reply reply = new Reply(0, 1234, 0, myDocs, false, false, false,
                false);
        final Reply reply0 = new Reply(0, 1234, 0, empty, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 1234, 0, myDocs, false, false, false,
                false);

        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall().times(1000000);
        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();
        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall().times(100);
        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall(); // To load data for the last assertTrue
        // hasNext.
        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall(); // Request for more data after reading
        // the last batch.
        mockClient.send(anyObject(KillCursors.class),
                isNull(ReplyCallback.class));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertTrue(iter.hasNext());

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#remove()}.
     */
    @Test
    public void testTailableCursorFailure() {
        final DocumentBuilder b = BuilderFactory.start();

        final List<Document> empty = Collections.emptyList();

        myQuery = new Query("db", "c", b.build(), b.build(), 5, 0, 0, true,
                ReadPreference.PRIMARY, false, false, false, false);

        final Client mockClient = createStrictMock(Client.class);
        final Reply reply = new Reply(0, 1234, 0, myDocs, false, false, false,
                false);
        final Reply reply0 = new Reply(0, 1234, 0, empty, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 1234, 0, myDocs, false, false, false,
                false);
        final Reply replyDone = new Reply(0, 0, 0, empty, false, false, false,
                false);

        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall().times(100);
        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();
        mockClient.send(anyObject(GetMore.class), cb(reply0));
        expectLastCall().times(100);
        mockClient.send(anyObject(GetMore.class), cb(replyDone));
        expectLastCall();

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);
        assertSame(iter, iter.iterator());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(0), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(1), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(2), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(3), iter.next());
        assertTrue(iter.hasNext());
        assertSame(myDocs.get(4), iter.next());
        assertFalse(iter.hasNext());

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#toArray()} .
     */
    @Test
    public void testToArray() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        assertThat(iter.toArray(), arrayContaining(myDocs.toArray()));

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#toArray} .
     */
    @Test
    public void testToArrayTyped() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        assertThat(iter.toArray(new Document[0]),
                arrayContaining(myDocs.toArray()));

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIteratorImpl#toList()} .
     */
    @Test
    public void testToList() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIteratorImpl iter = new MongoIteratorImpl(myQuery,
                mockClient, myAddress, reply);

        assertThat(iter.toList(), is(myDocs));

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for
     * {@link MongoIteratorImpl#MongoIteratorImpl(Document, Client)} .
     */
    @SuppressWarnings("boxing")
    @Test
    public void testWithCursorDocButNoDotInName() {

        final DocumentBuilder b = BuilderFactory.start();
        b.add(MongoCursorControl.NAME_SPACE_FIELD, "ab");
        b.add(MongoCursorControl.CURSOR_ID_FIELD, 123456);
        b.add(MongoCursorControl.SERVER_FIELD, "server");
        b.add(MongoCursorControl.LIMIT_FIELD, 4321);
        b.add(MongoCursorControl.BATCH_SIZE_FIELD, 23);
        final Client mockClient = createMock(Client.class);

        replay(mockClient);

        final MongoIteratorImpl iterImpl = new MongoIteratorImpl(b.build(),
                mockClient);

        assertThat(iterImpl.getBatchSize(), is(23));
        assertThat(iterImpl.getLimit(), is(4321));
        assertThat(iterImpl.getCursorId(), is(123456L));
        assertThat(iterImpl.getDatabaseName(), is("ab"));
        assertThat(iterImpl.getCollectionName(), is("ab"));
        assertThat(iterImpl.getClient(), is(mockClient));
        assertThat(iterImpl.getReadPerference(),
                is(ReadPreference.server("server")));

        verify(mockClient);

        // Will call kill cursor.
        reset(mockClient);
        iterImpl.close();
    }
}
