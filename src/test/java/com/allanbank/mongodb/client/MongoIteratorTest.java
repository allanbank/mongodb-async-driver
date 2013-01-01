/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.connection.CallbackReply.cb;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.message.GetMore;
import com.allanbank.mongodb.connection.message.KillCursors;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * MongoIteratorTest provides tests for the {@link MongoIterator} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
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
     * Test method for
     * {@link MongoIterator#MongoIterator(Query, Client, String, Reply)} .
     */
    @Test
    public void testAllDocsInFirstReply() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for
     * {@link MongoIterator#MongoIterator(Query, Client, String, Reply)} .
     */
    @Test
    public void testAskForMore() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for
     * {@link MongoIterator#MongoIterator(Query, Client, String, Reply)} .
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

        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply3)))
                .andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for
     * {@link MongoIterator#MongoIterator(Query, Client, String, Reply)} .
     */
    @Test
    public void testAskForMoreWhenNoMore() {

        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for {@link MongoIterator#close()}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCloseWithoutReading() {
        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        expect(
                mockClient.send(anyObject(KillCursors.class),
                        isNull(Callback.class))).andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIterator#close()}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCloseWithPending() {
        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);
        final Reply reply2 = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress);
        expect(
                mockClient.send(anyObject(KillCursors.class),
                        isNull(Callback.class))).andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for {@link MongoIterator#nextBatchSize()}.
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

        MongoIterator iter = new MongoIterator(myQuery, mockClient, myAddress,
                reply);
        assertEquals(batchSize, iter.nextBatchSize());

        limit = 5;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        iter = new MongoIterator(myQuery, mockClient, myAddress, reply);
        assertEquals(-limit, iter.nextBatchSize());

        limit = -1;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        iter = new MongoIterator(myQuery, mockClient, myAddress, reply);
        assertEquals(batchSize, iter.nextBatchSize());

        iter.close();
        verify(mockClient);
    }

    /**
     * Test method for
     * {@link MongoIterator#MongoIterator(Query, Client, String, Reply)} .
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
        final Reply reply = new Reply(0, 10, 0, myDocs, false, false, false,
                false);

        expect(
                mockClient.send(anyObject(KillCursors.class),
                        isNull(Callback.class))).andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for
     * {@link MongoIterator#MongoIterator(Query, Client, String, Reply)} .
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

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for {@link MongoIterator#remove()}.
     */
    @Test
    public void testRemove() {
        final Client mockClient = createMock(Client.class);
        final Reply reply = new Reply(0, 0, 0, myDocs, false, false, false,
                false);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);

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
     * Test method for {@link MongoIterator#setBatchSize(int)}.
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

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);

        assertEquals(batchSize, iter.getBatchSize());
        iter.setBatchSize(10);
        assertEquals(10, iter.getBatchSize());

        iter.close();
        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIterator#remove()}.
     */
    @SuppressWarnings("unchecked")
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

        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress).times(100);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress).times(100);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress); // To load data for the last assertTrue
                                       // hasNext.
        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress); // Request for more data after reading
                                       // the last batch.
        expect(
                mockClient.send(anyObject(KillCursors.class),
                        (Callback<Reply>) isNull())).andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for {@link MongoIterator#remove()}.
     */
    @SuppressWarnings("unchecked")
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

        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress).times(1000000);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress).times(100);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress); // To load data for the last assertTrue
                                       // hasNext.
        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress); // Request for more data after reading
                                       // the last batch.
        expect(
                mockClient.send(anyObject(KillCursors.class),
                        (Callback<Reply>) isNull())).andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
     * Test method for {@link MongoIterator#remove()}.
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

        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress).times(100);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply2)))
                .andReturn(myAddress);
        expect(mockClient.send(anyObject(GetMore.class), cb(reply0)))
                .andReturn(myAddress).times(100);
        expect(mockClient.send(anyObject(GetMore.class), cb(replyDone)))
                .andReturn(myAddress);

        replay(mockClient);

        final MongoIterator iter = new MongoIterator(myQuery, mockClient,
                myAddress, reply);
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
}
