/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static com.allanbank.mongodb.connection.CallbackReply.cb;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

    /** A set of documents for the test. */
    private List<Document> myDocs = null;

    /** A set of documents for the test. */
    private Query myQuery = null;

    /**
     * Creates a set of documents for the test.
     */
    @Before
    public void setUp() {
        DocumentBuilder b = BuilderFactory.start();
        myDocs = new ArrayList<Document>();
        myDocs.add(b.build());
        myDocs.add(b.build());
        myDocs.add(b.build());
        myDocs.add(b.build());
        myDocs.add(b.build());

        myQuery = new Query("db", "c", b.build(), b.build(), 5, 0, 0, false,
                ReadPreference.PRIMARY, false, false, false, false);
    }

    /**
     * Cleans up after the test.
     */
    @After
    public void tearDown() {
        myDocs = null;
        myQuery = null;
    }

    /**
     * Test method for {@link MongoIterator#MongoIterator(Query, Client, Reply)}
     * .
     */
    @Test
    public void testAllDocsInFirstReply() {

        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 0, 0, myDocs, false, false, false, false);

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);
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
     * Test method for {@link MongoIterator#MongoIterator(Query, Client, Reply)}
     * .
     */
    @Test
    public void testAskForMoreWhenNoMore() {

        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 0, 0, myDocs, false, false, false, false);

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);
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
        catch (NoSuchElementException nsee) {
            // Good.
        }
        
        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIterator#MongoIterator(Query, Client, Reply)}
     * .
     */
    @Test
    public void testOverLimit() {
        int batchSize = 5;
        int limit = 4;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 10, 0, myDocs, false, false, false, false);

        mockClient.send(anyObject(KillCursors.class));
        expectLastCall();

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);
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
     * Test method for {@link MongoIterator#MongoIterator(Query, Client, Reply)}
     * .
     */
    @Test
    public void testOverLimitCursorAlreadyDead() {
        int batchSize = 5;
        int limit = 4;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 0, 0, myDocs, false, false, false, false);

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);
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
     * Test method for {@link MongoIterator#MongoIterator(Query, Client, Reply)}
     * .
     */
    @Test
    public void testAskForMore() {

        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 10, 0, myDocs, false, false, false, false);
        Reply reply2 = new Reply(0, 0, 0, myDocs, false, false, false, false);

        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);
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
     * Test method for {@link MongoIterator#close()}.
     */
    @Test
    public void testCloseWithPending() {
        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 10, 0, myDocs, false, false, false, false);
        Reply reply2 = new Reply(0, 10, 0, myDocs, false, false, false, false);

        mockClient.send(anyObject(GetMore.class), cb(reply2));
        expectLastCall();
        mockClient.send(anyObject(KillCursors.class));
        expectLastCall();

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);
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
     * Test method for {@link MongoIterator#close()}.
     */
    @Test
    public void testCloseWithoutReading() {
        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 10, 0, myDocs, false, false, false, false);

        mockClient.send(anyObject(KillCursors.class));
        expectLastCall();

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);

        iter.close();

        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIterator#nextBatchSize()}.
     */
    @Test
    public void testNextBatchSize() {
        int batchSize = 5;
        int limit = 100;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);

        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 0, 0, myDocs, false, false, false, false);

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);
        assertEquals(batchSize, iter.nextBatchSize());

        limit = 5;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        iter = new MongoIterator(myQuery, mockClient, reply);
        assertEquals(-limit, iter.nextBatchSize());

        limit = -1;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                limit, 0, false, ReadPreference.PRIMARY, false, false, false,
                false);
        iter = new MongoIterator(myQuery, mockClient, reply);
        assertEquals(batchSize, iter.nextBatchSize());

        iter.close();
        verify(mockClient);
    }

    /**
     * Test method for {@link MongoIterator#remove()}.
     */
    @Test
    public void testRemove() {
        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 0, 0, myDocs, false, false, false, false);

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);

        try {
            iter.remove();
            fail("Should throw an exception.");
        }
        catch (UnsupportedOperationException uoe) {
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
        int batchSize = 5;
        myQuery = new Query("db", "c", myDocs.get(0), myDocs.get(0), batchSize,
                0, 0, false, ReadPreference.PRIMARY, false, false, false, false);

        Client mockClient = createMock(Client.class);
        Reply reply = new Reply(0, 0, 0, myDocs, false, false, false, false);

        replay(mockClient);

        MongoIterator iter = new MongoIterator(myQuery, mockClient, reply);

        assertEquals(batchSize, iter.getBatchSize());
        iter.setBatchSize(10);
        assertEquals(10, iter.getBatchSize());

        iter.close();
        verify(mockClient);
    }
}
