/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Header;
import com.allanbank.mongodb.connection.message.Reply;

/**
 * ReplyTest provides tests for the {@link Reply} message.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyTest {

    /**
     * Test method for {@link Reply#equals(Object)}.
     */
    @Test
    @SuppressWarnings({ "unchecked", "boxing" })
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().get();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).get();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).get();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).get();

        final List<Document>[] docs = new List[4];
        docs[0] = new ArrayList<Document>();
        docs[0].add(doc1);
        docs[1] = new ArrayList<Document>();
        docs[1].add(doc1);
        docs[1].add(doc2);
        docs[2] = new ArrayList<Document>();
        docs[2].add(doc1);
        docs[2].add(doc2);
        docs[2].add(doc3);
        docs[3] = new ArrayList<Document>();
        docs[3].add(doc1);
        docs[3].add(doc2);
        docs[3].add(doc3);
        docs[3].add(doc4);

        final Integer[] ints = new Integer[5];
        for (int i = 0; i < ints.length; ++i) {
            ints[i] = Integer.valueOf(random.nextInt());
        }
        final Long[] longs = new Long[5];
        for (int i = 0; i < longs.length; ++i) {
            longs[i] = Long.valueOf(random.nextLong());
        }

        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        for (final int responseToId : ints) {
            for (final long cursorId : longs) {
                for (final int cursorOffset : ints) {
                    for (final List<Document> results : docs) {
                        final boolean awaitCapable = random.nextBoolean();
                        final boolean cursorNotFound = random.nextBoolean();
                        final boolean queryFailed = random.nextBoolean();
                        final boolean shardConfigStale = random.nextBoolean();

                        objs1.add(new Reply(responseToId, cursorId,
                                cursorOffset, results, awaitCapable,
                                cursorNotFound, queryFailed, shardConfigStale));
                        objs2.add(new Reply(responseToId, cursorId,
                                cursorOffset, results, awaitCapable,
                                cursorNotFound, queryFailed, shardConfigStale));
                    }
                }
            }
        }
        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Message obj1 = objs1.get(i);
            Message obj2 = objs2.get(i);

            assertTrue(obj1.equals(obj1));
            assertNotSame(obj1, obj2);
            assertEquals(obj1, obj2);

            assertEquals(obj1.hashCode(), obj2.hashCode());

            for (int j = i + 1; j < objs1.size(); ++j) {
                obj2 = objs2.get(j);

                assertFalse(obj1.equals(obj2));
                assertFalse(obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
            assertFalse(obj1.equals(new Command(obj1.getDatabaseName(), doc1)));
        }
    }

    /**
     * Test method for {@link Reply#Reply(Header, BsonInputStream)}.
     * 
     * @throws IOException
     *             On a test failure.
     */
    @SuppressWarnings({ "boxing", "unchecked" })
    @Test
    public void testReplyHeaderBsonInputStream() throws IOException {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().get();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).get();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).get();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).get();

        final List<Document>[] docs = new List[4];
        docs[0] = new ArrayList<Document>();
        docs[0].add(doc1);
        docs[1] = new ArrayList<Document>();
        docs[1].add(doc1);
        docs[1].add(doc2);
        docs[2] = new ArrayList<Document>();
        docs[2].add(doc1);
        docs[2].add(doc2);
        docs[2].add(doc3);
        docs[3] = new ArrayList<Document>();
        docs[3].add(doc1);
        docs[3].add(doc2);
        docs[3].add(doc3);
        docs[3].add(doc4);

        final Integer[] ints = new Integer[5];
        for (int i = 0; i < ints.length; ++i) {
            ints[i] = Integer.valueOf(random.nextInt());
        }
        final Long[] longs = new Long[5];
        for (int i = 0; i < longs.length; ++i) {
            longs[i] = Long.valueOf(random.nextLong());
        }

        for (final int responseToId : ints) {
            for (final long cursorId : longs) {
                for (final int cursorOffset : ints) {
                    for (final List<Document> results : docs) {
                        final boolean awaitCapable = random.nextBoolean();
                        final boolean cursorNotFound = random.nextBoolean();
                        final boolean queryFailed = random.nextBoolean();
                        final boolean shardConfigStale = random.nextBoolean();

                        final Reply message = new Reply(responseToId, cursorId,
                                cursorOffset, results, awaitCapable,
                                cursorNotFound, queryFailed, shardConfigStale);
                        final ByteArrayOutputStream out = new ByteArrayOutputStream();
                        final BsonOutputStream bOut = new BsonOutputStream(out);

                        message.write(1234, bOut);

                        final ByteArrayInputStream in = new ByteArrayInputStream(
                                out.toByteArray());
                        final BsonInputStream bIn = new BsonInputStream(in);

                        final Header header = new Header(bIn);

                        assertEquals(Operation.REPLY, header.getOperation());
                        assertEquals(1234, header.getRequestId());
                        assertEquals(message.getResponseToId(),
                                header.getResponseId());
                        assertEquals(out.size(), header.getLength());

                        final Reply read = new Reply(header, bIn);

                        assertEquals(message, read);

                    }
                }
            }
        }
    }

    /**
     * Test method for
     * {@link Reply#Reply(int, long, int, List, boolean, boolean, boolean, boolean)}
     * .
     */
    @Test
    public void testReplyIntLongIntListOfDocumentBooleanBooleanBooleanBoolean() {
        final Random random = new Random(System.currentTimeMillis());
        final Document doc1 = BuilderFactory.start().get();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).get();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).get();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).get();

        final List<Document> docs = new ArrayList<Document>();
        docs.add(doc1);
        docs.add(doc2);
        docs.add(doc3);
        docs.add(doc4);

        final int responseToId = random.nextInt();
        final long cursorId = random.nextLong();
        final int cursorOffset = random.nextInt();
        final List<Document> results = docs;
        final boolean awaitCapable = random.nextBoolean();
        final boolean cursorNotFound = random.nextBoolean();
        final boolean queryFailed = random.nextBoolean();
        final boolean shardConfigStale = random.nextBoolean();

        final Reply message = new Reply(responseToId, cursorId, cursorOffset,
                results, awaitCapable, cursorNotFound, queryFailed,
                shardConfigStale);

        assertEquals("", message.getDatabaseName());
        assertEquals("", message.getCollectionName());
        assertEquals(responseToId, message.getResponseToId());
        assertEquals(cursorId, message.getCursorId());
        assertEquals(cursorOffset, message.getCursorOffset());
        assertEquals(results, message.getResults());
        assertEquals(awaitCapable, message.isAwaitCapable());
        assertEquals(cursorNotFound, message.isCursorNotFound());
        assertEquals(queryFailed, message.isQueryFailed());
        assertEquals(shardConfigStale, message.isShardConfigStale());
    }

}
