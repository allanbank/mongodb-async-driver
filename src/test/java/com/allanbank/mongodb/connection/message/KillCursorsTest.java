/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
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

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;

/**
 * KillCursorsTest provides tests for the {@link KillCursors} message.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class KillCursorsTest {

    /**
     * Test method for {@link KillCursors#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        for (int i = 0; i < 1000; ++i) {
            final long cursorId = random.nextLong();

            objs1.add(new KillCursors(new long[] { cursorId }));
            objs2.add(new KillCursors(new long[] { cursorId }));
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
            assertFalse(obj1.equals(new Command(obj1.getDatabaseName(),
                    BuilderFactory.start().get())));
        }
    }

    /**
     * Test method for {@link KillCursors#KillCursors(BsonInputStream)} .
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testKillCursorsBsonInputStream() throws IOException {
        final Random random = new Random(System.currentTimeMillis());

        for (int i = 0; i < 1000; ++i) {

            KillCursors message = null;
            if (random.nextBoolean()) {
                message = new KillCursors(new long[] { random.nextLong() });
            }
            else {
                message = new KillCursors(new long[] { random.nextLong(),
                        random.nextLong() });
            }

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final BsonOutputStream bOut = new BsonOutputStream(out);

            message.write(1234, bOut);

            final ByteArrayInputStream in = new ByteArrayInputStream(
                    out.toByteArray());
            final BsonInputStream bIn = new BsonInputStream(in);

            final Header header = new Header(bIn);

            assertEquals(Operation.KILL_CURSORS, header.getOperation());
            assertEquals(1234, header.getRequestId());
            assertEquals(0, header.getResponseId());
            assertEquals(out.size(), header.getLength());

            final Message read = new KillCursors(bIn);

            assertEquals(message, read);

        }
    }

    /**
     * Test method for {@link KillCursors#KillCursors(long[])} .
     */
    @Test
    public void testKillCursorsLongArray() {

        final long[] ids = new long[] { 1234 };
        final KillCursors message = new KillCursors(ids);

        assertEquals("", message.getDatabaseName());
        assertEquals("", message.getCollectionName());
        assertNotSame(ids, message.getCursorIds());
        assertArrayEquals(ids, message.getCursorIds());

        ids[0] = 2345;
        assertArrayEquals(new long[] { 1234 }, message.getCursorIds());
    }
}
