/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * KillCursorsTest provides tests for the {@link KillCursors} message.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
            for (final ReadPreference prefs : Arrays.asList(
                    ReadPreference.PRIMARY, ReadPreference.SECONDARY)) {
                final long cursorId = random.nextLong();

                objs1.add(new KillCursors(new long[] { cursorId }, prefs));
                objs2.add(new KillCursors(new long[] { cursorId }, prefs));
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
            assertFalse(obj1.equals(new Command(obj1.getDatabaseName(),
                    BuilderFactory.start().build())));
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
                message = new KillCursors(new long[] { random.nextLong() },
                        ReadPreference.PRIMARY);
            }
            else {
                message = new KillCursors(new long[] { random.nextLong(),
                        random.nextLong() }, ReadPreference.PRIMARY);
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
     * Test method for {@link KillCursors#KillCursors(long[], ReadPreference)} .
     */
    @Test
    public void testKillCursorsLongArray() {

        final long[] ids = new long[] { 1234 };
        final KillCursors message = new KillCursors(ids, ReadPreference.PRIMARY);

        assertEquals("", message.getDatabaseName());
        assertEquals("", message.getCollectionName());
        assertNotSame(ids, message.getCursorIds());
        assertArrayEquals(ids, message.getCursorIds());

        ids[0] = 2345;
        assertArrayEquals(new long[] { 1234 }, message.getCursorIds());
        assertThat(message.getOperationName(),
                is(Operation.KILL_CURSORS.name()));
    }

    /**
     * Test method for {@link KillCursors#validateSize(SizeOfVisitor, int)} .
     */
    @Test
    public void testValidateSize() {
        final long[] ids = new long[] { 1234 };
        final KillCursors message = new KillCursors(ids, ReadPreference.PRIMARY);

        message.validateSize(new SizeOfVisitor(), 1024);

        // Should be able to call again without visitor since size is cached.
        message.validateSize(null, 1024);
    }

    /**
     * Test method for {@link KillCursors#validateSize(SizeOfVisitor, int)} .
     */
    @Test
    public void testValidateSizeThrows() {
        final long[] ids = new long[] { 1234 };
        final KillCursors message = new KillCursors(ids, ReadPreference.PRIMARY);

        try {
            message.validateSize(null, 1);
        }
        catch (final DocumentToLargeException dtle) {
            assertEquals(1, dtle.getMaximumSize());
            assertEquals(8, dtle.getSize());
            assertNull(dtle.getDocument());
        }
    }
}
