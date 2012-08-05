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
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;

/**
 * GetMoreTest provides tests for the {@link GetMore} message.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GetMoreTest {

    /**
     * Test method for {@link GetMore#equals(java.lang.Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        for (final String db : Arrays.asList("n1", "n2", "n3", "n4")) {
            for (final String collection : Arrays
                    .asList("n1", "n2", "n3", "n4")) {
                for (final ReadPreference prefs : Arrays.asList(
                        ReadPreference.PRIMARY, ReadPreference.SECONDARY)) {
                    long cursorId = random.nextLong();
                    int numberToReturn = random.nextInt();

                    objs1.add(new GetMore(db, collection, cursorId,
                            numberToReturn, prefs));
                    objs2.add(new GetMore(db, collection, cursorId,
                            numberToReturn, prefs));

                    numberToReturn = random.nextInt();

                    objs1.add(new GetMore(db, collection, cursorId,
                            numberToReturn, prefs));
                    objs2.add(new GetMore(db, collection, cursorId,
                            numberToReturn, prefs));

                    cursorId = random.nextLong();

                    objs1.add(new GetMore(db, collection, cursorId,
                            numberToReturn, prefs));
                    objs2.add(new GetMore(db, collection, cursorId,
                            numberToReturn, prefs));
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
            assertFalse(obj1.equals(new Command(obj1.getDatabaseName(),
                    BuilderFactory.start().build())));
        }
    }

    /**
     * Test method for {@link GetMore#GetMore(BsonInputStream)} .
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testGetMoreBsonInputStream() throws IOException {
        final Random random = new Random(System.currentTimeMillis());

        for (final String db : Arrays.asList("n1", "n2", "n3", "n4")) {
            for (final String collection : Arrays
                    .asList("n1", "n2", "n3", "n4")) {
                for (final ReadPreference prefs : Arrays
                        .asList(ReadPreference.PRIMARY)) {

                    final long cursorId = random.nextLong();
                    final int numberToReturn = random.nextInt();

                    final GetMore message = new GetMore(db, collection,
                            cursorId, numberToReturn, prefs);

                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    final BsonOutputStream bOut = new BsonOutputStream(out);

                    message.write(1234, bOut);

                    final ByteArrayInputStream in = new ByteArrayInputStream(
                            out.toByteArray());
                    final BsonInputStream bIn = new BsonInputStream(in);

                    final Header header = new Header(bIn);

                    assertEquals(Operation.GET_MORE, header.getOperation());
                    assertEquals(1234, header.getRequestId());
                    assertEquals(0, header.getResponseId());
                    assertEquals(out.size(), header.getLength());

                    final Message read = new GetMore(bIn);

                    assertEquals(message, read);
                }
            }
        }
    }

    /**
     * Test method for
     * {@link GetMore#GetMore(String, String, long, int, ReadPreference)} .
     */
    @Test
    public void testGetMoreStringStringLongInt() {
        final Random random = new Random(System.currentTimeMillis());

        final String db = "db";
        final String collection = "collection";
        final long cursorId = random.nextLong();
        final int numberToReturn = random.nextInt();

        final GetMore message = new GetMore(db, collection, cursorId,
                numberToReturn, ReadPreference.PRIMARY);

        assertEquals(db, message.getDatabaseName());
        assertEquals(collection, message.getCollectionName());
        assertEquals(cursorId, message.getCursorId());
        assertEquals(numberToReturn, message.getNumberToReturn());
    }
}
