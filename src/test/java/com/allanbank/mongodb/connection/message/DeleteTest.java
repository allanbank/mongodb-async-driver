/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Delete;
import com.allanbank.mongodb.connection.message.Header;

/**
 * DeleteTest provides tests for the {@link Delete} message.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DeleteTest {

    /**
     * Test method for {@link Delete#Delete(BsonInputStream)} .
     * 
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testDeleteBsonInputStream() throws IOException {
        final Document doc1 = BuilderFactory.start().get();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).get();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).get();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).get();

        for (final String db : Arrays.asList("n1", "n2", "n3", "n4")) {
            for (final String collection : Arrays
                    .asList("n1", "n2", "n3", "n4")) {
                for (final Document doc : Arrays.asList(doc1, doc2, doc3, doc4)) {
                    for (final Boolean bool : Arrays.asList(Boolean.TRUE,
                            Boolean.FALSE)) {
                        final Delete message = new Delete(db, collection, doc,
                                bool.booleanValue());

                        final ByteArrayOutputStream out = new ByteArrayOutputStream();
                        final BsonOutputStream bOut = new BsonOutputStream(out);

                        message.write(1234, bOut);

                        final ByteArrayInputStream in = new ByteArrayInputStream(
                                out.toByteArray());
                        final BsonInputStream bIn = new BsonInputStream(in);

                        final Header header = new Header(bIn);

                        assertEquals(Operation.DELETE, header.getOperation());
                        assertEquals(1234, header.getRequestId());
                        assertEquals(0, header.getResponseId());
                        assertEquals(out.size(), header.getLength());

                        final Delete read = new Delete(bIn);

                        assertEquals(message, read);
                    }
                }
            }
        }
    }

    /**
     * Test method for {@link Delete#equals(Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Document doc1 = BuilderFactory.start().get();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).get();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).get();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).get();

        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        for (final String db : Arrays.asList("n1", "n2", "n3", "n4")) {
            for (final String collection : Arrays
                    .asList("n1", "n2", "n3", "n4")) {
                for (final Document doc : Arrays.asList(doc1, doc2, doc3, doc4)) {
                    for (final Boolean bool : Arrays.asList(Boolean.TRUE,
                            Boolean.FALSE)) {
                        objs1.add(new Delete(db, collection, doc, bool
                                .booleanValue()));
                        objs2.add(new Delete(db, collection, doc, bool
                                .booleanValue()));
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
     * Test method for {@link Delete#getQuery()}.
     */
    @Test
    public void testGetQuery() {
        final Document doc = BuilderFactory.start().addInteger("1", 1).get();

        final Delete message = new Delete("db", "collection", doc, true);

        assertEquals("db", message.getDatabaseName());
        assertEquals("collection", message.getCollectionName());
        assertSame(doc, message.getQuery());
        assertEquals(true, message.isSingleDelete());

    }

    /**
     * Test method for {@link Delete#isSingleDelete()} .
     */
    @Test
    public void testIsSingleDelete() {
        final Document doc = BuilderFactory.start().addInteger("1", 1).get();
        final Delete message = new Delete("db", "collection", doc, false);

        assertEquals(false, message.isSingleDelete());
    }
}
