/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
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

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * UpdateTest provides tests for the {@link Update} message.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UpdateTest {

    /**
     * Test method for {@link Update#equals(Object)}.
     */
    @Test
    public void testEqualsObject() {
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        for (final String databaseName : Arrays.asList("d1", "d2", "d3", "d4")) {
            for (final String collectionName : Arrays.asList("d1", "d2", "d3",
                    "d4")) {
                for (final Document query : Arrays.asList(doc1, doc2, doc3,
                        doc4)) {
                    for (final Document update : Arrays.asList(doc1, doc2,
                            doc3, doc4)) {
                        for (final Boolean multiUpdate : Arrays.asList(
                                Boolean.TRUE, Boolean.FALSE)) {
                            for (final Boolean upsert : Arrays.asList(
                                    Boolean.TRUE, Boolean.FALSE)) {
                                objs1.add(new Update(databaseName,
                                        collectionName, query, update,
                                        multiUpdate.booleanValue(), upsert
                                                .booleanValue()));
                                objs2.add(new Update(databaseName,
                                        collectionName, query, update,
                                        multiUpdate.booleanValue(), upsert
                                                .booleanValue()));
                            }
                        }
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
     * Test method for {@link Update#Update(BsonInputStream)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testUpdateBsonInputStream() throws IOException {
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

        final List<Message> objs1 = new ArrayList<Message>();

        for (final String databaseName : Arrays.asList("d1", "d2", "d3", "d4")) {
            for (final String collectionName : Arrays.asList("d1", "d2", "d3",
                    "d4")) {
                for (final Document query : Arrays.asList(doc1, doc2, doc3,
                        doc4)) {
                    for (final Document update : Arrays.asList(doc1, doc2,
                            doc3, doc4)) {
                        for (final Boolean multiUpdate : Arrays.asList(
                                Boolean.TRUE, Boolean.FALSE)) {
                            for (final Boolean upsert : Arrays.asList(
                                    Boolean.TRUE, Boolean.FALSE)) {
                                objs1.add(new Update(databaseName,
                                        collectionName, query, update,
                                        multiUpdate.booleanValue(), upsert
                                                .booleanValue()));
                            }
                        }
                    }
                }
            }
        }
        for (final Message message : objs1) {

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final BsonOutputStream bOut = new BsonOutputStream(out);

            message.write(1234, bOut);

            final ByteArrayInputStream in = new ByteArrayInputStream(
                    out.toByteArray());
            final BsonInputStream bIn = new BsonInputStream(in);

            final Header header = new Header(bIn);

            assertEquals(Operation.UPDATE, header.getOperation());
            assertEquals(1234, header.getRequestId());
            assertEquals(0, header.getResponseId());
            assertEquals(out.size(), header.getLength());

            final Update read = new Update(bIn);

            assertEquals(message, read);
        }

    }

    /**
     * Test method for
     * {@link Update#Update(String, String, Document, Document, boolean, boolean)}
     * .
     */
    @Test
    public void testUpdateStringStringDocumentDocumentBooleanBoolean() {
        final Random random = new Random(System.currentTimeMillis());

        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().build();
        final boolean multiUpdate = random.nextBoolean();
        final boolean upsert = random.nextBoolean();
        final Update message = new Update(databaseName, collectionName, query,
                update, multiUpdate, upsert);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertSame(query, message.getQuery());
        assertSame(update, message.getUpdate());
        assertEquals(Boolean.valueOf(multiUpdate),
                Boolean.valueOf(message.isMultiUpdate()));
        assertEquals(Boolean.valueOf(upsert),
                Boolean.valueOf(message.isUpsert()));
        assertThat(message.getOperationName(), is(Operation.UPDATE.name()));
    }

    /**
     * Test method for {@link Update#validateSize(SizeOfVisitor, int)} .
     */
    @Test
    public void testValidateSize() {
        final Random random = new Random(System.currentTimeMillis());

        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().build();
        final boolean multiUpdate = random.nextBoolean();
        final boolean upsert = random.nextBoolean();
        final Update message = new Update(databaseName, collectionName, query,
                update, multiUpdate, upsert);

        message.validateSize(new SizeOfVisitor(), 1024);

        // Should be able to call again without visitor since size is cached.
        message.validateSize(null, 1024);
    }

    /**
     * Test method for {@link Update#validateSize(SizeOfVisitor, int)} .
     */
    @Test
    public void testValidateSizeThrows() {
        final Random random = new Random(System.currentTimeMillis());

        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = BuilderFactory.start().build();
        final Document update = BuilderFactory.start().build();
        final boolean multiUpdate = random.nextBoolean();
        final boolean upsert = random.nextBoolean();
        final Update message = new Update(databaseName, collectionName, query,
                update, multiUpdate, upsert);

        try {
            message.validateSize(new SizeOfVisitor(), 1);
        }
        catch (final DocumentToLargeException dtle) {
            assertEquals(1, dtle.getMaximumSize());
            assertEquals(10, dtle.getSize());
            assertSame(update, dtle.getDocument());
        }
    }

    /**
     * Test method for {@link Update#validateSize(SizeOfVisitor, int)} .
     */
    @Test
    public void testValidateSizeWithNullQueryAndUpdate() {
        final Random random = new Random(System.currentTimeMillis());

        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = null;
        final Document update = null;
        final boolean multiUpdate = random.nextBoolean();
        final boolean upsert = random.nextBoolean();
        final Update message = new Update(databaseName, collectionName, query,
                update, multiUpdate, upsert);

        message.validateSize(new SizeOfVisitor(), 1);

        // Should be able to call again without visitor since size is cached.
        message.validateSize(null, 1);
    }

}
