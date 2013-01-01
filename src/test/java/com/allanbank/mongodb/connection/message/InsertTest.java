/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.message;

import static org.junit.Assert.assertEquals;
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
import com.allanbank.mongodb.bson.io.SizeOfVisitor;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * InsertTest provides tests for the {@link Insert} message.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class InsertTest {

    /**
     * Test method for {@link Insert#equals(Object)} .
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testEqualsObject() {
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

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

        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        for (final String db : Arrays.asList("n1", "n2", "n3", "n4")) {
            for (final String collection : Arrays
                    .asList("n1", "n2", "n3", "n4")) {
                for (final List<Document> doc : docs) {
                    for (final Boolean bool : Arrays.asList(Boolean.TRUE,
                            Boolean.FALSE)) {
                        objs1.add(new Insert(db, collection, doc, bool
                                .booleanValue()));
                        objs2.add(new Insert(db, collection, doc, bool
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
     * Test method for {@link Insert#Insert(Header, BsonInputStream)} .
     * 
     * @throws IOException
     *             On a test failure.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testInsertHeaderBsonInputStream() throws IOException {
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

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

        for (final String db : Arrays.asList("n1", "n2", "n3", "n4")) {
            for (final String collection : Arrays
                    .asList("n1", "n2", "n3", "n4")) {
                for (final List<Document> doc : docs) {
                    for (final Boolean bool : Arrays.asList(Boolean.TRUE,
                            Boolean.FALSE)) {
                        final Insert message = new Insert(db, collection, doc,
                                bool.booleanValue());

                        final ByteArrayOutputStream out = new ByteArrayOutputStream();
                        final BsonOutputStream bOut = new BsonOutputStream(out);

                        message.write(1234, bOut);

                        final ByteArrayInputStream in = new ByteArrayInputStream(
                                out.toByteArray());
                        final BsonInputStream bIn = new BsonInputStream(in);

                        final Header header = new Header(bIn);

                        assertEquals(Operation.INSERT, header.getOperation());
                        assertEquals(1234, header.getRequestId());
                        assertEquals(0, header.getResponseId());
                        assertEquals(out.size(), header.getLength());

                        final Insert read = new Insert(header, bIn);

                        assertEquals(message, read);
                    }
                }
            }
        }
    }

    /**
     * Test method for {@link Insert#Insert(String, String, List, boolean)} .
     */
    @Test
    public void testInsertStringStringListOfDocumentBoolean() {
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

        final List<Document> docs = new ArrayList<Document>();
        docs.add(doc1);
        docs.add(doc2);
        docs.add(doc3);
        docs.add(doc4);

        final String db = "db";
        final String collection = "collection";
        final Insert message = new Insert(db, collection, docs, false);

        assertEquals(db, message.getDatabaseName());
        assertEquals(collection, message.getCollectionName());
        assertEquals(docs, message.getDocuments());
        assertFalse(message.isContinueOnError());
    }

    /**
     * Test method for {@link KillCursors#validateSize(SizeOfVisitor, int)} .
     */
    @Test
    public void testValidateSize() {
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

        final List<Document> docs = new ArrayList<Document>();
        docs.add(doc1);
        docs.add(doc2);
        docs.add(doc3);
        docs.add(doc4);

        final String db = "db";
        final String collection = "collection";
        final Insert message = new Insert(db, collection, docs, false);

        message.validateSize(new SizeOfVisitor(), 1024);

        // Should be able to call again without visitor since size is cached.
        message.validateSize(null, 1024);
    }

    /**
     * Test method for {@link KillCursors#validateSize(SizeOfVisitor, int)} .
     */
    @Test
    public void testValidateSizeThrows() {
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

        final List<Document> docs = new ArrayList<Document>();
        docs.add(doc1);
        docs.add(doc2);
        docs.add(doc3);
        docs.add(doc4);

        final String db = "db";
        final String collection = "collection";
        final Insert message = new Insert(db, collection, docs, false);

        try {
            message.validateSize(new SizeOfVisitor(), 1);
        }
        catch (final DocumentToLargeException dtle) {
            assertEquals(1, dtle.getMaximumSize());
            assertEquals(41, dtle.getSize());
            assertSame(doc1, dtle.getDocument());
        }
    }
}
