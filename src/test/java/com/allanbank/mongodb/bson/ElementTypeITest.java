/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.ClosableIterator;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.Sort;
import com.allanbank.mongodb.util.IOUtils;

/**
 * ElementTypeTest provides tests verify the ordering of elements by MongoDB.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ElementTypeITest extends ServerTestDriverSupport {

    /**
     * Stop the server we started.
     */
    @After
    public void tearDown() {
        stopStandAlone();
    }

    /**
     * Test method for {@link ElementType#valueOf(byte)}.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testSortOrder() {
        startStandAlone();

        // Insert a document with each of the element types into MongoDB and
        // then fetch them sorting on the field.
        final List<Document> docs = new ArrayList<Document>();
        final DocumentBuilder builder = BuilderFactory.start();

        docs.add(builder.reset().addBinary("f", new byte[0]).build());
        docs.add(builder.reset().addBoolean("f", true).build());
        docs.add(builder.reset().addDBPointer("f", "d", "c", new ObjectId())
                .build());
        docs.add(builder.reset()
                .addDocument("f", BuilderFactory.start().build()).build());
        docs.add(builder.reset().addDouble("f", 1.0).build());
        docs.add(builder.reset().addInteger("f", 1).build());
        docs.add(builder.reset().addJavaScript("f", "function c() {}").build());
        docs.add(builder
                .reset()
                .addJavaScript("f", "function c() {}",
                        BuilderFactory.start().build()).build());
        docs.add(builder.reset().addLong("f", 1).build());
        docs.add(builder.reset().addMaxKey("f").build());
        docs.add(builder.reset().addMinKey("f").build());
        // Mongo timestamp and regular timestamp do not like each other.
        // docs.add(builder.reset().addMongoTimestamp("f", 1).build());
        docs.add(builder.reset().addNull("f").build());
        docs.add(builder.reset().addObjectId("f", new ObjectId(1, 1)).build());
        docs.add(builder.reset()
                .addRegularExpression("f", Pattern.compile(".*")).build());
        docs.add(builder.reset().addString("f", "1").build());
        docs.add(builder.reset().addSymbol("f", "1").build());
        docs.add(builder.reset().addTimestamp("f", 1).build());

        Collections.shuffle(docs);

        final MongoClientConfiguration config = new MongoClientConfiguration(
                new InetSocketAddress("127.0.0.1", 27017));
        config.setDefaultDurability(Durability.ACK);

        final MongoClient m = MongoFactory.createClient(config);
        try {
            final MongoDatabase db = m.getDatabase("test");
            final MongoCollection c = db.getCollection("testSortOrder");
            for (final Document doc : docs) {
                c.insert(doc);
            }

            final Find find = new Find.Builder(BuilderFactory.start()).setSort(
                    Sort.asc("f")).build();

            final ClosableIterator<Document> iter = c.find(find);
            assertTrue(iter.hasNext());
            assertSame(ElementType.MIN_KEY, iter.next().get("f").getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.NULL, iter.next().get("f").getType());
            assertTrue(iter.hasNext());

            final Set<ElementType> compareSame = new HashSet<ElementType>();
            compareSame.add(ElementType.DOUBLE);
            compareSame.add(ElementType.INTEGER);
            compareSame.add(ElementType.LONG);
            assertTrue(compareSame.contains(iter.next().get("f").getType()));
            assertTrue(iter.hasNext());
            assertTrue(compareSame.contains(iter.next().get("f").getType()));
            assertTrue(iter.hasNext());
            assertTrue(compareSame.contains(iter.next().get("f").getType()));
            assertTrue(iter.hasNext());
            compareSame.clear();

            compareSame.add(ElementType.SYMBOL);
            compareSame.add(ElementType.STRING);
            assertTrue(compareSame.contains(iter.next().get("f").getType()));
            assertTrue(iter.hasNext());
            assertTrue(compareSame.contains(iter.next().get("f").getType()));
            assertTrue(iter.hasNext());

            assertSame(ElementType.DOCUMENT, iter.next().get("f").getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.BINARY, iter.next().get("f").getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.OBJECT_ID, iter.next().get("f").getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.BOOLEAN, iter.next().get("f").getType());
            assertTrue(iter.hasNext());

            // Mongo timestamp and regular timestamp do not like each other.
            // assertSame(ElementType.MONGO_TIMESTAMP, iter.next().get("f")
            // .getType());
            // assertTrue(iter.hasNext());

            assertSame(ElementType.UTC_TIMESTAMP, iter.next().get("f")
                    .getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.REGEX, iter.next().get("f").getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.DB_POINTER, iter.next().get("f").getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.JAVA_SCRIPT, iter.next().get("f").getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.JAVA_SCRIPT_WITH_SCOPE, iter.next().get("f")
                    .getType());
            assertTrue(iter.hasNext());
            assertSame(ElementType.MAX_KEY, iter.next().get("f").getType());
            assertFalse(iter.hasNext());
        } finally {
            IOUtils.close(m);
        }
    }
}
