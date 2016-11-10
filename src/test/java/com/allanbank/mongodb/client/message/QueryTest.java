/*
 * #%L
 * QueryTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
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

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * QueryTest provides tests for the {@link Query} message.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class QueryTest {

    /**
     * Test method for {@link Query#equals(Object)}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        for (final String databaseName : Arrays.asList("n1", "n2", "n3")) {
            for (final String collectionName : Arrays.asList("c1", "c2", "c3")) {
                for (final Document query : Arrays.asList(doc1, doc2, doc3)) {
                    for (final Document returnFields : Arrays.asList(null,
                            doc1, doc2, doc3)) {
                        for (final int batchSize : Arrays
                                .asList(-1, 3, 8, 0xFF)) {
                            for (final int limit : Arrays
                                    .asList(-1, 3, 8, 0xFF)) {
                                for (final int numberToSkip : Arrays.asList(0,
                                        1, 2, 0xFFF)) {

                                    if (random.nextInt(10) == 1) {
                                        final boolean tailable = random
                                                .nextBoolean();
                                        final ReadPreference readPreference = random
                                                .nextBoolean() ? ReadPreference.CLOSEST
                                                : ReadPreference.SECONDARY;
                                        final boolean noCursorTimeout = random
                                                .nextBoolean();
                                        final boolean awaitData = random
                                                .nextBoolean();
                                        final boolean exhaust = random
                                                .nextBoolean();
                                        final boolean partial = random
                                                .nextBoolean();

                                        objs1.add(new Query(databaseName,
                                                collectionName, query,
                                                returnFields, batchSize, limit,
                                                numberToSkip, tailable,
                                                readPreference,
                                                noCursorTimeout, awaitData,
                                                exhaust, partial));
                                        objs2.add(new Query(databaseName,
                                                collectionName, query,
                                                returnFields, batchSize, limit,
                                                numberToSkip, tailable,
                                                readPreference,
                                                noCursorTimeout, awaitData,
                                                exhaust, partial));
                                    }
                                }
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
            assertFalse(obj1.equals(new Command(obj1.getDatabaseName(), "coll",
                    doc1)));
        }
    }

    /**
     * Test method for number to return determination logic.
     */
    @Test
    public void testNumberToReturnWhenBatchAndBiggerLimit() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final int batchSize = 5;
        final int limit = 10;

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertEquals(numberToSkip, message.getNumberToSkip());
        assertEquals(query, message.getQuery());
        assertEquals(returnFields, message.getReturnFields());
        assertEquals(Boolean.valueOf(awaitData),
                Boolean.valueOf(message.isAwaitData()));
        assertEquals(Boolean.valueOf(exhaust),
                Boolean.valueOf(message.isExhaust()));
        assertEquals(Boolean.valueOf(noCursorTimeout),
                Boolean.valueOf(message.isNoCursorTimeout()));
        assertEquals(Boolean.valueOf(partial),
                Boolean.valueOf(message.isPartial()));
        assertSame(readPreference, message.getReadPreference());
        assertEquals(Boolean.valueOf(tailable),
                Boolean.valueOf(message.isTailable()));

        assertEquals(batchSize, message.getBatchSize());
        assertEquals(limit, message.getLimit());
        assertEquals(5, message.getNumberToReturn());

    }

    /**
     * Test method for number to return determination logic.
     */
    @Test
    public void testNumberToReturnWhenBatchAndNoLimit() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final int batchSize = 9394;
        final int limit = -1;

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertEquals(numberToSkip, message.getNumberToSkip());
        assertEquals(query, message.getQuery());
        assertEquals(returnFields, message.getReturnFields());
        assertEquals(Boolean.valueOf(awaitData),
                Boolean.valueOf(message.isAwaitData()));
        assertEquals(Boolean.valueOf(exhaust),
                Boolean.valueOf(message.isExhaust()));
        assertEquals(Boolean.valueOf(noCursorTimeout),
                Boolean.valueOf(message.isNoCursorTimeout()));
        assertEquals(Boolean.valueOf(partial),
                Boolean.valueOf(message.isPartial()));
        assertSame(readPreference, message.getReadPreference());
        assertEquals(Boolean.valueOf(tailable),
                Boolean.valueOf(message.isTailable()));

        assertEquals(batchSize, message.getBatchSize());
        assertEquals(limit, message.getLimit());
        assertEquals(batchSize, message.getNumberToReturn());

    }

    /**
     * Test method for number to return determination logic.
     */
    @Test
    public void testNumberToReturnWhenBatchAndSmallerLimit() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final int batchSize = 5;
        final int limit = 4;

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertEquals(numberToSkip, message.getNumberToSkip());
        assertEquals(query, message.getQuery());
        assertEquals(returnFields, message.getReturnFields());
        assertEquals(Boolean.valueOf(awaitData),
                Boolean.valueOf(message.isAwaitData()));
        assertEquals(Boolean.valueOf(exhaust),
                Boolean.valueOf(message.isExhaust()));
        assertEquals(Boolean.valueOf(noCursorTimeout),
                Boolean.valueOf(message.isNoCursorTimeout()));
        assertEquals(Boolean.valueOf(partial),
                Boolean.valueOf(message.isPartial()));
        assertSame(readPreference, message.getReadPreference());
        assertEquals(Boolean.valueOf(tailable),
                Boolean.valueOf(message.isTailable()));

        assertEquals(batchSize, message.getBatchSize());
        assertEquals(limit, message.getLimit());
        assertEquals(4, message.getNumberToReturn());

    }

    /**
     * Test method for number to return determination logic.
     */
    @Test
    public void testNumberToReturnWhenNoBatchAndBiggishLimit() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final int batchSize = 0;
        final int limit = 1000;

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertEquals(numberToSkip, message.getNumberToSkip());
        assertEquals(query, message.getQuery());
        assertEquals(returnFields, message.getReturnFields());
        assertEquals(Boolean.valueOf(awaitData),
                Boolean.valueOf(message.isAwaitData()));
        assertEquals(Boolean.valueOf(exhaust),
                Boolean.valueOf(message.isExhaust()));
        assertEquals(Boolean.valueOf(noCursorTimeout),
                Boolean.valueOf(message.isNoCursorTimeout()));
        assertEquals(Boolean.valueOf(partial),
                Boolean.valueOf(message.isPartial()));
        assertSame(readPreference, message.getReadPreference());
        assertEquals(Boolean.valueOf(tailable),
                Boolean.valueOf(message.isTailable()));

        assertEquals(batchSize, message.getBatchSize());
        assertEquals(limit, message.getLimit());
        assertEquals(0, message.getNumberToReturn());

    }

    /**
     * Test method for number to return determination logic.
     */
    @Test
    public void testNumberToReturnWhenNoBatchAndNoLimit() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final int batchSize = 0;
        final int limit = 0;

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertEquals(numberToSkip, message.getNumberToSkip());
        assertEquals(query, message.getQuery());
        assertEquals(returnFields, message.getReturnFields());
        assertEquals(Boolean.valueOf(awaitData),
                Boolean.valueOf(message.isAwaitData()));
        assertEquals(Boolean.valueOf(exhaust),
                Boolean.valueOf(message.isExhaust()));
        assertEquals(Boolean.valueOf(noCursorTimeout),
                Boolean.valueOf(message.isNoCursorTimeout()));
        assertEquals(Boolean.valueOf(partial),
                Boolean.valueOf(message.isPartial()));
        assertSame(readPreference, message.getReadPreference());
        assertEquals(Boolean.valueOf(tailable),
                Boolean.valueOf(message.isTailable()));

        assertEquals(batchSize, message.getBatchSize());
        assertEquals(limit, message.getLimit());
        assertEquals(0, message.getNumberToReturn());

    }

    /**
     * Test method for number to return determination logic.
     */
    @Test
    public void testNumberToReturnWhenNoBatchAndSmallishLimit() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final int batchSize = 0;
        final int limit = 5;

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertEquals(numberToSkip, message.getNumberToSkip());
        assertEquals(query, message.getQuery());
        assertEquals(returnFields, message.getReturnFields());
        assertEquals(Boolean.valueOf(awaitData),
                Boolean.valueOf(message.isAwaitData()));
        assertEquals(Boolean.valueOf(exhaust),
                Boolean.valueOf(message.isExhaust()));
        assertEquals(Boolean.valueOf(noCursorTimeout),
                Boolean.valueOf(message.isNoCursorTimeout()));
        assertEquals(Boolean.valueOf(partial),
                Boolean.valueOf(message.isPartial()));
        assertSame(readPreference, message.getReadPreference());
        assertEquals(Boolean.valueOf(tailable),
                Boolean.valueOf(message.isTailable()));

        assertEquals(batchSize, message.getBatchSize());
        assertEquals(limit, message.getLimit());
        assertEquals(5, message.getNumberToReturn());

    }

    /**
     * Test method for {@link Query#Query(Header,BsonInputStream)}.
     *
     * @throws IOException
     *             On a test failure.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testQueryHeaderBsonInputStream() throws IOException {
        final Random random = new Random(System.currentTimeMillis());

        final List<Message> objs1 = new ArrayList<Message>();

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        for (final String databaseName : Arrays.asList("n1", "n2", "n3")) {
            for (final String collectionName : Arrays.asList("c1", "c2", "c3")) {
                for (final Document query : Arrays.asList(doc1, doc2, doc3)) {
                    for (final Document returnFields : Arrays.asList(doc1,
                            doc2, doc3, null)) {
                        for (final int batchSize : Arrays.asList(0)) {
                            for (final int limit : Arrays.asList(0)) {
                                for (final int numberToSkip : Arrays.asList(0,
                                        1, 2, 0xFFF)) {
                                    final boolean tailable = random
                                            .nextBoolean();
                                    final ReadPreference readPreference = random
                                            .nextBoolean() ? ReadPreference.PRIMARY
                                            : ReadPreference.SECONDARY;
                                    final boolean noCursorTimeout = random
                                            .nextBoolean();
                                    final boolean awaitData = random
                                            .nextBoolean();
                                    final boolean exhaust = random
                                            .nextBoolean();
                                    final boolean partial = random
                                            .nextBoolean();

                                    objs1.add(new Query(databaseName,
                                            collectionName, query,
                                            returnFields, batchSize, limit,
                                            numberToSkip, tailable,
                                            readPreference, noCursorTimeout,
                                            awaitData, exhaust, partial));
                                }
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

            final byte[] bytes = out.toByteArray();
            assertThat(message.size(), is(bytes.length));

            final ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            final BsonInputStream bIn = new BsonInputStream(in);

            final Header header = new Header(bIn);

            assertEquals(Operation.QUERY, header.getOperation());
            assertEquals(1234, header.getRequestId());
            assertEquals(0, header.getResponseId());
            assertEquals(out.size(), header.getLength());

            final Query read = new Query(header, bIn);
            if (message.getReadPreference() == ReadPreference.PRIMARY) {
                assertEquals(message, read);
            }
        }
    }

    /**
     * Test method for
     * {@link Query#Query(String, String, Document, Document, int, int, int, boolean, ReadPreference, boolean, boolean, boolean, boolean)}
     * .
     */
    @Test
    public void testQueryStringStringDocumentDocumentIntIntBooleanBooleanBooleanBooleanBooleanBoolean() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int batchSize = random.nextInt();
        final int limit = random.nextInt();
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        assertEquals(databaseName, message.getDatabaseName());
        assertEquals(collectionName, message.getCollectionName());
        assertEquals(batchSize, message.getBatchSize());
        assertEquals(limit, message.getLimit());
        assertEquals(numberToSkip, message.getNumberToSkip());
        assertEquals(query, message.getQuery());
        assertEquals(returnFields, message.getReturnFields());
        assertEquals(Boolean.valueOf(awaitData),
                Boolean.valueOf(message.isAwaitData()));
        assertEquals(Boolean.valueOf(exhaust),
                Boolean.valueOf(message.isExhaust()));
        assertEquals(Boolean.valueOf(noCursorTimeout),
                Boolean.valueOf(message.isNoCursorTimeout()));
        assertEquals(Boolean.valueOf(partial),
                Boolean.valueOf(message.isPartial()));
        assertSame(readPreference, message.getReadPreference());
        assertEquals(Boolean.valueOf(tailable),
                Boolean.valueOf(message.isTailable()));
        assertThat(message.getOperationName(), is(Operation.QUERY.name()));
    }

    /**
     * Test method for {@link Query#validateSize(int)} .
     */
    @Test
    public void testValidateSize() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int batchSize = random.nextInt();
        final int limit = random.nextInt();
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        message.validateSize(1024);

        // Should be able to call again without visitor since size is cached.
        message.validateSize(1024);
    }

    /**
     * Test method for {@link Query#validateSize(int)} .
     */
    @Test
    public void testValidateSizeNoQueryNoFields() {
        final Random random = new Random(System.currentTimeMillis());

        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = null;
        final Document returnFields = null;
        final int batchSize = random.nextInt();
        final int limit = random.nextInt();
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        message.validateSize(1);

        // Should be able to call again without visitor since size is cached.
        message.validateSize(1024);
    }

    /**
     * Test method for {@link Query#validateSize(int)} .
     */
    @Test
    public void testValidateSizeThrows() {
        final Random random = new Random(System.currentTimeMillis());

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final String databaseName = "db";
        final String collectionName = "collection";
        final Document query = doc1;
        final Document returnFields = doc2;
        final int batchSize = random.nextInt();
        final int limit = random.nextInt();
        final int numberToSkip = random.nextInt();
        final boolean tailable = random.nextBoolean();
        final ReadPreference readPreference = random.nextBoolean() ? ReadPreference.PRIMARY
                : ReadPreference.SECONDARY;
        final boolean noCursorTimeout = random.nextBoolean();
        final boolean awaitData = random.nextBoolean();
        final boolean exhaust = random.nextBoolean();
        final boolean partial = random.nextBoolean();

        final Query message = new Query(databaseName, collectionName, query,
                returnFields, batchSize, limit, numberToSkip, tailable,
                readPreference, noCursorTimeout, awaitData, exhaust, partial);

        try {
            message.validateSize(1);
        }
        catch (final DocumentToLargeException dtle) {
            assertEquals(1, dtle.getMaximumSize());
            assertEquals(24, dtle.getSize());
            assertSame(query, dtle.getDocument());
        }
    }
}
