/*
 * #%L
 * DeleteTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.io.BsonInputStream;
import com.allanbank.mongodb.bson.io.BsonOutputStream;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.Operation;
import com.allanbank.mongodb.error.DocumentToLargeException;

/**
 * DeleteTest provides tests for the {@link Delete} message.
 * 
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
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
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

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

                        final byte[] bytes = out.toByteArray();
                        assertThat(message.size(), is(bytes.length));

                        final ByteArrayInputStream in = new ByteArrayInputStream(
                                bytes);
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
        final Document doc1 = BuilderFactory.start().build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();
        final Document doc4 = BuilderFactory.start().addInteger("1", 3).build();

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
            assertFalse(obj1.equals(new Command(obj1.getDatabaseName(), "coll",
                    doc1)));
        }
    }

    /**
     * Test method for {@link Delete#getQuery()}.
     */
    @Test
    public void testGetQuery() {
        final Document doc = BuilderFactory.start().addInteger("1", 1).build();

        final Delete message = new Delete("db", "collection", doc, true);

        assertEquals("db", message.getDatabaseName());
        assertEquals("collection", message.getCollectionName());
        assertSame(doc, message.getQuery());
        assertTrue(message.isSingleDelete());
        assertThat(message.getOperationName(), is(Operation.DELETE.name()));
    }

    /**
     * Test method for {@link Delete#isSingleDelete()} .
     */
    @Test
    public void testIsSingleDelete() {
        final Document doc = BuilderFactory.start().addInteger("1", 1).build();
        final Delete message = new Delete("db", "collection", doc, false);

        assertFalse(message.isSingleDelete());
    }

    /**
     * Test method for {@link Delete#validateSize(int)} .
     */
    @Test
    public void testValidateSize() {
        final Document doc = BuilderFactory.start().addInteger("1", 1).build();
        final Delete message = new Delete("db", "collection", doc, false);

        message.validateSize(1024);

        // Should be able to call again without visitor since size is cached.
        message.validateSize(1024);
    }

    /**
     * Test method for {@link Delete#validateSize(int)} .
     */
    @Test
    public void testValidateSizeThrows() {
        final Document doc = BuilderFactory.start().addInteger("1", 1).build();
        final Delete message = new Delete("db", "collection", doc, false);

        try {
            message.validateSize(1);
        }
        catch (final DocumentToLargeException dtle) {
            assertEquals(1, dtle.getMaximumSize());
            assertEquals(12, dtle.getSize());
            assertSame(doc, dtle.getDocument());
        }
    }
}
