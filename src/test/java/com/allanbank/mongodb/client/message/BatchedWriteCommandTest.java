/*
 * #%L
 * BatchedWriteCommandTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.BatchedWrite;
import com.allanbank.mongodb.client.Client;
import com.allanbank.mongodb.client.Message;

/**
 * BatchedWriteCommandTest provides tests for the {@link BatchedWriteCommand}
 * class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BatchedWriteCommandTest {

    /**
     * Test method for {@link BatchedWriteCommand#equals(Object)} and
     * {@link BatchedWriteCommand#hashCode()} .
     */
    @SuppressWarnings("boxing")
    @Test
    public void testEqualsObject() {
        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        final Document doc1 = BuilderFactory.start().addInteger("1", 0).build();
        final Document doc2 = BuilderFactory.start().addInteger("1", 1).build();
        final Document doc3 = BuilderFactory.start().addInteger("1", 2).build();

        for (final Document document : Arrays.asList(doc1, doc2, doc3)) {
            for (final String databaseName : Arrays.asList("n1", "n2", "n3")) {
                for (final String collectionName : Arrays.asList("n1", "n2",
                        "n3")) {
                    objs1.add(new BatchedWriteCommand(databaseName,
                            collectionName, document));
                    objs2.add(new BatchedWriteCommand(databaseName,
                            collectionName, document));
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
            assertFalse(obj1.equals(new IsMaster()));
        }
    }

    /**
     * Test method for {@link BatchedWriteCommand#getBundle()}.
     */
    @Test
    public void testGetBundle() {
        final Document doc = d().build();

        final String databaseName = "db";
        final String collectionName = "collection";
        final BatchedWrite write = BatchedWrite.builder().insert(doc)
                .update(doc, doc).delete(doc).build();
        final List<BatchedWrite.Bundle> bundles = write.toBundles(
                collectionName, Client.MAX_DOCUMENT_SIZE, 1000);

        for (final BatchedWrite.Bundle bundle : bundles) {
            final BatchedWriteCommand command = new BatchedWriteCommand(
                    databaseName, collectionName, bundle);

            assertThat(command.getCommand(), is(bundle.getCommand()));
            assertThat(command.getReadPreference(),
                    sameInstance(ReadPreference.PRIMARY));
            assertThat(command.getRequiredVersionRange(),
                    sameInstance(BatchedWriteCommand.REQUIRED_VERSION_RANGE));
            assertThat(command.getBundle(), sameInstance(bundle));
        }
    }

}
