/*
 * #%L
 * CreateIndexCommandTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.SymbolElement;
import com.allanbank.mongodb.builder.Index;
import com.allanbank.mongodb.client.Message;

/**
 * CreateIndexCommandTest provides privodes tests for the
 * {@link CreateIndexCommand} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class CreateIndexCommandTest {
    /**
     * Test method for {@link CreateIndexCommand#buildIndexName(Element...)} .
     */
    @Test
    public void testBuildIndexName() {
        String name = CreateIndexCommand.buildIndexName(new DoubleElement("l",
                1.2));
        assertEquals("l_1", name);

        name = CreateIndexCommand
                .buildIndexName(new StringElement("l", "true"));
        assertEquals("l_true", name);

        name = CreateIndexCommand
                .buildIndexName(new SymbolElement("l", "true"));
        assertEquals("l_true", name);
    }

    /**
     * Test method for {@link CreateIndexCommand#equals(Object)} and
     * {@link CreateIndexCommand#hashCode()} .
     */
    @SuppressWarnings("boxing")
    @Test
    public void testEqualsObject() {
        final List<Message> objs1 = new ArrayList<Message>();
        final List<Message> objs2 = new ArrayList<Message>();

        final Element[] keys1 = new Element[] { Index.asc("a") };
        final Element[] keys2 = new Element[] { Index.desc("b") };
        final Element[] keys3 = new Element[] { Index.asc("c"), Index.desc("d") };

        final Document options1 = BuilderFactory.start().addInteger("1", 0)
                .build();
        final Document options2 = BuilderFactory.start().addInteger("1", 1)
                .build();
        final Document options3 = BuilderFactory.start().addInteger("1", 2)
                .build();

        for (final Element[] keys : Arrays.asList(keys1, keys2, keys3)) {
            for (final Document options : Arrays.asList(options1, options2,
                    options3, null)) {
                for (final String databaseName : Arrays.asList("d1")) {
                    for (final String collectionName : Arrays.asList("c1")) {
                        for (final String indexName : Arrays.asList("i1", "i2",
                                "i3", null)) {
                            objs1.add(new CreateIndexCommand(databaseName,
                                    collectionName, keys, indexName, options));
                            objs2.add(new CreateIndexCommand(databaseName,
                                    collectionName, keys, indexName, options));
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
                assertFalse("" + obj1 + ":" + obj2,
                        obj1.hashCode() == obj2.hashCode());
            }

            assertFalse(obj1.equals("foo"));
            assertFalse(obj1.equals(null));
            assertFalse(obj1.equals(new IsMaster()));
        }
    }

}
