/*
 * #%L
 * UpdateOperationTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.builder.write;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.d;
import static com.allanbank.mongodb.bson.builder.BuilderFactory.e;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * UpdateOperationTest provides tests for the {@link UpdateOperation} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class UpdateOperationTest {

    /**
     * Test method for {@link UpdateOperation#equals(Object)} and
     * {@link UpdateOperation#hashCode()}.
     */
    @Test
    public void testEqualsAndHashCode() {
        final List<UpdateOperation> objs1 = new ArrayList<UpdateOperation>();
        final List<UpdateOperation> objs2 = new ArrayList<UpdateOperation>();

        for (final DocumentBuilder query : Arrays.asList(d(), d(e("a", 1)))) {
            for (final DocumentBuilder update : Arrays
                    .asList(d(), d(e("a", 1)))) {
                objs1.add(new UpdateOperation(query, update, false, false));
                objs2.add(new UpdateOperation(query, update, false, false));

                objs1.add(new UpdateOperation(query, update, true, false));
                objs2.add(new UpdateOperation(query, update, true, false));

                objs1.add(new UpdateOperation(query, update, false, true));
                objs2.add(new UpdateOperation(query, update, false, true));
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final UpdateOperation obj1 = objs1.get(i);
            UpdateOperation obj2 = objs2.get(i);

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
        }
    }

    /**
     * Test method for {@link UpdateOperation#toString()}.
     */
    @Test
    public void testToString() {
        assertThat(
                new UpdateOperation(d(e("a", 1)), d(e("b", 1)), false, false)
                        .toString(),
                is("Update[upsert=false,multi=false,query={ a : 1 },update={ b : 1 }]"));
        assertThat(
                new UpdateOperation(d(e("a", 1)), d(e("b", 1)), true, false)
                        .toString(),
                is("Update[upsert=false,multi=true,query={ a : 1 },update={ b : 1 }]"));
        assertThat(
                new UpdateOperation(d(e("a", 1)), d(e("b", 1)), false, true)
                        .toString(),
                is("Update[upsert=true,multi=false,query={ a : 1 },update={ b : 1 }]"));
    }
}
