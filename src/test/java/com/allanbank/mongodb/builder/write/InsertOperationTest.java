/*
 * #%L
 * InsertOperationTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.impl.EmptyDocument;

/**
 * InsertOperationTest provides tests for the {@link InsertOperation} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class InsertOperationTest {

    /**
     * Test method for {@link InsertOperation#equals(Object)} and
     * {@link InsertOperation#hashCode()}.
     */
    @Test
    public void testEqualsAndHashCode() {
        final List<InsertOperation> objs1 = new ArrayList<InsertOperation>();
        final List<InsertOperation> objs2 = new ArrayList<InsertOperation>();

        for (final Document doc : Arrays.asList(d().build(), d(e("a", 1))
                .build(), EmptyDocument.INSTANCE)) {
            objs1.add(new InsertOperation(doc));
            objs2.add(new InsertOperation(doc));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final InsertOperation obj1 = objs1.get(i);
            InsertOperation obj2 = objs2.get(i);

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
     * Test method for {@link InsertOperation#toString()}.
     */
    @Test
    public void testToString() {
        final Document doc = d(e("_id", 1)).build();
        assertThat(new InsertOperation(doc).toString(),
                is("Insert[{ '_id' : 1 }]"));
    }
}
