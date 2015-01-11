/*
 * #%L
 * HeaderTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.client.Operation;

/**
 * HeaderTest provides tests for the {@link Header} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class HeaderTest {

    /**
     * Test method for {@link Header#equals(Object)} .
     */
    @Test
    public void testEqualsObject() {
        final Random random = new Random(System.currentTimeMillis());

        final Integer[] ints = new Integer[5];
        for (int i = 0; i < ints.length; ++i) {
            ints[i] = Integer.valueOf(random.nextInt());
        }

        final List<Header> objs1 = new ArrayList<Header>();
        final List<Header> objs2 = new ArrayList<Header>();

        for (final Integer length : ints) {
            for (final Integer requestId : ints) {
                for (final Integer responseId : ints) {
                    for (final Operation operation : Operation.values()) {

                        objs1.add(new Header(length.intValue(), requestId
                                .intValue(), responseId.intValue(), operation));
                        objs2.add(new Header(length.intValue(), requestId
                                .intValue(), responseId.intValue(), operation));
                    }
                }
            }
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final Header obj1 = objs1.get(i);
            Header obj2 = objs2.get(i);

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

}
