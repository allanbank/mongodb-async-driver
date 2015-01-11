/*
 * #%L
 * SortTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.builder;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;

/**
 * SortTest provides tests for the Sort helper class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SortTest {

    /**
     * Test method for {@link Sort#asc(String)}.
     */
    @Test
    public void testAsc() {
        assertEquals(new IntegerElement("f", 1), Sort.asc("f"));
    }

    /**
     * Test method for {@link Sort#desc(String)}.
     */
    @Test
    public void testDesc() {
        assertEquals(new IntegerElement("g", -1), Sort.desc("g"));
    }

    /**
     * Test method for {@link Sort#geo2d(String)}.
     *
     * @deprecated See {@link Sort#geo2d(String)}.
     */
    @Deprecated
    @Test
    public void testGeo2d() {
        assertEquals(new StringElement("h", "2d"), Sort.geo2d("h"));
    }

    /**
     * Test method for {@link Sort#natural()}.
     */
    @Test
    public void testNatural() {
        assertEquals(new IntegerElement("$natural", 1), Sort.natural());
    }

    /**
     * Test method for {@link Sort#natural(int)}.
     */
    @Test
    public void testNaturalInt() {
        assertEquals(new IntegerElement("$natural", 1),
                Sort.natural(Sort.ASCENDING));
        assertEquals(new IntegerElement("$natural", -1),
                Sort.natural(Sort.DESCENDING));
        assertEquals(new IntegerElement("$natural", -23), Sort.natural(-23));
    }
}
