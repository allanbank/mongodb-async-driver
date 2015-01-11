/*
 * #%L
 * IndexTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
 * IndexTest provides tests for the Index helper class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class IndexTest {

    /**
     * Test method for {@link Index#asc(String)}.
     */
    @Test
    public void testAsc() {
        assertEquals(new IntegerElement("f", 1), Index.asc("f"));
    }

    /**
     * Test method for {@link Index#desc(String)}.
     */
    @Test
    public void testDesc() {
        assertEquals(new IntegerElement("g", -1), Index.desc("g"));
    }

    /**
     * Test method for {@link Index#geo2d(String)}.
     */
    @Test
    public void testGeo2d() {
        assertEquals(new StringElement("h", "2d"), Index.geo2d("h"));
    }

    /**
     * Test method for {@link Index#geo2dSphere(String)}.
     */
    @Test
    public void testGeo2dSphere() {
        assertEquals(new StringElement("h", "2dsphere"), Index.geo2dSphere("h"));
    }

    /**
     * Test method for {@link Index#geoHaystack(String)}.
     */
    @Test
    public void testGeoHaystack() {
        assertEquals(new StringElement("h", "geoHaystack"),
                Index.geoHaystack("h"));
    }

    /**
     * Test method for {@link Index#hashed(String)}.
     */
    @Test
    public void testHashed() {
        assertEquals(new StringElement("h", "hashed"), Index.hashed("h"));
    }

    /**
     * Test method for {@link Index#text(String)}.
     */
    @Test
    public void testText() {
        assertEquals(new StringElement("h", "text"), Index.text("h"));
    }
}
