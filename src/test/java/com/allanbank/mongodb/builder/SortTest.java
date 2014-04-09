/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
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
