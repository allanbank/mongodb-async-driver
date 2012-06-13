/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson;

import static org.junit.Assert.assertSame;

import org.junit.Test;

/**
 * ElementTypeTest provides tests for the elements type.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ElementTypeTest {

    /**
     * Test method for {@link ElementType#valueOf(byte)}.
     */
    @Test
    public void testValueOf() {
        for (final ElementType type : ElementType.values()) {
            assertSame(type, ElementType.valueOf(type.getToken()));
        }
    }

}
