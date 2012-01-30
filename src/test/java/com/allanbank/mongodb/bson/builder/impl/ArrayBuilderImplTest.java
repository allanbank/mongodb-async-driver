/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.builder.impl;

import static org.junit.Assert.*;

import org.junit.Test;

import com.allanbank.mongodb.bson.element.ArrayElement;

/**
 * ArrayBuilderImplTest provides tests for the {@link ArrayBuilderImpl} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ArrayBuilderImplTest {

    /**
     * Test method for {@link ArrayBuilderImpl#pushArray()}.
     */
    @Test
    public void testPushArray() {
        ArrayBuilderImpl builder = new ArrayBuilderImpl();

        builder.pushArray();

        ArrayElement element = (ArrayElement) builder.get("foo");
        assertTrue(element.getEntries().size() == 1);
        assertTrue(element.getEntries().get(0) instanceof ArrayElement);
        assertTrue(((ArrayElement) element.getEntries().get(0)).getEntries()
                .size() == 0);
    }

}
