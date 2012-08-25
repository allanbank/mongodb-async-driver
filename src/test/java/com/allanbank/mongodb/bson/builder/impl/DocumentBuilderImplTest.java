/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.bson.builder.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.ArrayElement;

/**
 * DocumentBuilderImplTest provides tests for a {@link DocumentBuilderImpl}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DocumentBuilderImplTest {

    /**
     * Test method for {@link DocumentBuilderImpl#build()} .
     */
    @Test
    public void testGetWithoutUniqueNames() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();
        builder.addBoolean("bool", true);
        builder.addBoolean("bool", true);

        try {
            builder.build();
            fail("Should not be able to create a document without unique names.");
        }
        catch (final AssertionError error) {
            // good.
        }
    }

    /**
     * Test method for {@link DocumentBuilderImpl#reset()}.
     */
    @Test
    public void testReset() {
        final DocumentBuilderImpl builder = new DocumentBuilderImpl();

        builder.pushArray("a");

        Document element = builder.build();

        Iterator<Element> iter = element.iterator();
        assertTrue(iter.hasNext());
        assertTrue(iter.next() instanceof ArrayElement);
        assertFalse(iter.hasNext());

        assertSame(builder, builder.reset());

        element = builder.build();
        iter = element.iterator();
        assertFalse(iter.hasNext());
    }

}
