/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
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
 * DeleteOperationTest provides tests for the {@link DeleteOperation} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class DeleteOperationTest {

    /**
     * Test method for {@link DeleteOperation#equals(Object)} and
     * {@link DeleteOperation#hashCode()}.
     */
    @Test
    public void testEqualsAndHashCode() {
        final List<DeleteOperation> objs1 = new ArrayList<DeleteOperation>();
        final List<DeleteOperation> objs2 = new ArrayList<DeleteOperation>();

        for (final DocumentBuilder doc : Arrays.asList(d(), d(e("a", 1)))) {
            objs1.add(new DeleteOperation(doc, false));
            objs2.add(new DeleteOperation(doc, false));
            objs1.add(new DeleteOperation(doc, true));
            objs2.add(new DeleteOperation(doc, true));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final DeleteOperation obj1 = objs1.get(i);
            DeleteOperation obj2 = objs2.get(i);

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
     * Test method for {@link DeleteOperation#toString()}.
     */
    @Test
    public void testToString() {
        assertThat(new DeleteOperation(d(e("a", 1)), false).toString(),
                is("Delete[singleDelete=false,query={ a : 1 }]"));
        assertThat(new DeleteOperation(d(e("a", 2)), true).toString(),
                is("Delete[singleDelete=true,query={ a : 2 }]"));
    }
}
