/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * TextResultTest provides tests for the {@link TextResult} class.
 * 
 * @deprecated Support for the {@code text} command was deprecated in the 2.6
 *             version of MongoDB. Use the {@link ConditionBuilder#text(String)
 *             $text} query operator instead. This class will not be removed
 *             until two releases after the MongoDB 2.6 release (e.g. 2.10 if
 *             the releases are 2.8 and 2.10).
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
@SuppressWarnings("boxing")
public class TextResultTest {

    /**
     * Test method for {@link TextResult#equals} and
     * {@link TextResult#hashCode()}.
     */
    @Test
    public void testEqualsObject() {
        final Random rand = new Random(System.currentTimeMillis());

        final List<TextResult> objs1 = new ArrayList<TextResult>();
        final List<TextResult> objs2 = new ArrayList<TextResult>();

        for (final Document doc : Arrays.asList(BuilderFactory.start().build(),
                BuilderFactory.start().add("score", rand.nextInt()).build())) {

            objs1.add(new TextResult(doc));
            objs2.add(new TextResult(doc));
        }

        // Sanity check.
        assertEquals(objs1.size(), objs2.size());

        for (int i = 0; i < objs1.size(); ++i) {
            final TextResult obj1 = objs1.get(i);
            TextResult obj2 = objs2.get(i);

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
     * Test method for {@link TextResult#TextResult(DocumentAssignable)}.
     */
    @Test
    public void testTextResult() {
        final DocumentBuilder obj = BuilderFactory.start().add("f", 1);
        final DocumentAssignable in = BuilderFactory.start().add("score", 1)
                .add("obj", obj);

        final TextResult result = new TextResult(in);
        assertThat(result.getScore(), is(1D));
        assertThat(result.getDocument(), is(obj.build()));
        assertThat(result.getRawDocument(), is(in.asDocument()));
    }

    /**
     * Test method for {@link TextResult#TextResult(DocumentAssignable)}.
     */
    @Test
    public void testTextResultNoResult() {
        final DocumentBuilder obj = BuilderFactory.start().add("f", 1);
        final DocumentAssignable in = BuilderFactory.start().add("score1", 1)
                .add("obj1", obj);

        final TextResult result = new TextResult(in);
        assertThat(result.getScore(), is(-1D));
        assertThat(result.getDocument(), nullValue());
        assertThat(result.getRawDocument(), is(in.asDocument()));
    }

}
