/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * NamedReferenceTest provides tests for the {@link NamedReference} class.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NamedReferenceTest {

    /**
     * Test method for {@link NamedReference#getName()}.
     */
    @Test
    public void testGetName() {

        final NamedReference<String> ref = new NamedReference<String>("foo",
                "other", null);

        assertThat(ref.getName(), is("foo"));
    }
}
