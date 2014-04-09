/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/**
 * NoOpCallbackTest provides tests for the {@link NoOpCallback} class.
 * 
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoOpCallbackTest {

    /**
     * Test method for {@link NoOpCallback#callback}.
     */
    @Test
    public void testCallback() {
        final NoOpCallback cb = new NoOpCallback();

        cb.callback(null); // No exception. Nothing.
    }

    /**
     * Test method for {@link NoOpCallback#exception}.
     */
    @Test
    public void testException() {
        final NoOpCallback cb = new NoOpCallback();

        cb.exception(null); // No exception. Nothing.
    }

    /**
     * Test method for {@link NoOpCallback#isLightWeight()}.
     */
    @Test
    public void testIsLightWeight() {
        final NoOpCallback cb = new NoOpCallback();

        assertThat(cb.isLightWeight(), is(true));
    }
}
