/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.socket;

import org.junit.Test;

/**
 * NoopCallbackTest provides tests for the
 * {@link AbstractSocketConnection.NoopCallback} class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoopCallbackTest {

    /**
     * Test method for {@link AbstractSocketConnection.NoopCallback#callback}.
     */
    @Test
    public void testCallback() {
        final AbstractSocketConnection.NoopCallback cb = new AbstractSocketConnection.NoopCallback();

        cb.callback(null);
    }

    /**
     * Test method for {@link AbstractSocketConnection.NoopCallback#exception} .
     */
    @Test
    public void testException() {
        final AbstractSocketConnection.NoopCallback cb = new AbstractSocketConnection.NoopCallback();

        cb.exception(null);
    }
}
