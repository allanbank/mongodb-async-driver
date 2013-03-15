/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.socket;

import org.junit.Test;

/**
 * NoopCallbackTest provides tests for the {@link SocketConnection.NoopCallback}
 * class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class NoopCallbackTest {

    /**
     * Test method for {@link SocketConnection.NoopCallback#callback}.
     */
    @Test
    public void testCallback() {
        final SocketConnection.NoopCallback cb = new SocketConnection.NoopCallback();

        cb.callback(null);
    }

    /**
     * Test method for {@link SocketConnection.NoopCallback#exception} .
     */
    @Test
    public void testException() {
        final SocketConnection.NoopCallback cb = new SocketConnection.NoopCallback();

        cb.exception(null);
    }
}
