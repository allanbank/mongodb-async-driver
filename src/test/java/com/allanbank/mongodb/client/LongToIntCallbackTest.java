/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.junit.Test;

import com.allanbank.mongodb.Callback;

/**
 * LongToIntCallbackTest provides tests for the {@link LongToIntCallback} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LongToIntCallbackTest {

    /**
     * Test method for {@link LongToIntCallback#callback(Long)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCallback() {
        final Callback<Integer> mockCallback = createMock(Callback.class);

        mockCallback.callback(Integer.valueOf(1));
        expectLastCall();

        replay(mockCallback);

        final LongToIntCallback lticb = new LongToIntCallback(mockCallback);

        lticb.callback(Long.valueOf(1));

        verify(mockCallback);
    }

    /**
     * Test method for {@link LongToIntCallback#exception(Throwable)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testException() {
        final Throwable t = new RuntimeException();

        final Callback<Integer> mockCallback = createMock(Callback.class);

        mockCallback.exception(t);
        expectLastCall();

        replay(mockCallback);

        final LongToIntCallback lticb = new LongToIntCallback(mockCallback);

        lticb.exception(t);

        verify(mockCallback);
    }

}
