/*
 * #%L
 * LongToIntCallbackTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.allanbank.mongodb.client.callback;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.junit.Test;

import com.allanbank.mongodb.Callback;

/**
 * LongToIntCallbackTest provides tests for the {@link LongToIntCallback} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
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
