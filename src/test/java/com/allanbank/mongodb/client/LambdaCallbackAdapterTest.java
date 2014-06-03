/*
 * #%L
 * LambdaCallbackAdapterTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb.client;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;

import org.junit.Test;

import com.allanbank.mongodb.LambdaCallback;
import com.allanbank.mongodb.MongoDbException;

/**
 * LambdaCallbackAdapterTest provides tests for the
 * {@link LambdaCallbackAdapter} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class LambdaCallbackAdapterTest {

    /**
     * Test method for {@link LambdaCallbackAdapter#callback(Object)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCallback() {
        final Object result = Integer.valueOf(123);

        final LambdaCallback<Object> mockCallback = createMock(LambdaCallback.class);

        mockCallback.accept(isNull(Throwable.class), same(result));

        replay(mockCallback);

        final LambdaCallbackAdapter<Object> adapter = new LambdaCallbackAdapter<Object>(
                mockCallback);

        adapter.callback(result);

        verify(mockCallback);
    }

    /**
     * Test method for {@link LambdaCallbackAdapter#done()}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testDone() {
        final LambdaCallback<Object> mockCallback = createMock(LambdaCallback.class);

        mockCallback.accept(isNull(Throwable.class), isNull(Object.class));

        replay(mockCallback);

        final LambdaCallbackAdapter<Object> adapter = new LambdaCallbackAdapter<Object>(
                mockCallback);

        adapter.done();

        verify(mockCallback);
    }

    /**
     * Test method for {@link LambdaCallbackAdapter#exception(Throwable)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testException() {
        final Throwable thrown = new MongoDbException();

        final LambdaCallback<Object> mockCallback = createMock(LambdaCallback.class);

        mockCallback.accept(same(thrown), isNull(Object.class));

        replay(mockCallback);

        final LambdaCallbackAdapter<Object> adapter = new LambdaCallbackAdapter<Object>(
                mockCallback);

        adapter.exception(thrown);

        verify(mockCallback);
    }

}
