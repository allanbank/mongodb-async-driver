/*
 * #%L
 * LegacyStreamCallbackAdapterTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;

/**
 * LegacyStreamCallbackAdapterTest provides tests for the
 * {@link LegacyStreamCallbackAdapter}.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public class LegacyStreamCallbackAdapterTest {

    /**
     * Test method for {@link LegacyStreamCallbackAdapter#callback(Document)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCallback() {
        final Document doc = BuilderFactory.start().build();
        final Callback<Document> mockCallback = createMock(Callback.class);

        mockCallback.callback(doc);
        expectLastCall();

        replay(mockCallback);

        final LegacyStreamCallbackAdapter adapter = new LegacyStreamCallbackAdapter(
                mockCallback);
        adapter.callback(doc);

        verify(mockCallback);
    }

    /**
     * Test method for {@link LegacyStreamCallbackAdapter#done()}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testDone() {
        final Callback<Document> mockCallback = createMock(Callback.class);

        mockCallback.callback(isNull(Document.class));
        expectLastCall();

        replay(mockCallback);

        final LegacyStreamCallbackAdapter adapter = new LegacyStreamCallbackAdapter(
                mockCallback);
        adapter.done();

        verify(mockCallback);
    }

    /**
     * Test method for {@link LegacyStreamCallbackAdapter#exception(Throwable)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testException() {
        final Throwable thrown = new NullPointerException();
        final Callback<Document> mockCallback = createMock(Callback.class);

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockCallback);

        final LegacyStreamCallbackAdapter adapter = new LegacyStreamCallbackAdapter(
                mockCallback);
        adapter.exception(thrown);

        verify(mockCallback);
    }

}
