/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

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
