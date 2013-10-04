/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Arrays;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.TextResult;

/**
 * TextCallbackTest provides tests for the {@link TextCallback} class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class TextCallbackTest {

    /**
     * Test method for {@link TextCallback#callback(List)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCallback() {

        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .add("score", 1).build());

        final Callback<List<TextResult>> mockCallback = EasyMock
                .createMock(Callback.class);

        mockCallback.callback(Arrays.asList(new TextResult(docs.get(0))));
        expectLastCall();

        replay(mockCallback);

        final TextCallback cb = new TextCallback(mockCallback);
        cb.callback(docs);

        verify(mockCallback);
    }

    /**
     * Test method for {@link TextCallback#exception(Throwable)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testException() {
        final Throwable thrown = new IllegalAccessError();

        final Callback<List<TextResult>> mockCallback = EasyMock
                .createMock(Callback.class);

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockCallback);

        final TextCallback cb = new TextCallback(mockCallback);
        cb.exception(thrown);

        verify(mockCallback);
    }
}
