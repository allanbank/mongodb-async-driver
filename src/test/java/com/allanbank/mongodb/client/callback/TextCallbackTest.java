/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.callback;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.client.SimpleMongoIteratorImpl;

/**
 * TextCallbackTest provides tests for the {@link TextCallback} class.
 * 
 * @deprecated Support for the {@code text} command was deprecated in the 2.6
 *             version of MongoDB. Use the {@link ConditionBuilder#text(String)
 *             $text} query operator instead. This class will not be removed
 *             until two releases after the MongoDB 2.6 release (e.g. 2.10 if
 *             the releases are 2.8 and 2.10).
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public class TextCallbackTest {

    /**
     * Test method for {@link TextCallback#callback}.
     */
    @SuppressWarnings("unchecked")
    @Test
    @Deprecated
    public void testCallback() {

        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .add("score", 1).build());

        final Callback<MongoIterator<com.allanbank.mongodb.builder.TextResult>> mockCallback = EasyMock
                .createMock(Callback.class);

        final Capture<MongoIterator<com.allanbank.mongodb.builder.TextResult>> capture = new Capture<MongoIterator<com.allanbank.mongodb.builder.TextResult>>();
        mockCallback.callback(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final TextCallback cb = new TextCallback(mockCallback);
        cb.callback(new SimpleMongoIteratorImpl<Document>(docs));

        verify(mockCallback);

        assertThat(
                capture.getValue().toList(),
                is(Collections
                        .singletonList(new com.allanbank.mongodb.builder.TextResult(
                                docs.get(0)))));
    }

    /**
     * Test method for {@link TextCallback#exception(Throwable)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    @Deprecated
    public void testException() {
        final Throwable thrown = new IllegalAccessError();

        final Callback<MongoIterator<com.allanbank.mongodb.builder.TextResult>> mockCallback = EasyMock
                .createMock(Callback.class);

        mockCallback.exception(thrown);
        expectLastCall();

        replay(mockCallback);

        final TextCallback cb = new TextCallback(mockCallback);
        cb.exception(thrown);

        verify(mockCallback);
    }
}
