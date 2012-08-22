/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.error.ReplyException;
import com.allanbank.mongodb.error.ShardConfigStaleException;

/**
 * ReplyCallbackTest provides tests for the {@link ReplyCallback} class.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplyCallbackTest {

    /**
     * Test method for {@link ReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorMoreThan1Doc() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .build(), BuilderFactory.start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertTrue(error.getMessage().contains(
                "Should only be a single document in the reply."));

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorNotKnownError() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertNull(error);

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithDollarErr() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addString("$err", "This is an error.").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);
        final Capture<Throwable> capture = new Capture<Throwable>();

        mockCallback.exception(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(ShardConfigStaleException.class));
        assertTrue(thrown.getMessage().contains("This is an error."));
    }

    /**
     * Test method for {@link ReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithErrorMsg() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addString("errmsg", "This is an error.").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);
        final Capture<Throwable> capture = new Capture<Throwable>();

        mockCallback.exception(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(ShardConfigStaleException.class));
        assertTrue(thrown.getMessage().contains("This is an error."));
    }

    /**
     * Test method for {@link ReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOk() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("code", -23).addDouble("ok", 1).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertNull(error);

        verify(mockCallback);

    }

    /**
     * Test method for {@link ReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithStringOk() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addString("ok", "0").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertEquals(-1, error.getErrorNumber());

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithStringOkNotKnownError() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertNull(error);

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyCallback#convert(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertReplyManyDoc() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .build(), BuilderFactory.start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        assertNull(callback.convert(reply));

        verify(mockCallback);
    }

    /**
     * Test method for {@link ReplyCallback#convert(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertReplyOneDoc() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyCallback callback = new ReplyCallback(mockCallback);
        assertSame(docs.get(0), callback.convert(reply));

        verify(mockCallback);
    }
}
