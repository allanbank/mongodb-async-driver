/*
 * #%L
 * SingleDocumentReplyCallbackTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.ReplyException;
import com.allanbank.mongodb.error.ShardConfigStaleException;

/**
 * SingleDocumentReplyCallbackTest provides tests for the
 * {@link SingleDocumentReplyCallback} class.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SingleDocumentReplyCallbackTest {

    /**
     * Test method for {@link SingleDocumentReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorMoreThan1Doc() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .build(), BuilderFactory.start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertTrue(error.getMessage().contains(
                "Should only be a single document in the reply."));

        verify(mockCallback);
    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorNotKnownError() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertNull(error);

        verify(mockCallback);
    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#asError(Reply)} .
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

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(ShardConfigStaleException.class));
        assertTrue(thrown.getMessage().contains("This is an error."));
    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#asError(Reply)} .
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

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(ShardConfigStaleException.class));
        assertTrue(thrown.getMessage().contains("This is an error."));
    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOk() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("code", -23).addDouble("ok", 1).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, false);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertNull(error);

        verify(mockCallback);

    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithStringOk() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addString("ok", "0").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertEquals(-1, error.getErrorNumber());

        verify(mockCallback);
    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithStringOkNotKnownError() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply);
        assertNull(error);

        verify(mockCallback);
    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#convert(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertReplyManyDoc() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .build(), BuilderFactory.start().build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        assertNull(callback.convert(reply));

        verify(mockCallback);
    }

    /**
     * Test method for {@link SingleDocumentReplyCallback#convert(Reply)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testConvertReplyOneDoc() {
        final List<Document> docs = Arrays.asList(BuilderFactory.start()
                .build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Document> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final SingleDocumentReplyCallback callback = new SingleDocumentReplyCallback(
                mockCallback);
        assertSame(docs.get(0), callback.convert(reply));

        verify(mockCallback);
    }
}
