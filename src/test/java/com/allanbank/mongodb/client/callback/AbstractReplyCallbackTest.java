/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.CursorNotFoundException;
import com.allanbank.mongodb.error.DuplicateKeyException;
import com.allanbank.mongodb.error.QueryFailedException;
import com.allanbank.mongodb.error.ReplyException;
import com.allanbank.mongodb.error.ShardConfigStaleException;

/**
 * AbstractReplyCallbackTest provides tests of the {@link AbstractReplyCallback}
 * class.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AbstractReplyCallbackTest {

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorNotKnownError() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().build());
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                false);
        assertNull(error);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorOk_1_WithErrorNullElement() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().add("ok", 1).addNull("err").build());
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                false);
        assertNull(error);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithDollarErr() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addString("$err", "This is an error.").build());
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final Capture<Throwable> capture = new Capture<Throwable>();

        mockCallback.exception(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(ShardConfigStaleException.class));
        assertTrue(thrown.getMessage().contains("This is an error."));
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithErrorMsg() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addString("errmsg", "This is an error.").build());
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final Capture<Throwable> capture = new Capture<Throwable>();

        mockCallback.exception(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(ShardConfigStaleException.class));
        assertTrue(thrown.getMessage().contains("This is an error."));
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOk() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", -23).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<ArrayElement> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyArrayCallback callback = new ReplyArrayCallback(mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                true);
        assertEquals(-23, error.getOkValue());

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOkButErrorMessageNotNull() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1).add("err", "This is an error")
                .build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Integer> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyIntegerCallback callback = new ReplyIntegerCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                true);
        assertEquals("This is an error", error.getMessage());

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOkButErrorMessageNotNullAndIsDuplicateKey11000() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1)
                .add("err", "E11000 This is an error").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Integer> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyIntegerCallback callback = new ReplyIntegerCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                true);
        assertThat(error, instanceOf(DuplicateKeyException.class));
        assertEquals(-1, error.getErrorNumber());
        assertEquals("E11000 This is an error", error.getMessage());

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOkButErrorMessageNotNullAndIsDuplicateKey11001() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1)
                .add("err", "E11001 This is an error").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Integer> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyIntegerCallback callback = new ReplyIntegerCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                true);
        assertThat(error, instanceOf(DuplicateKeyException.class));
        assertEquals(-1, error.getErrorNumber());
        assertEquals("E11001 This is an error", error.getMessage());

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOkButErrorMessageNotNullErrorCodeIsDuplicateKey11000() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1).add("err", "This is an error")
                .add("code", 11000).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Integer> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyIntegerCallback callback = new ReplyIntegerCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                true);
        assertThat(error, instanceOf(DuplicateKeyException.class));
        assertEquals(11000, error.getErrorNumber());
        assertEquals("This is an error", error.getMessage());

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOkButErrorMessageNotNullErrorCodeIsDuplicateKey11001() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1).add("err", "This is an error")
                .add("code", 11001).build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Integer> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyIntegerCallback callback = new ReplyIntegerCallback(
                mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                true);
        assertThat(error, instanceOf(DuplicateKeyException.class));
        assertEquals(11001, error.getErrorNumber());
        assertEquals("This is an error", error.getMessage());

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithOkButErrorNumberNull() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1).addNull("err").build());
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<Integer> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final ReplyIntegerCallback callback = new ReplyIntegerCallback(
                mockCallback);
        assertNull(callback.asError(reply, false));

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithStringOk() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addString("ok", "0").build());
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                true);
        assertEquals(-1, error.getOkValue());

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#asError(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testAsErrorWithStringOkNotKnownError() {
        final List<Document> docs = Collections.singletonList(BuilderFactory
                .start().addInteger("ok", 1).build());
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        final ReplyException error = (ReplyException) callback.asError(reply,
                false);
        assertNull(error);

        verify(mockCallback);
    }

    /**
     * Test method for {@link AbstractReplyCallback#verify(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testVerifyOnCursorNotFound() {
        final List<Document> docs = Collections.emptyList();
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, true, false, false);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final Capture<Throwable> capture = new Capture<Throwable>();

        mockCallback.exception(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(CursorNotFoundException.class));
    }

    /**
     * Test method for {@link AbstractReplyCallback#verify(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testVerifyOnQueryFailed() {
        final List<Document> docs = Collections.emptyList();
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, true, false);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final Capture<Throwable> capture = new Capture<Throwable>();

        mockCallback.exception(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(QueryFailedException.class));
    }

    /**
     * Test method for {@link AbstractReplyCallback#verify(Reply)} .
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testVerifyOnShardConfigStale() {
        final List<Document> docs = Collections.emptyList();
        final Query q = new Query("db", "c", BuilderFactory.start().build(),
                null, 0, 0, 0, false, ReadPreference.PRIMARY, false, false,
                false, false);
        final Reply reply = new Reply(0, 0, 0, docs, false, false, false, true);

        final Callback<MongoIterator<Document>> mockCallback = createMock(Callback.class);
        final Capture<Throwable> capture = new Capture<Throwable>();

        mockCallback.exception(capture(capture));
        expectLastCall();

        replay(mockCallback);

        final QueryCallback callback = new QueryCallback(null, q, mockCallback);
        callback.callback(reply);

        verify(mockCallback);

        final Throwable thrown = capture.getValue();
        assertThat(thrown, instanceOf(ShardConfigStaleException.class));
    }

}
