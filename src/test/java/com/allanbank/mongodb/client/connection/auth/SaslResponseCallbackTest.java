/*
 * #%L
 * SaslResponseCallbackTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.client.connection.auth;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.junit.Test;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * SaslResponseCallbackTest provides tests for the {@link SaslResponseCallback}
 * class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class SaslResponseCallbackTest {

    /**
     * Test method for {@link SaslResponseCallback#exception(Throwable)}.
     */
    @Test
    public void testExceptionThrowable() {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        replay(mockClient, mockConnection);

        final Exception error = new Exception("test");
        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        callback.exception(error);

        verify(mockClient, mockConnection);

        try {
            results.get();
            fail("Should have thrown an ExecutionException.");
        }
        catch (final InterruptedException e) {
            fail(e.getMessage());
        }
        catch (final ExecutionException e) {
            // Good.
            assertSame(error, e.getCause());
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testHandleDone() throws InterruptedException,
            ExecutionException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        expect(mockClient.isComplete()).andReturn(true);

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("done", true).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        assertEquals(Boolean.TRUE, results.get());
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleDoneClientDoesNotComplete()
            throws InterruptedException, ExecutionException, SaslException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        expect(mockClient.isComplete()).andReturn(false);
        expect(mockClient.evaluateChallenge(aryEq(new byte[0]))).andReturn(
                new byte[0]);
        expect(mockClient.isComplete()).andReturn(false);

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("done", true).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get();
            fail("Should have thrown an ExecutionException.");
        }
        catch (final ExecutionException expected) {
            assertThat(expected.getCause(),
                    instanceOf(MongoDbAuthenticationException.class));
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleDoneClientNotAtFirst() throws InterruptedException,
            ExecutionException, SaslException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        expect(mockClient.isComplete()).andReturn(false);
        expect(mockClient.evaluateChallenge(aryEq(new byte[0]))).andReturn(
                new byte[0]);
        expect(mockClient.isComplete()).andReturn(true);

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("done", true).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        assertEquals(Boolean.TRUE, results.get());
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleMissingDoneField() throws InterruptedException,
            ExecutionException, SaslException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        expect(mockClient.evaluateChallenge(aryEq(new byte[1]))).andReturn(
                new byte[3]);

        final Document command = BuilderFactory.start().add("saslContinue", 1)
                .add("conversationId", 1L).add("payload", new byte[3]).build();
        mockConnection.send(new Command(SaslResponseCallback.EXTERNAL,
                Command.COMMAND_COLLECTION, command), callback);
        expectLastCall();

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("conversationId", 1L).add("payload", new byte[1]).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get(1, TimeUnit.MICROSECONDS);
            fail("Should have timed out.");
        }
        catch (final TimeoutException good) {
            // Good.
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleMissingPayloadField() throws InterruptedException,
            ExecutionException, SaslException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        expect(mockClient.evaluateChallenge(aryEq(new byte[0]))).andReturn(
                new byte[3]);

        final Document command = BuilderFactory.start().add("saslContinue", 1)
                .add("conversationId", "foo").add("payload", new byte[3])
                .build();
        mockConnection.send(new Command(SaslResponseCallback.EXTERNAL,
                Command.COMMAND_COLLECTION, command), callback);
        expectLastCall();

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("done", false).add("conversationId", "foo").build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get(1, TimeUnit.MICROSECONDS);
            fail("Should have timed out.");
        }
        catch (final TimeoutException good) {
            // Good.
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleNotDone() throws InterruptedException,
            ExecutionException, SaslException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        expect(mockClient.evaluateChallenge(aryEq(new byte[1]))).andReturn(
                new byte[3]);

        final Document command = BuilderFactory.start().add("saslContinue", 1)
                .add("conversationId", "foo").add("payload", new byte[3])
                .build();
        mockConnection.send(new Command(SaslResponseCallback.EXTERNAL,
                Command.COMMAND_COLLECTION, command), callback);
        expectLastCall();

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("done", false).add("conversationId", "foo")
                .add("payload", new byte[1]).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get(1, TimeUnit.MICROSECONDS);
            fail("Should have timed out.");
        }
        catch (final TimeoutException good) {
            // Good.
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleOnErrorReply() throws InterruptedException,
            ExecutionException, SaslException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 0)
                .add("err", "Injected").add("done", false)
                .add("conversationId", "foo").add("payload", new byte[1])
                .build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get();
            fail("Should have thrown an ExecutionException.");
        }
        catch (final InterruptedException e) {
            fail(e.getMessage());
        }
        catch (final ExecutionException e) {
            // Good.
            assertThat(e.getCause().getMessage(), containsString("Injected"));
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleOnMongoDbException() throws InterruptedException,
            ExecutionException, SaslException {
        final MongoDbException error = new MongoDbException("Injected");

        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        expect(mockClient.evaluateChallenge(aryEq(new byte[1])))
                .andThrow(error);

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("done", false).add("conversationId", "foo")
                .add("payload", new byte[1]).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get();
            fail("Should have thrown an ExecutionException.");
        }
        catch (final InterruptedException e) {
            fail(e.getMessage());
        }
        catch (final ExecutionException e) {
            // Good.
            assertSame(error, e.getCause());
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testHandleOnSaslClientException() throws InterruptedException,
            ExecutionException, SaslException {

        final SaslException error = new SaslException("Injected");

        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        expect(mockClient.evaluateChallenge(aryEq(new byte[1])))
                .andThrow(error);

        replay(mockClient, mockConnection);

        final Document doc = BuilderFactory.start().add("ok", 1)
                .add("done", false).add("conversationId", "foo")
                .add("payload", new byte[1]).build();
        final Reply reply = new Reply(1, 0, 0, Collections.singletonList(doc),
                true, false, false, false);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get();
            fail("Should have thrown an ExecutionException.");
        }
        catch (final InterruptedException e) {
            fail(e.getMessage());
        }
        catch (final ExecutionException e) {
            // Good.
            assertSame(error, e.getCause().getCause());
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#handle(Reply)}.
     */
    @Test
    public void testHandleWithReplyMissingDocument() {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        replay(mockClient, mockConnection);

        final List<Document> docs = Collections.emptyList();
        final Reply reply = new Reply(1, 0, 0, docs, true, false, false, false);

        callback.handle(reply);

        verify(mockClient, mockConnection);

        try {
            results.get();
            fail("Should have thrown an ExecutionException.");
        }
        catch (final InterruptedException e) {
            fail(e.getMessage());
        }
        catch (final ExecutionException e) {
            // Good.
            assertThat(
                    e.getCause().getMessage(),
                    containsString("Did not receive a valid reply to an authentication command."));
        }
    }

    /**
     * Test method for {@link SaslResponseCallback#isLightWeight}.
     * 
     * @throws ExecutionException
     *             On a test failure.
     * @throws InterruptedException
     *             On a test failure.
     */
    @Test
    public void testIsLightWeight() throws InterruptedException,
            ExecutionException {
        final SaslClient mockClient = createMock(SaslClient.class);
        final Connection mockConnection = createMock(Connection.class);
        final FutureCallback<Boolean> results = new FutureCallback<Boolean>();

        replay(mockClient, mockConnection);

        final SaslResponseCallback callback = new SaslResponseCallback(
                mockClient, mockConnection, results);

        assertThat(callback.isLightWeight(), is(true));

        verify(mockClient, mockConnection);
    }
}
