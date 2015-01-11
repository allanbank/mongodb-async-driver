/*
 * #%L
 * ScramSaslClientTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.text.Normalizer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;

import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Test;

import com.allanbank.mongodb.util.IOUtils;

/**
 * ScramSaslClientTest provides tests for the {@link ScramSaslClient}.
 *
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ScramSaslClientTest {

    /**
     * Test method for {@link ScramSaslClient#createInitialMessage()}.
     *
     * @throws UnsupportedCallbackException
     *             On a test failure.
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testCreateInitialMessageOnCallbackThrowingAnIOException()
            throws IOException, UnsupportedCallbackException {
        final CallbackHandler mockHandler = createMock(CallbackHandler.class);

        final IOException thrown = new IOException("Injected.");
        final Capture<Callback[]> callbackCapture = new Capture<Callback[]>();
        mockHandler.handle(capture(callbackCapture));
        expectLastCall().andThrow(thrown);

        replay(mockHandler);

        final ScramSaslClient client = new ScramSaslClient(mockHandler);
        try {
            client.evaluateChallenge(null);
        }
        catch (final SaslException expected) {
            assertThat(expected.getCause(), sameInstance((Throwable) thrown));
        }

        verify(mockHandler);

        assertThat(callbackCapture.getValue().length, is(1));
        assertThat(callbackCapture.getValue()[0],
                instanceOf(NameCallback.class));
    }

    /**
     * Test method for {@link ScramSaslClient#createInitialMessage()}.
     *
     * @throws UnsupportedCallbackException
     *             On a test failure.
     * @throws IOException
     *             On a test failure.
     */
    @Test
    public void testCreateInitialMessageOnCallbackThrowingAnUnsupportedCallbackException()
            throws IOException, UnsupportedCallbackException {
        final CallbackHandler mockHandler = createMock(CallbackHandler.class);

        final UnsupportedCallbackException thrown = new UnsupportedCallbackException(
                null);
        final Capture<Callback[]> callbackCapture = new Capture<Callback[]>();
        mockHandler.handle(capture(callbackCapture));
        expectLastCall().andThrow(thrown);

        replay(mockHandler);

        final ScramSaslClient client = new ScramSaslClient(mockHandler);
        try {
            client.evaluateChallenge(null);
        }
        catch (final SaslException expected) {
            assertThat(expected.getCause(), sameInstance((Throwable) thrown));
        }

        verify(mockHandler);

        assertThat(callbackCapture.getValue().length, is(1));
        assertThat(callbackCapture.getValue()[0],
                instanceOf(NameCallback.class));
    }

    /**
     * Test method for {@link ScramSaslClient#createNonce()}.
     */
    @Test
    public void testCreateNonce() {
        final CallbackHandler handler = new TestHandler("user", "pencil");
        final ScramSaslClient client = new ScramSaslClient(handler);

        final String nonce = client.createNonce();
        assertThat(nonce, notNullValue());
        assertThat(IOUtils.base64ToBytes(nonce).length,
                is(ScramSaslClient.RANDOM_BYTES));

        assertThat(client.createNonce(), not(is(nonce)));
        assertThat(client.createNonce(), not(is(nonce)));
        assertThat(client.createNonce(), not(is(nonce)));
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testCreateProof() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        // Hard code the nonce for the test.
        final ScramSaslClient client = new TestRfcScramSaslClient(handler);

        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        // Initialize the state.
        final byte[] firstMessage = client.evaluateChallenge(null);
        assertThat(client.isComplete(), is(false));
        assertThat(firstMessage, notNullValue());
        assertThat(new String(firstMessage, ScramSaslClient.UTF_8),
                is("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"));

        // Call createProof.
        final byte[] proof = client.createProof(("r=fyko+d2lbbFgONRv9qkxdawL"
                + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(false));
        assertThat(proof, notNullValue());
        assertThat(new String(proof, ScramSaslClient.UTF_8),
                is("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                        + "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="));
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws UnsupportedCallbackException
     *             On a test failure.
     * @throws IOException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofOnAnIOException() throws IOException,
            UnsupportedCallbackException {

        final CallbackHandler mockHandler = createMock(CallbackHandler.class);

        final IOException thrown = new IOException("Injected.");

        final Capture<Callback[]> userCapture = new Capture<Callback[]>();
        mockHandler.handle(capture(userCapture));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                assertThat(userCapture.getValue().length, is(1));
                assertThat(userCapture.getValue()[0],
                        instanceOf(NameCallback.class));

                ((NameCallback) userCapture.getValue()[0]).setName("user");
                return null;
            }
        });

        final Capture<Callback[]> passwordCapture = new Capture<Callback[]>();
        mockHandler.handle(capture(passwordCapture));
        expectLastCall().andThrow(thrown);

        replay(mockHandler);

        final ScramSaslClient client = new ScramSaslClient(mockHandler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=" + clientNonce + "abcd,s=abcd,i=1")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getCause(), sameInstance((Throwable) thrown));
        }

        verify(mockHandler);

        assertThat(passwordCapture.getValue().length, is(1));
        assertThat(passwordCapture.getValue()[0],
                instanceOf(PasswordCallback.class));
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws UnsupportedCallbackException
     *             On a test failure.
     * @throws IOException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofOnAnUnsupportedCallback() throws IOException,
            UnsupportedCallbackException {

        final CallbackHandler mockHandler = createMock(CallbackHandler.class);

        final UnsupportedCallbackException thrown = new UnsupportedCallbackException(
                null);

        final Capture<Callback[]> userCapture = new Capture<Callback[]>();
        mockHandler.handle(capture(userCapture));
        expectLastCall().andAnswer(new IAnswer<Object>() {

            @Override
            public Object answer() throws Throwable {
                assertThat(userCapture.getValue().length, is(1));
                assertThat(userCapture.getValue()[0],
                        instanceOf(NameCallback.class));

                ((NameCallback) userCapture.getValue()[0]).setName("user");
                return null;
            }
        });

        final Capture<Callback[]> passwordCapture = new Capture<Callback[]>();
        mockHandler.handle(capture(passwordCapture));
        expectLastCall().andThrow(thrown);

        replay(mockHandler);

        final ScramSaslClient client = new ScramSaslClient(mockHandler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=" + clientNonce + "abcd,s=abcd,i=1")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getCause(), sameInstance((Throwable) thrown));
        }

        verify(mockHandler);

        assertThat(passwordCapture.getValue().length, is(1));
        assertThat(passwordCapture.getValue()[0],
                instanceOf(PasswordCallback.class));
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofWithBadIteration() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=" + clientNonce + "abcd,s=abcd,i=a")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(), is("For input string: \"a\""));
            assertThat(expected.getCause(),
                    instanceOf(NumberFormatException.class));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofWithBadIteration2() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=" + clientNonce + "abcd,s=abcd,i=0")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    is("Iteration count 0 must be a positive integer."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofWithBadSalt() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=" + clientNonce + "abcd,s=abc,i=4096")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    is("The server's salt is not a valid Base64 value: 'abc'."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofWithBadServerNonce() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=server_nonce,s=abcd,i=4096")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(
                    expected.getMessage(),
                    is("The server's nonce 'server_nonce' must start with the client's nonce '"
                            + clientNonce + "'."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     */
    @Test()
    public void testCreateProofWithMFieldThrows() {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);
        try {
            client.createProof(("m=is_not_allowed")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    is("The server required mandatory extension "
                            + "is not supported: m=is_not_allowed"));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofWithMissingIteration() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=" + clientNonce + "abcd,s=abcd")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    is("Could not find the iteration count: 'r=" + clientNonce
                            + "abcd,s=abcd'."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test()
    public void testCreateProofWithMissingSalt() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);

        // To create the client nonce.
        final byte[] initial = client.createInitialMessage();
        final String initialMessage = new String(initial, ScramSaslClient.UTF_8);
        final String clientNonce = initialMessage.substring(initialMessage
                .indexOf(",r=") + 3);

        try {
            client.createProof(("r=" + clientNonce + "abcd,i=4096")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    is("Could not find the server's salt: 'r=" + clientNonce
                            + "abcd,i=4096'."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#createProof(byte[])}.
     */
    @Test()
    public void testCreateProofWithMissingServerNonce() {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        final ScramSaslClient client = new ScramSaslClient(handler);
        try {
            client.createProof(("s=abcd,i=4096")
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    is("Could not find the server's nonce: 's=abcd,i=4096'."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#dispose()}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testDispose() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        // Hard code the nonce for the test.
        final ScramSaslClient client = new TestRfcScramSaslClient(handler);

        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        final byte[] firstMessage = client.evaluateChallenge(null);
        assertThat(client.isComplete(), is(false));
        assertThat(firstMessage, notNullValue());
        assertThat(new String(firstMessage, ScramSaslClient.UTF_8),
                is("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"));

        final byte[] proof = client
                .evaluateChallenge(("r=fyko+d2lbbFgONRv9qkxdawL"
                        + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(false));
        assertThat(proof, notNullValue());
        assertThat(new String(proof, ScramSaslClient.UTF_8),
                is("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                        + "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="));

        final byte[] last = client
                .evaluateChallenge("v=rmF9pqV8S7suAoZWja4dJRkFsKQ="
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(true));
        assertThat(last, notNullValue());
        assertThat(last.length, is(0));

        //
        // Now Dispose.
        //
        client.dispose();

        // And go again.
        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        client.evaluateChallenge(null);
        client.evaluateChallenge(("r=fyko+d2lbbFgONRv9qkxdawL"
                + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                .getBytes(ScramSaslClient.UTF_8));
        client.evaluateChallenge("v=rmF9pqV8S7suAoZWja4dJRkFsKQ="
                .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(true));

    }

    /**
     * Test method for {@link ScramSaslClient#evaluateChallenge(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testEvaluateChallenge() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        // Hard code the nonce for the test.
        final ScramSaslClient client = new TestRfcScramSaslClient(handler);

        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        final byte[] firstMessage = client.evaluateChallenge(null);
        assertThat(client.isComplete(), is(false));
        assertThat(firstMessage, notNullValue());
        assertThat(new String(firstMessage, ScramSaslClient.UTF_8),
                is("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"));

        final byte[] proof = client
                .evaluateChallenge(("r=fyko+d2lbbFgONRv9qkxdawL"
                        + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(false));
        assertThat(proof, notNullValue());
        assertThat(new String(proof, ScramSaslClient.UTF_8),
                is("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                        + "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="));

        final byte[] last = client
                .evaluateChallenge("v=rmF9pqV8S7suAoZWja4dJRkFsKQ="
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(true));
        assertThat(last, notNullValue());
        assertThat(last.length, is(0));

        // Calling evaluate again throws an error.
        try {
            client.evaluateChallenge(null);
            fail("Should have thrown an SaslException.");
        }
        catch (final SaslException good) {
            // Good.
            assertThat(good.getMessage(),
                    is("No challenge expected in state COMPLETE"));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#evaluateFinalResult(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testEvaluateFinalResult() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        // Hard code the nonce for the test.
        final ScramSaslClient client = new TestRfcScramSaslClient(handler);

        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        final byte[] firstMessage = client.evaluateChallenge(null);
        assertThat(client.isComplete(), is(false));
        assertThat(firstMessage, notNullValue());
        assertThat(new String(firstMessage, ScramSaslClient.UTF_8),
                is("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"));

        final byte[] proof = client
                .evaluateChallenge(("r=fyko+d2lbbFgONRv9qkxdawL"
                        + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(false));
        assertThat(proof, notNullValue());
        assertThat(new String(proof, ScramSaslClient.UTF_8),
                is("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                        + "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="));

        client.evaluateFinalResult("v=rmF9pqV8S7suAoZWja4dJRkFsKQ="
                .getBytes(ScramSaslClient.UTF_8));
    }

    /**
     * Test method for {@link ScramSaslClient#evaluateFinalResult(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testEvaluateFinalResultWithBadV() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        // Hard code the nonce for the test.
        final ScramSaslClient client = new TestRfcScramSaslClient(handler);

        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        final byte[] firstMessage = client.evaluateChallenge(null);
        assertThat(client.isComplete(), is(false));
        assertThat(firstMessage, notNullValue());
        assertThat(new String(firstMessage, ScramSaslClient.UTF_8),
                is("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"));

        final byte[] proof = client
                .evaluateChallenge(("r=fyko+d2lbbFgONRv9qkxdawL"
                        + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(false));
        assertThat(proof, notNullValue());
        assertThat(new String(proof, ScramSaslClient.UTF_8),
                is("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                        + "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="));

        try {
            client.evaluateFinalResult("v=BADVALUES7suAoZWja4dJRkFsQ="
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(
                    expected.getMessage(),
                    is("The server's signature ('BADVALUES7suAoZWja4dJRkFsQ=') "
                            + "does not match the expected signature: "
                            + "'rmF9pqV8S7suAoZWja4dJRkFsKQ='."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#evaluateFinalResult(byte[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testEvaluateFinalResultWithMissingV() throws SaslException {
        final CallbackHandler handler = new TestHandler("user", "pencil");

        // Hard code the nonce for the test.
        final ScramSaslClient client = new TestRfcScramSaslClient(handler);

        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        final byte[] firstMessage = client.evaluateChallenge(null);
        assertThat(client.isComplete(), is(false));
        assertThat(firstMessage, notNullValue());
        assertThat(new String(firstMessage, ScramSaslClient.UTF_8),
                is("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"));

        final byte[] proof = client
                .evaluateChallenge(("r=fyko+d2lbbFgONRv9qkxdawL"
                        + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(false));
        assertThat(proof, notNullValue());
        assertThat(new String(proof, ScramSaslClient.UTF_8),
                is("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                        + "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="));

        try {
            client.evaluateFinalResult("a=rmF9pqV8S7suAoZWja4dJRkFsKQ="
                    .getBytes(ScramSaslClient.UTF_8));
            fail("Should have thrown a SaslException.");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    is("The Server's final message did not contain "
                            + "a verifier: 'a=rmF9pqV8S7suAoZWja4dJRkFsKQ='"));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#getMechanismName()}.
     */
    @Test
    public void testGetMechanismName() {
        final ScramSaslClient client = new ScramSaslClient(null);

        assertThat(client.getMechanismName(), is("SCRAM-SHA-1"));
    }

    /**
     * Test method for {@link ScramSaslClient#getNegotiatedProperty(String)}.
     */
    @Test(expected = IllegalStateException.class)
    public void testGetNegotiatedProperty() {
        new ScramSaslClient(null).unwrap(null, 0, 0);
    }

    /**
     * Test method for {@link ScramSaslClient#hasInitialResponse()}.
     */
    @Test
    public void testHasInitialResponse() {
        final ScramSaslClient client = new ScramSaslClient(null);

        assertThat(client.hasInitialResponse(), is(true));
    }

    /**
     * Test based on the example in the RFC 5802, section 5. <blockquote>
     *
     * This is a simple example of a SCRAM-SHA-1 authentication exchange when
     * the client doesn't support channel bindings (username 'user' and password
     * 'pencil' are used):
     *
     * <pre>
     * C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
     * S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096
     * C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=
     * S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=
     * </pre>
     *
     * </blockquote>
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testRfcExample() throws SaslException {

        final CallbackHandler handler = new TestHandler("user", "pencil");

        // Hard code the nonce for the test.
        final ScramSaslClient client = new TestRfcScramSaslClient(handler);

        assertThat(client.hasInitialResponse(), is(true));
        assertThat(client.isComplete(), is(false));

        final byte[] firstMessage = client.evaluateChallenge(null);
        assertThat(client.isComplete(), is(false));
        assertThat(firstMessage, notNullValue());
        assertThat(new String(firstMessage, ScramSaslClient.UTF_8),
                is("n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"));

        final byte[] proof = client
                .evaluateChallenge(("r=fyko+d2lbbFgONRv9qkxdawL"
                        + "3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(false));
        assertThat(proof, notNullValue());
        assertThat(new String(proof, ScramSaslClient.UTF_8),
                is("c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,"
                        + "p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="));

        final byte[] last = client
                .evaluateChallenge("v=rmF9pqV8S7suAoZWja4dJRkFsKQ="
                        .getBytes(ScramSaslClient.UTF_8));
        assertThat(client.isComplete(), is(true));
        assertThat(last, notNullValue());
        assertThat(last.length, is(0));
    }

    /**
     * Test method for {@link ScramSaslClient#saslName(String)}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testSaslName() throws SaslException {
        final ScramSaslClient client = new ScramSaslClient(null);

        assertThat(client.saslName("abc"), is("abc"));
        assertThat(client.saslName("a=bc"), is("a=3Dbc"));
        assertThat(client.saslName("a,bc"), is("a=2Cbc"));
        assertThat(client.saslName("a=b,c"), is("a=3Db=2Cc"));
    }

    /**
     * Test method for {@link ScramSaslClient#saslPrep(char[])}.
     *
     * @throws SaslException
     *             On a test failure.
     */
    @Test
    public void testSaslPrep() throws SaslException {
        final ScramSaslClient client = new ScramSaslClient(null);

        assertThat(client.saslPrep("abc".toCharArray()),
                is("abc".toCharArray()));

        // Non-ASCII.
        assertThat(client.saslPrep("ab\u2345c".toCharArray()),
                is("ab\u2345c".toCharArray()));

        // Mappings.
        for (final Map.Entry<Character, String> mapping : ScramSaslClient.SASL_PREP_MAPPINGS
                .entrySet()) {
            assertThat(client.saslPrep(("ab"
                    + String.valueOf(mapping.getKey().charValue()) + "c")
                    .toCharArray()),
                    is(("ab" + mapping.getValue() + "c").toCharArray()));
        }

        // Disallowed.
        final Set<Integer> values = new HashSet<Integer>(
                ScramSaslClient.SASL_PREP_DISALLOWED);
        // ((0xE000 <= codePoint) && (codePoint <= 0xF8FF))
        // ((0xF0000 <= codePoint) && (codePoint <= 0xFFFFD))
        // ((0x100000 <= codePoint) && (codePoint <= 0x10FFFD)) ||
        // ((0xD800 <= codePoint) && (codePoint <= 0xDFFF))) {
        values.add(0xE000);
        values.add(0xF8FF);
        values.add(0xF0000);
        values.add(0xFFFFD);
        values.add(0x100000);
        values.add(0x10FFFD);
        values.add(0xD800);
        values.add(0xDFFF);
        for (final Integer disallowCodePoint : values) {

            final char[] chars = Character
                    .toChars(disallowCodePoint.intValue());
            if ((chars.length == 1)
                    && ScramSaslClient.SASL_PREP_MAPPINGS.containsKey(Character
                            .valueOf(chars[0]))) {
                // Mapping removes/replaces.
                continue;
            }

            final String disallowed = String.valueOf(chars);
            final String test = "ab" + disallowed + "c";
            if (!Normalizer.normalize(test, Normalizer.Form.NFKC).equals(test)) {
                // Normalization clears.
                continue;
            }

            try {
                final char[] testChars = test.toCharArray();
                final char[] result = client.saslPrep(testChars);
                fail("Should have thrown a SaslException: "
                        + String.valueOf(result));
            }
            catch (final SaslException expected) {
                assertThat(
                        expected.getMessage(),
                        is("SaslPrep disallowed character '"
                                + disallowed
                                + "' (0x"
                                + Integer.toHexString(disallowCodePoint
                                        .intValue())
                                + "). See RFC 4013 section 2.3."));
            }
        }

        // Bidi
        // DIRECTIONALITY_RIGHT_TO_LEFT ==> \u05be
        // DIRECTIONALITY_RIGHT_TO_LEFT_ARABIC ==> \uFEFC
        // DIRECTIONALITY_LEFT_TO_RIGHT ==> \u0041
        client.saslPrep("\u05be\uFEFC".toCharArray());
        client.saslPrep("\uFEFC\u05be".toCharArray());
        client.saslPrep("\u0041".toCharArray());
        try {
            client.saslPrep("\u05be\u0041".toCharArray());
            fail("Should have thrown a SaslException");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    startsWith("SaslPrep does not allow mixing left-to-right"));
        }
        try {
            client.saslPrep("\u05be\u0041\uFEFC".toCharArray());
            fail("Should have thrown a SaslException");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    startsWith("SaslPrep does not allow mixing left-to-right"));
        }
        try {
            client.saslPrep("\u0041\uFEFC".toCharArray());
            fail("Should have thrown a SaslException");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    startsWith("SaslPrep does not allow left-to-right"));
        }

        // Unassigned \uFDD0
        try {
            client.saslPrep("\uFDD0".toCharArray());
            fail("Should have thrown a SaslException");
        }
        catch (final SaslException expected) {
            assertThat(expected.getMessage(),
                    startsWith("SaslPrep disallowed character '\uFDD0' "
                            + "(0xfdd0). See RFC 4013 section 2.3."));
        }
    }

    /**
     * Test method for {@link ScramSaslClient#unwrap(byte[], int, int)}.
     */
    @Test(expected = IllegalStateException.class)
    public void testUnwrap() {
        new ScramSaslClient(null).unwrap(null, 0, 0);
    }

    /**
     * Test method for {@link ScramSaslClient#wrap(byte[], int, int)}.
     */
    @Test(expected = IllegalStateException.class)
    public void testWrap() {
        new ScramSaslClient(null).wrap(null, 0, 0);
    }

    /**
     * TestHandler provides a simple callback for user names and passwords for
     * use in the tests.
     *
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class TestHandler
            implements CallbackHandler {

        /** The password to provide via the handler. */
        private final String myPassword;

        /** The user name to provide via the handler. */
        private final String myUserName;

        /**
         * Creates a new TestHandler.
         *
         * @param userName
         *            The user name to provide via the handler.
         * @param password
         *            The password to provide via the handler.
         */
        public TestHandler(final String userName, final String password) {
            myUserName = userName;
            myPassword = password;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to respond to the user name and password callbacks.
         * </p>
         */
        @Override
        public void handle(final Callback[] callbacks) throws IOException,
                UnsupportedCallbackException {
            for (final Callback cb : callbacks) {
                if (cb instanceof PasswordCallback) {
                    ((PasswordCallback) cb).setPassword(myPassword
                            .toCharArray());
                }
                if (cb instanceof NameCallback) {
                    ((NameCallback) cb).setName(myUserName);
                }
            }

        }
    }

    /**
     * TestRfcScramSaslClient provides a specialization of the
     * {@link ScramSaslClient} that returns a fixed nonce.
     *
     * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    protected static final class TestRfcScramSaslClient
            extends ScramSaslClient {
        /**
         * Creates a new TestRfcScramSaslClient.
         *
         * @param callbackHandler
         *            The handler for the user name and password.
         */
        protected TestRfcScramSaslClient(final CallbackHandler callbackHandler) {
            super(callbackHandler);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to return the nonce from RFC 5802, section 5.
         * </p>
         */
        @Override
        protected String createNonce() {
            return "fyko+d2lbbFgONRv9qkxdawL";
        }
    }

}
