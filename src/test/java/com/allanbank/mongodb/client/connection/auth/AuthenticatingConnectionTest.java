/*
 * #%L
 * AuthenticatingConnectionTest.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import static com.allanbank.mongodb.bson.builder.BuilderFactory.start;
import static com.allanbank.mongodb.client.connection.CallbackReply.cb;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.Durability;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.FutureReplyCallback;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Delete;
import com.allanbank.mongodb.client.message.GetLastError;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AuthenticatingConnectionTest provides test for the
 * {@link AuthenticatingConnection}.
 *
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticatingConnectionTest {

    /** An empty document for use in constructing messages. */
    public static final Document EMPTY_DOC = BuilderFactory.start().build();

    /** The database name used for testing. */
    public static final String TEST_DB = "db";

    /** The authenticate reply message. */
    private DocumentBuilder myAuthReply;

    /** The authenticate request message. */
    private DocumentBuilder myAuthRequest;

    /** A configuration setup to authenticate requests. */
    private MongoClientConfiguration myConfig;

    /** The nonce reply message. */
    private DocumentBuilder myNonceReply;

    /** The nonce request message. */
    private DocumentBuilder myNonceRequest;

    /**
     * Creates the basic authenticate messages.
     */
    @Before
    public void setUp() {
        myConfig = new MongoClientConfiguration();
        myConfig.addCredential(Credential.builder().userName("allanbank")
                .password("super_secret_password".toCharArray())
                .database(TEST_DB).authenticationType(Credential.MONGODB_CR)
                .build());

        myNonceRequest = BuilderFactory.start();
        myNonceRequest.addInteger("getnonce", 1);

        myNonceReply = BuilderFactory.start();
        myNonceReply.addString("nonce", "deadbeef4bee");

        myAuthRequest = BuilderFactory.start();
        myAuthRequest.addInteger("authenticate", 1);
        myAuthRequest.addString("nonce", "deadbeef4bee");
        myAuthRequest.addString("user", "allanbank");
        myAuthRequest.addString("key", "d74c03a816dee3427c7459dce9c94e54");

        myAuthReply = BuilderFactory.start();
        myAuthReply.addInteger("ok", 1);
    }

    /**
     * Cleans up the test.
     */
    @After
    public void tearDown() {
        myConfig = null;
        myNonceRequest = null;
        myNonceReply = null;
        myAuthRequest = null;
        myAuthReply = null;
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFails() throws IOException {

        myAuthReply.reset();
        myAuthReply.addInteger("ok", 0);

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            // Good. Ask once. Keep failing.
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFailsNonNumericOk() throws IOException {

        myAuthReply.reset();
        myAuthReply.addString("ok", "foo");

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            // Good. Ask once. Keep failing.
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFailsNoNonceInReplyDoc() throws IOException {

        myNonceReply.reset();
        myNonceReply.addString("not_a_nonce", "foo");

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFailsNoNonceReplyDoc() throws IOException {

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb());
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFailsWithRetry() throws IOException {

        myAuthReply.reset();
        myAuthReply.addInteger("ok", 0);

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            // Good. Ask once. Keep failing.
        }
        verify(mockConnection);

        // Setup for the auth retry.
        reset(mockConnection);
        myAuthReply.reset();
        myAuthReply.addInteger("ok", 1);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        // Wait for the retry interval to expire.
        sleep(AuthenticatingConnection.RETRY_INTERVAL_MS
                + AuthenticatingConnection.RETRY_INTERVAL_MS);

        // Note that normally the auth would be delayed but with the EasyMock
        // callbacks the authentication completes on this thread.
        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication fails.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
            fail("Should not throw an exception when authentication retry succeeds.");
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFailsWithRetryAndCredentialRemoved()
            throws IOException {

        myAuthReply.reset();
        myAuthReply.addInteger("ok", 0);

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            // Good. Ask once. Keep failing.
        }
        verify(mockConnection);

        // Setup for the auth retry.
        reset(mockConnection);

        // Remove the credentials.
        final List<Credential> noCredentials = Collections.emptyList();
        myConfig.setCredentials(noCredentials);

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        // Wait for the retry interval to expire.
        sleep(AuthenticatingConnection.RETRY_INTERVAL_MS
                + AuthenticatingConnection.RETRY_INTERVAL_MS);

        // Note that normally the auth would be delayed but with the EasyMock
        // callbacks the authentication completes on this thread.
        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication fails.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
            fail("Should not throw an exception when authentication credentials are removed.");
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFailsWithRetryExternal() throws IOException {
        myConfig.addCredential(Credential.builder().userName("allanbank")
                .password("super_secret_password".toCharArray())
                .database(AuthenticatingConnection.EXTERNAL_DB_NAME)
                .authenticationType(Credential.MONGODB_CR).build());

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Nonce - external
        mockConnection.send(eq(new Command(
                AuthenticatingConnection.EXTERNAL_DB_NAME,
                Command.COMMAND_COLLECTION, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth - external
        myAuthReply.reset();
        myAuthReply.addInteger("ok", 0);
        mockConnection.send(eq(new Command(
                AuthenticatingConnection.EXTERNAL_DB_NAME,
                Command.COMMAND_COLLECTION, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            fail("Should not fail when the authentication succeeds on the database.");
        }

        verify(mockConnection);

        // Setup for the auth retry.
        reset(mockConnection);
        myAuthReply.reset();
        myAuthReply.addInteger("ok", 1);

        // Nonce.
        mockConnection.send(eq(new Command(
                AuthenticatingConnection.EXTERNAL_DB_NAME,
                Command.COMMAND_COLLECTION, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth - external
        myAuthReply.reset();
        myAuthReply.addInteger("ok", 1);
        mockConnection.send(eq(new Command(
                AuthenticatingConnection.EXTERNAL_DB_NAME,
                Command.COMMAND_COLLECTION, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        // Wait for the retry interval to expire.
        sleep(AuthenticatingConnection.RETRY_INTERVAL_MS
                + AuthenticatingConnection.RETRY_INTERVAL_MS);

        // Note that normally the auth would be delayed but with the EasyMock
        // callbacks the authentication completes on this thread.
        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            fail("Should not fail when the authentication succeeds on the database.");
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateFailsWithRetryFails() throws IOException {

        myAuthReply.reset();
        myAuthReply.addInteger("ok", 0);

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
        }
        catch (final MongoDbAuthenticationException good) {
            // Good. Ask once. Keep failing.
        }
        verify(mockConnection);

        // Setup for the auth retry.
        reset(mockConnection);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        // Wait for the retry interval to expire.
        sleep(AuthenticatingConnection.RETRY_INTERVAL_MS
                + AuthenticatingConnection.RETRY_INTERVAL_MS);

        // Note that normally the auth would be delayed but with the EasyMock
        // callbacks the authentication completes on this thread.
        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication fails.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication fails.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good. Ask once. Keep failing.
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateReplyException() throws IOException {

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);
        final MongoDbException injected = new MongoDbException("Injected");

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(injected));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbException good) {
            // Good.
            assertSame(injected, good.getCause().getCause().getCause());
        }

        // And again.
        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbException good) {
            // Good.
            assertSame(injected, good.getCause().getCause().getCause());
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthenticateRequestFails() throws IOException {
        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);
        final MongoDbException injected = new MongoDbException("Injected");

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), anyObject(ReplyCallback.class));
        expectLastCall().andThrow(injected);

        // Just a log message (now).

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication fails.");
        }
        catch (final MongoDbException good) {
            // Good.
            // Our error should be in there somewhere.
            Throwable thrown = good;
            while (thrown != null) {
                if (injected == thrown) {
                    break;
                }
                thrown = thrown.getCause();
            }
            assertSame(injected, thrown);
        }

        // And again.
        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbException good) {
            // Good.
            // Our error should be in there somewhere.
            Throwable thrown = good;
            while (thrown != null) {
                if (injected == thrown) {
                    break;
                }
                thrown = thrown.getCause();
            }
            assertSame(injected, thrown);
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthWithMultipleCredentials() throws IOException {
        myConfig.addCredential(Credential.builder().userName("allanbank")
                .password("super_secret_password".toCharArray())
                .database(TEST_DB + '1')
                .authenticationType(Credential.MONGODB_CR).build());

        final Delete msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB + '1',
                Command.COMMAND_COLLECTION, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB + '1',
                Command.COMMAND_COLLECTION, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testAuthWithMultipleCredentialsFailures() throws IOException {
        myConfig.addCredential(Credential.builder().userName("allanbank")
                .password("super_secret_password".toCharArray())
                .database(TEST_DB + '1')
                .authenticationType(Credential.MONGODB_CR).build());

        final Delete msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth -- Failure
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(start(myAuthReply).remove("ok")));
        expectLastCall();

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB + '1',
                Command.COMMAND_COLLECTION, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth -- Failure 2
        mockConnection.send(eq(new Command(TEST_DB + '1',
                Command.COMMAND_COLLECTION, myAuthRequest.build())),
                cb(start(myAuthReply).remove("ok").add("ok", 0)));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Authentication should have failed.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }
        finally {
            IOUtils.close(conn);
        }

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testNoAuthenticateDoc() throws IOException {

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb());
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testNoAuthenticateOkDoc() throws IOException {
        myAuthReply.reset();
        myAuthReply.addInteger("not_ok", 0);

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);
        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }
        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testNonceFails() throws IOException {

        final MongoDbException injected = new MongoDbException("Injected");

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb());
        expectLastCall().andThrow(injected);

        replay(mockConnection);

        try {
            final AuthenticatingConnection conn = new AuthenticatingConnection(
                    mockConnection, myConfig);

            fail("Should throw an exception when nonce fails.");

            conn.close();
        }
        catch (final MongoDbException good) {
            // Good.
            assertSame(injected, good);
        }

        verify(mockConnection);
    }

    /**
     * Test that the old and new password hash values match.
     *
     * @throws NoSuchAlgorithmException
     *             On a failure.
     */
    @Test
    @Deprecated
    public void testPasswordHash() throws NoSuchAlgorithmException {
        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.authenticate("allanbank", "super_secret_password");

        final Credential credential = Credential.builder()
                .userName("allanbank")
                .password("super_secret_password".toCharArray())
                .database(TEST_DB).authenticationType(Credential.MONGODB_CR)
                .build();

        assertEquals(config.getPasswordHash(),
                new MongoDbAuthenticator().passwordHash(credential));
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendAdminMessageArrayAsNonAdmin() throws IOException {

        myConfig.setCredentials(Arrays.asList(Credential.builder()
                .userName("allanbank")
                .password("super_secret_password".toCharArray())
                .database("foo").authenticationType(Credential.MONGODB_CR)
                .build()));

        final Delete msg = new Delete(MongoClientConfiguration.ADMIN_DB_NAME,
                "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command("foo", Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command("foo", Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for
     * {@link AuthenticatingConnection#send(Message, ReplyCallback)} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendCallbackOfReplyMessageArray() throws IOException {

        final ReplyCallback reply = new FutureReplyCallback();
        final Delete msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, reply);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        conn.send(msg, reply);

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendMessage2() throws IOException {

        final Delete msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);
        final GetLastError msg2 = new GetLastError(TEST_DB, Durability.ACK);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, msg2, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        conn.send(msg, msg2, null);

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendMessageArray() throws IOException {

        final Delete msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendMessageArrayAsAdmin() throws IOException {

        myConfig.setCredentials(Arrays.asList(Credential.builder()
                .userName("allanbank")
                .password("super_secret_password".toCharArray())
                .authenticationType(Credential.MONGODB_CR).build()));

        final Delete msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command("admin", Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command("admin", Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#send} .
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendOnlyAuthenticateOnce() throws IOException {

        final Message msg = new Delete(TEST_DB, "collection", EMPTY_DOC, true);

        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnection.send(msg, null);
        expectLastCall();

        // Message, again.
        mockConnection.send(msg, null);
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        conn.send(msg, null);
        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Test method for {@link AuthenticatingConnection#toString()}.
     *
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testToString() throws IOException {
        final Connection mockConnection = createMock(Connection.class);

        // Nonce.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myNonceRequest.build())), cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnection.send(eq(new Command(TEST_DB, Command.COMMAND_COLLECTION,
                myAuthRequest.build())), cb(myAuthReply));
        expectLastCall();

        mockConnection.close();
        expectLastCall();

        replay(mockConnection);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnection, myConfig);

        assertEquals(
                "Auth(EasyMock for interface com.allanbank.mongodb.client.connection.Connection)",
                conn.toString());

        IOUtils.close(conn);

        verify(mockConnection);
    }

    /**
     * Pauses for the specified amount of time.
     *
     * @param timeMs
     *            The time to sleep in milliseconds.
     */
    private void sleep(final long timeMs) {
        try {
            Thread.sleep(timeMs);
        }
        catch (final InterruptedException e) {
            // Ignore
        }
    }
}
