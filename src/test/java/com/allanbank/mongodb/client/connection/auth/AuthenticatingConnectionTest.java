/*
 * Copyright 2012-2014, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.auth;

import static com.allanbank.mongodb.bson.builder.BuilderFactory.start;
import static com.allanbank.mongodb.client.connection.CallbackReply.cb;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

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

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

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

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb());
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(injected));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

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

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                anyObject(ReplyCallback.class));
        expectLastCall().andThrow(injected);

        // Just a log message (now).

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

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

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Nonce.
        mockConnetion.send(
                eq(new Command(TEST_DB + '1', myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(
                eq(new Command(TEST_DB + '1', myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnetion.send(msg, null);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth -- Failure
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(start(myAuthReply).remove("ok")));
        expectLastCall();

        // Nonce.
        mockConnetion.send(
                eq(new Command(TEST_DB + '1', myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth -- Failure 2
        mockConnetion.send(
                eq(new Command(TEST_DB + '1', myAuthRequest.build())),
                cb(start(myAuthReply).remove("ok").add("ok", 0)));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

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

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb());
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);
        try {
            conn.send(msg, null);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }
        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb());
        expectLastCall().andThrow(injected);

        replay(mockConnetion);

        try {
            final AuthenticatingConnection conn = new AuthenticatingConnection(
                    mockConnetion, myConfig);

            fail("Should throw an exception when nonce fails.");

            conn.close();
        }
        catch (final MongoDbException good) {
            // Good.
            assertSame(injected, good);
        }

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command("foo", myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command("foo", myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnetion.send(msg, null);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnetion.send(msg, reply);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(msg, reply);

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnetion.send(msg, msg2, null);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(msg, msg2, null);

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnetion.send(msg, null);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command("admin", myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command("admin", myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnetion.send(msg, null);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnetion);
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

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        // Message.
        mockConnetion.send(msg, null);
        expectLastCall();

        // Message, again.
        mockConnetion.send(msg, null);
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(msg, null);
        conn.send(msg, null);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for {@link AuthenticatingConnection#toString()}.
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testToString() throws IOException {
        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        mockConnetion.send(eq(new Command(TEST_DB, myNonceRequest.build())),
                cb(myNonceReply));
        expectLastCall();

        // Auth.
        mockConnetion.send(eq(new Command(TEST_DB, myAuthRequest.build())),
                cb(myAuthReply));
        expectLastCall();

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        assertEquals(
                "Auth(EasyMock for interface com.allanbank.mongodb.client.connection.Connection)",
                conn.toString());

        IOUtils.close(conn);

        verify(mockConnetion);
    }
}
