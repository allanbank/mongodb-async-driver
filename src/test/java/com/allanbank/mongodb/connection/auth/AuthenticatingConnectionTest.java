/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import static com.allanbank.mongodb.connection.CallbackReply.cb;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Delete;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AuthenticatingConnectionTest provides test for the
 * {@link AuthenticatingConnection}.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticatingConnectionTest {

    /** An empty document for use in constructing messages. */
    public static final Document EMPTY_DOC = BuilderFactory.start().build();

    /** The address for the test. */
    private String myAddress = null;

    /** The authenticate reply message. */
    private DocumentBuilder myAuthReply;

    /** The authenticate request message. */
    private DocumentBuilder myAuthRequest;

    /** A configuration setup to authenticate requests. */
    private MongoDbConfiguration myConfig;

    /** The nonce reply message. */
    private DocumentBuilder myNonceReply;

    /** The nonce request message. */
    private DocumentBuilder myNonceRequest;

    /**
     * Creates the basic authenticate messages.
     */
    @Before
    public void setUp() {
        myConfig = new MongoDbConfiguration();
        myConfig.authenticate("allanbank", "super_secret_password");

        myNonceRequest = BuilderFactory.start();
        myNonceRequest.addInteger("getnonce", 1);

        myNonceReply = BuilderFactory.start();
        myNonceReply.addString("nonce", "deadbeef4bee");

        myAuthRequest = BuilderFactory.start();
        myAuthRequest.addInteger("authenticate", 1);
        myAuthRequest.addString("user", myConfig.getUsername());
        myAuthRequest.addString("nonce", "deadbeef4bee");
        myAuthRequest.addString("key", "d74c03a816dee3427c7459dce9c94e54");

        myAuthReply = BuilderFactory.start();
        myAuthReply.addInteger("ok", 1);

        myAddress = "localhost:27017";
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
        myAddress = null;
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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(null, msg);
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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbAuthenticationException good) {
            // Good.
        }

        try {
            conn.send(null, msg);
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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(),
                        eq(new Command("db", myNonceRequest.build()))))
                .andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final MongoDbException injected = new MongoDbException("Injected");

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(injected), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Retry.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Message.
        expect(mockConnetion.send(null, msg)).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbException good) {
            // Good.
            assertSame(injected, good.getCause());
        }

        conn.send(null, msg);

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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final MongoDbException injected = new MongoDbException("Injected");

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andThrow(injected);

        // Retry.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Message.
        expect(mockConnetion.send(null, msg)).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbException good) {
            // Good.
            assertSame(injected, good);
        }

        conn.send(null, msg);

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
    public void testNoAuthenticateDoc() throws IOException {

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(),
                        eq(new Command("db", myAuthRequest.build()))))
                .andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);
        final MongoDbException injected = new MongoDbException("Injected");

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andThrow(injected);

        // Retry.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Message.
        expect(mockConnetion.send(null, msg)).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        try {
            conn.send(null, msg);
            fail("Should throw an exception when authentication falis.");
        }
        catch (final MongoDbException good) {
            // Good.
            assertSame(injected, good);
        }

        conn.send(null, msg);

        IOUtils.close(conn);

        verify(mockConnetion);
    }

    /**
     * Test method for
     * {@link AuthenticatingConnection#send(Callback, Message[])} .
     * 
     * @throws IOException
     *             On a failure setting up the mocks for the test.
     */
    @Test
    public void testSendCallbackOfReplyMessageArray() throws IOException {

        final Callback<Reply> reply = new FutureCallback<Reply>();
        final Delete msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Message.
        expect(mockConnetion.send(reply, msg)).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(reply, msg);

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

        final Delete msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Message.
        expect(mockConnetion.send(null, msg)).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(null, msg);

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

        myConfig.authenticateAsAdmin("allanbank", "super_secret_password");

        final Delete msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("admin",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("admin",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Message.
        expect(mockConnetion.send(null, msg)).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(null, msg);

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

        final Message msg = new Delete("db", "collection", EMPTY_DOC, true);

        final Connection mockConnetion = createMock(Connection.class);

        // Nonce.
        expect(
                mockConnetion.send(cb(myNonceReply), eq(new Command("db",
                        myNonceRequest.build())))).andReturn(myAddress);

        // Auth.
        expect(
                mockConnetion.send(cb(myAuthReply), eq(new Command("db",
                        myAuthRequest.build())))).andReturn(myAddress);

        // Message.
        expect(mockConnetion.send(null, msg)).andReturn(myAddress);

        // Message, again.
        expect(mockConnetion.send(null, msg)).andReturn(myAddress);

        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        conn.send(null, msg);
        conn.send(null, msg);

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
        mockConnetion.close();
        expectLastCall();

        replay(mockConnetion);

        final AuthenticatingConnection conn = new AuthenticatingConnection(
                mockConnetion, myConfig);

        assertEquals(
                "Auth(EasyMock for interface com.allanbank.mongodb.connection.Connection)",
                conn.toString());

        IOUtils.close(conn);

        verify(mockConnetion);
    }
}
