/*
 * Copyright 2013-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.auth;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;

import org.easymock.Capture;
import org.junit.Test;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * MongoDbAuthenticatorTest provides tests for the {@link MongoDbAuthenticator}.
 *
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbAuthenticatorTest {

    /**
     * Test method for
     * {@link MongoDbAuthenticator#startAuthentication(Credential, Connection)}
     * .
     */
    @Test
    public void testAuthentication() {
        final Credential credential = Credential
                .builder()
                .userName("allanbank")
                .password(
                        new char[] { 's', 'u', 'p', 'e', 'r', 's', 'e', 'c',
                                'r', 'e', 't' }).build();

        final Capture<Message> getNonceCapture = new Capture<Message>();
        final Capture<ReplyCallback> getNonceReplyCapture = new Capture<ReplyCallback>();
        final Capture<Message> authCapture = new Capture<Message>();
        final Capture<ReplyCallback> authReplyCapture = new Capture<ReplyCallback>();

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.send(capture(getNonceCapture),
                capture(getNonceReplyCapture));
        expectLastCall();
        mockConnection.send(capture(authCapture), capture(authReplyCapture));
        expectLastCall();

        replay(mockConnection);

        final MongoDbAuthenticator authenticator = new MongoDbAuthenticator();

        // Start the process with a getnonce command.
        authenticator.startAuthentication(credential, mockConnection);

        // Should have sent the getnonce command.
        assertThat(getNonceCapture.hasCaptured(), is(true));
        assertThat(getNonceCapture.getValue(), instanceOf(Command.class));

        final Command nonceCommand = (Command) getNonceCapture.getValue();
        assertThat(nonceCommand.getCommand(),
                is(BuilderFactory.start().add("getnonce", 1).build()));

        assertThat(getNonceReplyCapture.hasCaptured(), is(true));

        // Trigger the next stage of processing.
        Document reply = BuilderFactory.start()
                .add("nonce", "0123456789abcdef").build();
        getNonceReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        // And the auth command is sent.
        assertThat(authCapture.hasCaptured(), is(true));
        assertThat(authCapture.getValue(), instanceOf(Command.class));

        final Command authCommand = (Command) authCapture.getValue();
        assertThat(
                authCommand.getCommand(),
                is(BuilderFactory.start().addInteger("authenticate", 1)
                        .add(reply.get("nonce"))
                        .addString("user", credential.getUserName())
                        .addString("key", "54d8cd8954719ec33c7166807c164969")
                        .build()));

        assertThat(authReplyCapture.hasCaptured(), is(true));

        // and trigger the auth.
        reply = BuilderFactory.start().add("ok", 1).build();
        authReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        assertThat(authenticator.result(), is(true));

        verify(mockConnection);
    }

    /**
     * Test method for
     * {@link MongoDbAuthenticator#startAuthentication(Credential, Connection)}
     * .
     */
    @Test
    public void testAuthenticationFails() {
        final Credential credential = Credential
                .builder()
                .userName("allanbank")
                .password(
                        new char[] { 's', 'u', 'p', 'e', 'r', 's', 'e', 'c',
                                'r', 'e', 't' }).build();

        final Capture<Message> getNonceCapture = new Capture<Message>();
        final Capture<ReplyCallback> getNonceReplyCapture = new Capture<ReplyCallback>();
        final Capture<Message> authCapture = new Capture<Message>();
        final Capture<ReplyCallback> authReplyCapture = new Capture<ReplyCallback>();

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.send(capture(getNonceCapture),
                capture(getNonceReplyCapture));
        expectLastCall();
        mockConnection.send(capture(authCapture), capture(authReplyCapture));
        expectLastCall();

        replay(mockConnection);

        final MongoDbAuthenticator authenticator = new MongoDbAuthenticator();

        // Start the process with a getnonce command.
        authenticator.startAuthentication(credential, mockConnection);

        // Should have sent the getnonce command.
        assertThat(getNonceCapture.hasCaptured(), is(true));
        assertThat(getNonceCapture.getValue(), instanceOf(Command.class));

        final Command nonceCommand = (Command) getNonceCapture.getValue();
        assertThat(nonceCommand.getCommand(),
                is(BuilderFactory.start().add("getnonce", 1).build()));

        assertThat(getNonceReplyCapture.hasCaptured(), is(true));

        // Trigger the next stage of processing.
        Document reply = BuilderFactory.start()
                .add("nonce", "0123456789abcdef").build();
        getNonceReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        // And the auth command is sent.
        assertThat(authCapture.hasCaptured(), is(true));
        assertThat(authCapture.getValue(), instanceOf(Command.class));

        final Command authCommand = (Command) authCapture.getValue();
        assertThat(
                authCommand.getCommand(),
                is(BuilderFactory.start().addInteger("authenticate", 1)
                        .add(reply.get("nonce"))
                        .addString("user", credential.getUserName())
                        .addString("key", "54d8cd8954719ec33c7166807c164969")
                        .build()));

        assertThat(authReplyCapture.hasCaptured(), is(true));

        // and trigger the auth failure.
        reply = BuilderFactory.start().build();
        authReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        assertThat(authenticator.result(), is(false));

        verify(mockConnection);
    }

    /**
     * Test method for
     * {@link MongoDbAuthenticator#startAuthentication(Credential, Connection)}
     * .
     */
    @Test
    public void testAuthenticationNonceReplyMissingNonce() {
        final Credential credential = Credential
                .builder()
                .userName("allanbank")
                .password(
                        new char[] { 's', 'u', 'p', 'e', 'r', 's', 'e', 'c',
                                'r', 'e', 't' }).build();

        final Capture<Message> getNonceCapture = new Capture<Message>();
        final Capture<ReplyCallback> getNonceReplyCapture = new Capture<ReplyCallback>();

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.send(capture(getNonceCapture),
                capture(getNonceReplyCapture));
        expectLastCall();

        replay(mockConnection);

        final MongoDbAuthenticator authenticator = new MongoDbAuthenticator();

        // Start the process with a getnonce command.
        authenticator.startAuthentication(credential, mockConnection);

        // Should have sent the getnonce command.
        assertThat(getNonceCapture.hasCaptured(), is(true));
        assertThat(getNonceCapture.getValue(), instanceOf(Command.class));

        final Command nonceCommand = (Command) getNonceCapture.getValue();
        assertThat(nonceCommand.getCommand(),
                is(BuilderFactory.start().add("getnonce", 1).build()));

        assertThat(getNonceReplyCapture.hasCaptured(), is(true));

        // Trigger the next stage of processing.
        final Document reply = BuilderFactory.start().build();
        getNonceReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        try {
            authenticator.result();
            fail("Should have thrown a MongoDbAuthenticationException");
        }
        catch (final MongoDbAuthenticationException error) {
            // Good.
        }

        verify(mockConnection);
    }

    /**
     * Test method for
     * {@link MongoDbAuthenticator#startAuthentication(Credential, Connection)}
     * .
     */
    @Test
    public void testAuthenticationNonceReplyMissingResults() {
        final Credential credential = Credential
                .builder()
                .userName("allanbank")
                .password(
                        new char[] { 's', 'u', 'p', 'e', 'r', 's', 'e', 'c',
                                'r', 'e', 't' }).build();

        final Capture<Message> getNonceCapture = new Capture<Message>();
        final Capture<ReplyCallback> getNonceReplyCapture = new Capture<ReplyCallback>();

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.send(capture(getNonceCapture),
                capture(getNonceReplyCapture));
        expectLastCall();

        replay(mockConnection);

        final MongoDbAuthenticator authenticator = new MongoDbAuthenticator();

        // Start the process with a getnonce command.
        authenticator.startAuthentication(credential, mockConnection);

        // Should have sent the getnonce command.
        assertThat(getNonceCapture.hasCaptured(), is(true));
        assertThat(getNonceCapture.getValue(), instanceOf(Command.class));

        final Command nonceCommand = (Command) getNonceCapture.getValue();
        assertThat(nonceCommand.getCommand(),
                is(BuilderFactory.start().add("getnonce", 1).build()));

        assertThat(getNonceReplyCapture.hasCaptured(), is(true));

        // Trigger the next stage of processing.
        final List<Document> reply = Collections.emptyList();
        getNonceReplyCapture.getValue().callback(
                new Reply(1, 0, 0, reply, true, false, false, false));

        try {
            authenticator.result();
            fail("Should have thrown a MongoDbAuthenticationException");
        }
        catch (final MongoDbAuthenticationException error) {
            // Good.
        }

        verify(mockConnection);
    }

    /**
     * Test method for
     * {@link MongoDbAuthenticator#startAuthentication(Credential, Connection)}
     * .
     */
    @Test
    public void testAuthenticationNonceReplyNotOk() {
        final Credential credential = Credential
                .builder()
                .userName("allanbank")
                .password(
                        new char[] { 's', 'u', 'p', 'e', 'r', 's', 'e', 'c',
                                'r', 'e', 't' }).build();

        final Capture<Message> getNonceCapture = new Capture<Message>();
        final Capture<ReplyCallback> getNonceReplyCapture = new Capture<ReplyCallback>();

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.send(capture(getNonceCapture),
                capture(getNonceReplyCapture));
        expectLastCall();

        replay(mockConnection);

        final MongoDbAuthenticator authenticator = new MongoDbAuthenticator();

        // Start the process with a getnonce command.
        authenticator.startAuthentication(credential, mockConnection);

        // Should have sent the getnonce command.
        assertThat(getNonceCapture.hasCaptured(), is(true));
        assertThat(getNonceCapture.getValue(), instanceOf(Command.class));

        final Command nonceCommand = (Command) getNonceCapture.getValue();
        assertThat(nonceCommand.getCommand(),
                is(BuilderFactory.start().add("getnonce", 1).build()));

        assertThat(getNonceReplyCapture.hasCaptured(), is(true));

        // Trigger the next stage of processing.
        final Document reply = BuilderFactory.start().add("ok", 0)
                .add("nonce", "0123456789abcdef").build();
        getNonceReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        try {
            authenticator.result();
            fail("Should have thrown a MongoDbAuthenticationException");
        }
        catch (final MongoDbAuthenticationException error) {
            // Good.
        }

        verify(mockConnection);
    }

    /**
     * Test method for {@link MongoDbAuthenticator#result()} .
     */
    @Test
    public void testResultWhenInterrupted() {
        final Credential credential = Credential
                .builder()
                .userName("allanbank")
                .password(
                        new char[] { 's', 'u', 'p', 'e', 'r', 's', 'e', 'c',
                                'r', 'e', 't' }).build();

        final Capture<Message> getNonceCapture = new Capture<Message>();
        final Capture<ReplyCallback> getNonceReplyCapture = new Capture<ReplyCallback>();
        final Capture<Message> authCapture = new Capture<Message>();
        final Capture<ReplyCallback> authReplyCapture = new Capture<ReplyCallback>();

        final Connection mockConnection = createMock(Connection.class);

        mockConnection.send(capture(getNonceCapture),
                capture(getNonceReplyCapture));
        expectLastCall();
        mockConnection.send(capture(authCapture), capture(authReplyCapture));
        expectLastCall();

        replay(mockConnection);

        final MongoDbAuthenticator authenticator = new MongoDbAuthenticator();

        // Start the process with a getnonce command.
        authenticator.startAuthentication(credential, mockConnection);

        // Should have sent the getnonce command.
        assertThat(getNonceCapture.hasCaptured(), is(true));
        assertThat(getNonceCapture.getValue(), instanceOf(Command.class));

        final Command nonceCommand = (Command) getNonceCapture.getValue();
        assertThat(nonceCommand.getCommand(),
                is(BuilderFactory.start().add("getnonce", 1).build()));

        assertThat(getNonceReplyCapture.hasCaptured(), is(true));

        // Trigger the next stage of processing.
        Document reply = BuilderFactory.start()
                .add("nonce", "0123456789abcdef").build();
        getNonceReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        // And the auth command is sent.
        assertThat(authCapture.hasCaptured(), is(true));
        assertThat(authCapture.getValue(), instanceOf(Command.class));

        final Command authCommand = (Command) authCapture.getValue();
        assertThat(
                authCommand.getCommand(),
                is(BuilderFactory.start().addInteger("authenticate", 1)
                        .add(reply.get("nonce"))
                        .addString("user", credential.getUserName())
                        .addString("key", "54d8cd8954719ec33c7166807c164969")
                        .build()));

        assertThat(authReplyCapture.hasCaptured(), is(true));

        // and trigger the auth.
        reply = BuilderFactory.start().add("ok", 1).build();
        authReplyCapture.getValue().callback(
                new Reply(1, 0, 0, Collections.singletonList(reply), true,
                        false, false, false));

        // Should not clear the interrupted state.
        Thread.currentThread().interrupt();
        assertThat(authenticator.result(), is(true));
        assertThat(Thread.interrupted(), is(true));

        verify(mockConnection);
    }
}
