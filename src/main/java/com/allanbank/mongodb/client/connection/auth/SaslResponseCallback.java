/*
 * #%L
 * SaslResponseCallback.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.callback.AbstractValidatingReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * SaslResponseCallback provides the callback for all of the SASL requests.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
/* package */final class SaslResponseCallback
        extends AbstractValidatingReplyCallback {

    /** An empty set of bytes. */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /** The database to authenticate against. */
    public static final String EXTERNAL = "$external";

    /** The SASL client holding the authentication state. */
    private final SaslClient myClient;

    /** The connection to authenticate with. */
    private final Connection myConnection;

    /** The future to update with the results. */
    private final FutureCallback<Boolean> myResults;

    /**
     * Creates a new SaslResponseCallback.
     *
     * @param client
     *            The SASL client holding the authentication state.
     * @param connection
     *            The connection to authenticate with.
     * @param results
     *            The future to update with the results.
     */
    public SaslResponseCallback(final SaslClient client,
            final Connection connection, final FutureCallback<Boolean> results) {
        myClient = client;
        myConnection = connection;
        myResults = results;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to update the results with the failure.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myResults.exception(thrown);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to handle each SASL response until the exchange is complete.
     * </p>
     */
    @Override
    public void handle(final Reply reply) {
        final MongoDbException exception = asError(reply);
        if (exception != null) {
            exception(exception);
            return;
        }

        try {
            final Document replyDoc = extractDocument(reply);
            final boolean done = extractDone(replyDoc);
            final byte[] payload = extractPayload(replyDoc);

            // If the server thinks we are done...
            if (done) {
                // See what the client thinks.
                boolean clientDone = myClient.isComplete();
                if (!clientDone) {
                    // Maybe this last message will make it complete.
                    myClient.evaluateChallenge(payload);
                    clientDone = myClient.isComplete();
                }

                if (clientDone) {
                    myResults.callback(Boolean.TRUE);
                }
                else {
                    myResults.exception(new MongoDbAuthenticationException(
                            "The SASL Client did not complete "
                                    + "when the server did."));
                }
            }
            else {
                final byte[] response = myClient.evaluateChallenge(payload);
                sendReply(replyDoc, response);
            }
        }
        catch (final MongoDbException error) {
            exception(error);
        }
        catch (final SaslException e) {
            exception(new MongoDbAuthenticationException(e));
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return true to make sure the authentication is processed on
     * a thread we control.
     * </p>
     */
    @Override
    public boolean isLightWeight() {
        return true;
    }

    /**
     * Extracts the first document from the reply. Throws an exception if the
     * reply does not have atleast 1 document.
     *
     * @param reply
     *            The reply to extract the document from.
     * @return The first document in the reply.
     * @throws MongoDbAuthenticationException
     *             If the reply does not have atleast 1 document.
     */
    private Document extractDocument(final Reply reply)
            throws MongoDbAuthenticationException {
        if (reply.getResults().size() > 0) {
            return reply.getResults().get(0);
        }
        throw new MongoDbAuthenticationException(
                "Did not receive a valid reply to an authentication command.");
    }

    /**
     * Extracts the boolean done field. If the field does not exist or does not
     * contain a boolean then false is returned.
     *
     * @param reply
     *            The reply document to extract the response from.
     * @return The done field from the response or false if it does not exist.
     */
    private boolean extractDone(final Document reply) {
        boolean done = false;

        final BooleanElement doneElement = reply.get(BooleanElement.class,
                "done");
        if (doneElement != null) {
            done = doneElement.getValue();
        }

        return done;
    }

    /**
     * Extracts the payload from the reply document. If there is no payload then
     * an empty byte array is returned.
     *
     * @param reply
     *            The reply document to extract the payload from.
     * @return The payload bytes.
     */
    private byte[] extractPayload(final Document reply) {
        byte[] payload = EMPTY_BYTES;

        final BinaryElement payloadElement = reply.get(BinaryElement.class,
                "payload");
        if (payloadElement != null) {
            payload = payloadElement.getValue();
        }

        return payload;
    }

    /**
     * Sends a reply to a challenge from the server.
     *
     * @param reply
     *            The reply with the challenge from the server.
     * @param response
     *            The response to the challenge.
     */
    private void sendReply(final Document reply, final byte[] response) {
        final DocumentBuilder cmd = BuilderFactory.start();
        cmd.add("saslContinue", 1);
        cmd.add(reply.get("conversationId"));
        cmd.add("payload", response);

        myConnection.send(
                new Command(EXTERNAL, Command.COMMAND_COLLECTION, cmd.build()),
                this);
    }
}