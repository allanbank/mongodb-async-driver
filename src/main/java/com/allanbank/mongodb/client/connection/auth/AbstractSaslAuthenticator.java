/*
 * #%L
 * AbstractSaslAuthenticator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * AbstractSaslAuthenticator provides the basic logic for a SASL based
 * authenticator.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the
 *         extensions.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractSaslAuthenticator
        extends AbstractAuthenticator {
    /** An empty set of bytes. */
    public static final byte[] EMPTY_BYTES = SaslResponseCallback.EMPTY_BYTES;

    /** The database to authenticate against. */
    public static final String EXTERNAL = SaslResponseCallback.EXTERNAL;

    /**
     * Creates a new AbstractSaslAuthenticator.
     */
    public AbstractSaslAuthenticator() {
        super();
    }

    /**
     * Starts to authenticate the user with the specified credentials.
     *
     * @param credentials
     *            The credentials to use to login to the database.
     * @param connection
     *            The connection to authenticate the user with.
     * @throws MongoDbAuthenticationException
     *             On a failure in the protocol to authenticate the user on the
     *             connection.
     */
    public void startAuthentication(final Credential credentials,
            final Connection connection) throws MongoDbAuthenticationException {
        try {
            final SaslClient client = createSaslClient(credentials, connection);

            if (client != null) {
                byte[] payload = EMPTY_BYTES;
                if (client.hasInitialResponse()) {
                    payload = client.evaluateChallenge(payload);
                }

                sendStart(payload, connection, new SaslResponseCallback(client,
                        connection, myResults));
            }
            else {
                throw new MongoDbAuthenticationException(
                        "Could not locate a SASL provider.");
            }
        }
        catch (final SaslException e) {
            throw new MongoDbAuthenticationException(e);
        }
    }

    /**
     * Creates the SASL Client.
     *
     * @param credentials
     *            The credentials to use in creating the client.
     * @param connection
     *            The connection we are authenticating against.
     * @return The {@link SaslClient} for the authentication.
     * @throws MongoDbAuthenticationException
     *             On an authentication error.
     * @throws SaslException
     *             On a SASL error.
     */
    protected abstract SaslClient createSaslClient(
            final Credential credentials, final Connection connection)
            throws MongoDbAuthenticationException, SaslException;

    /**
     * The SASL mechanism name.
     *
     * @return The SASL mechanism name.
     */
    protected abstract String getMechanism();

    /**
     * Sends an initial request to authenticate with the server.
     *
     * @param payload
     *            The payload for the request.
     * @param connection
     *            The connection to authenticate.
     * @param callback
     *            The callback to handle the reply.
     */
    protected void sendStart(final byte[] payload, final Connection connection,
            final ReplyCallback callback) {
        final DocumentBuilder cmd = BuilderFactory.start();
        cmd.add("saslStart", 1);
        cmd.add("mechanism", getMechanism());
        cmd.add("payload", payload);

        connection.send(
                new Command(EXTERNAL, Command.COMMAND_COLLECTION, cmd.build()),
                callback);
    }

}