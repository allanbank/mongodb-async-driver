/*
 * #%L
 * MongoDbAuthenticator.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.client.FutureCallback;
import com.allanbank.mongodb.client.callback.AbstractReplyCallback;
import com.allanbank.mongodb.client.callback.AbstractValidatingReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.message.Command;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * MongoDbAuthenticator provides an authenticator for the legacy, pre-2.4
 * version, of MongoDB authentication.
 * 
 * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbAuthenticator extends AbstractAuthenticator implements
        Authenticator {

    /** The UTF-8 character encoding. */
    public static final Charset ASCII = Charset.forName("US-ASCII");

    /**
     * Creates a new MongoDbAuthenticator.
     */
    public MongoDbAuthenticator() {
        super();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a new authenticator.
     * </p>
     */
    @Override
    public MongoDbAuthenticator clone() {
        try {
            final MongoDbAuthenticator newAuth = (MongoDbAuthenticator) super
                    .clone();
            newAuth.myResults = new FutureCallback<Boolean>();

            return newAuth;
        }
        catch (final CloneNotSupportedException cannotHappen) {
            return new MongoDbAuthenticator();
        }
    }

    /**
     * Creates the MongoDB authentication hash of the password.
     * 
     * @param credentials
     *            The credentials to hash.
     * @return The hashed password/myCredential.
     * @throws NoSuchAlgorithmException
     *             On a failure to create a MD5 message digest.
     */
    public String passwordHash(final Credential credentials)
            throws NoSuchAlgorithmException {
        final MessageDigest md5 = MessageDigest.getInstance("MD5");

        return passwordHash(md5, credentials);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to authenticate with MongoDB using the native/legacy
     * authentication mechanisms.
     * </p>
     */
    @Override
    public void startAuthentication(final Credential credential,
            final Connection connection) throws MongoDbAuthenticationException {

        try {
            final DocumentBuilder builder = BuilderFactory.start();
            builder.addInteger("getnonce", 1);

            connection.send(new Command(credential.getDatabase(),
                    Command.COMMAND_COLLECTION, builder.build()),
                    new NonceReplyCallback(credential, connection));
        }
        catch (final MongoDbException errorOnSend) {
            myResults.exception(errorOnSend);

            throw errorOnSend;
        }
    }

    /**
     * Creates the MongoDB authentication hash of the password.
     * 
     * @param md5
     *            The MD5 digest to compute the hash.
     * @param credentials
     *            The credentials to hash.
     * @return The hashed password/myCredential.
     */
    protected String passwordHash(final MessageDigest md5,
            final Credential credentials) {

        final char[] password = credentials.getPassword();
        final ByteBuffer bb = ASCII.encode(CharBuffer.wrap(password));

        md5.update((credentials.getUserName() + ":mongo:").getBytes(ASCII));
        md5.update(bb.array(), 0, bb.limit());

        Arrays.fill(password, ' ');
        Arrays.fill(bb.array(), (byte) 0);

        return IOUtils.toHex(md5.digest());
    }

    /**
     * AuthenticateReplyCallback provides the callback for the second step of
     * the authentication.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    private static class AuthenticateReplyCallback extends
            AbstractReplyCallback<Boolean> {
        /**
         * Creates a new AuthenticateReplyCallback.
         * 
         * @param results
         *            The results to update once the reply is received.
         */
        public AuthenticateReplyCallback(final FutureCallback<Boolean> results) {
            super(results);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to convert the reply to a boolean based on if the
         * authentication worked.
         * </p>
         */
        @Override
        protected Boolean convert(final Reply reply) throws MongoDbException {
            boolean current = false;
            final List<Document> results = reply.getResults();
            if (results.size() == 1) {
                final Document doc = results.get(0);
                final Element okElem = doc.get("ok");
                if (okElem != null) {
                    final int okValue = toInt(okElem);
                    if (okValue == 1) {
                        current = true;
                    }

                }
            }

            return Boolean.valueOf(current);
        }
    }

    /**
     * NonceReplyCallback provides the callback for the reply to the nonce
     * request.
     * 
     * @copyright 2013-2014, Allanbank Consulting, Inc., All Rights Reserved
     */
    private class NonceReplyCallback extends AbstractValidatingReplyCallback {

        /** The connection to authenticate on. */
        private final Connection myConnection;

        /** The credentials to use in authentication. */
        private final Credential myCredential;

        /**
         * Creates a new NonceReplyCallback.
         * 
         * @param credential
         *            The credentials to use in authentication.
         * @param connection
         *            The connection to authenticate on.
         */
        public NonceReplyCallback(final Credential credential,
                final Connection connection) {
            myCredential = credential;
            myConnection = connection;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to set the exception on the results for the
         * authentication.
         * </p>
         */
        @Override
        public void exception(final Throwable thrown) {
            myResults.exception(thrown);
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to return true to make sure the authentication is
         * processed on a thread we control.
         * </p>
         */
        @Override
        public boolean isLightWeight() {
            return true;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Overridden to retrieve the nonce from the reply and request
         * authentication.
         * </p>
         */
        @Override
        protected void handle(final Reply reply) {

            StringElement nonce = null;
            if (reply.getResults().size() > 0) {
                final Document doc = reply.getResults().get(0);
                nonce = doc.get(StringElement.class, "nonce");
                if (nonce == null) {
                    // Bad reply. Try again.
                    exception(new MongoDbAuthenticationException(
                            "Bad response from nonce request."));
                    return;
                }
            }
            else {
                // Bad reply. Try again.
                exception(new MongoDbAuthenticationException(
                        "Bad response from nonce request."));
                return;
            }

            // Send an authenticate request.
            try {
                final MessageDigest md5 = MessageDigest.getInstance("MD5");
                final String passwordHash = passwordHash(md5, myCredential);

                final String text = nonce.getValue()
                        + myCredential.getUserName() + passwordHash;

                md5.reset();
                md5.update(text.getBytes(ASCII));
                final byte[] bytes = md5.digest();

                final DocumentBuilder builder = BuilderFactory.start();
                builder.addInteger("authenticate", 1);
                builder.add(nonce);
                builder.addString("user", myCredential.getUserName());
                builder.addString("key", IOUtils.toHex(bytes));

                myConnection.send(new Command(myCredential.getDatabase(),
                        Command.COMMAND_COLLECTION, builder.build()),
                        new AuthenticateReplyCallback(myResults));
            }
            catch (final NoSuchAlgorithmException e) {
                exception(new MongoDbAuthenticationException(e));
            }
            catch (final RuntimeException e) {
                exception(new MongoDbAuthenticationException(e));
            }
        }
    }
}
