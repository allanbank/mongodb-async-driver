/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.client.AbstractReplyCallback;
import com.allanbank.mongodb.client.AbstractValidatingReplyCallback;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * MongoDbAuthenticator provides an authenticator for the legacy, pre-2.4
 * version, of MongoDB authentication.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbAuthenticator implements Authenticator {

    /** The UTF-8 character encoding. */
    public static final Charset ASCII = Charset.forName("US-ASCII");

    /** The result of the Authentication attempt. */
    protected FutureCallback<Boolean> myResults = new FutureCallback<Boolean>();

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
     * {@inheritDoc}
     * <p>
     * Overriden to returns the results of the authentication, once complete.
     * </p>
     */
    @Override
    public boolean result() throws MongoDbAuthenticationException {
        try {
            return myResults.get().booleanValue();
        }
        catch (final InterruptedException e) {
            throw new MongoDbAuthenticationException(e);
        }
        catch (final ExecutionException e) {
            throw new MongoDbAuthenticationException(e);
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
    public String passwordHash(Credential credentials)
            throws NoSuchAlgorithmException {
        final MessageDigest md5 = MessageDigest.getInstance("MD5");

        return passwordHash(md5, credentials);
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
            Credential credentials) {

        final char[] password = credentials.getPassword();
        ByteBuffer bb = ASCII.encode(CharBuffer.wrap(password));

        md5.update((credentials.getUsername() + ":mongo:").getBytes(ASCII));
        md5.update(bb.array(), 0, bb.limit());

        Arrays.fill(password, ' ');
        Arrays.fill(bb.array(), (byte) 0);

        return IOUtils.toHex(md5.digest());
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

            connection.send(
                    new Command(credential.getDatabase(), builder.build()),
                    new NonceReplyCallback(credential, connection));
        }
        catch (final MongoDbException errorOnSend) {
            myResults.exception(errorOnSend);

            throw errorOnSend;
        }
    }

    /**
     * AuthenticateReplyCallback provides the callback for the second step of
     * the authentication.
     * 
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    private class AuthenticateReplyCallback extends
            AbstractReplyCallback<Boolean> {
        /**
         * Creates a new AuthenticateReplyCallback.
         */
        public AuthenticateReplyCallback() {
            super(myResults);
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
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
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
         * Overridden to retreive the nonce from the reply and request
         * authentication.
         * </p>
         */
        @Override
        protected void handle(final Reply reply) {
            final MongoDbException exception = asError(reply);
            if (exception != null) {
                exception(exception);
                return;
            }

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
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                final String passwordHash = passwordHash(md5, myCredential);

                String text = nonce.getValue() + myCredential.getUsername()
                        + passwordHash;
                
                md5.reset();
                md5.update(text.getBytes(ASCII));
                final byte[] bytes = md5.digest();

                DocumentBuilder builder = BuilderFactory.start();
                builder = BuilderFactory.start();
                builder.addInteger("authenticate", 1);
                builder.add(nonce.getName(), nonce.getValue());
                builder.addString("user", myCredential.getUsername());
                builder.addString("key", IOUtils.toHex(bytes));

                myConnection.send(new Command(myCredential.getDatabase(),
                        builder.build()), new AuthenticateReplyCallback());
            }
            catch (final NoSuchAlgorithmException e) {
                exception(new MongoDbAuthenticationException(e));
            }
        }

    }
}
