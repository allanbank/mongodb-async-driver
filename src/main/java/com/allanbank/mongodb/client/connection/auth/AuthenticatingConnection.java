/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.auth;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * AuthenticatingConnection provides a connection that authenticated with the
 * server for each database before it is used.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticatingConnection extends AbstractProxyConnection {

    /** The name of the administration database. */
    public static final String ADMIN_DB_NAME = MongoClientConfiguration.ADMIN_DB_NAME;

    /** The logger for the authenticator. */
    public static final Logger LOG = Logger
            .getLogger(AuthenticatingConnection.class.getName());

    /** Map of the authenticators. */
    private final Map<String, Authenticator> myAuthenticators;

    /** Set of the databases with authentication failures. */
    private final Map<String, MongoDbException> myFailures;

    /**
     * Creates a new AuthenticatingConnection.
     * 
     * @param connection
     *            The connection to ensure gets authenticated as needed.
     * @param config
     *            The MongoDB client configuration.
     */
    public AuthenticatingConnection(final Connection connection,
            final MongoClientConfiguration config) {
        super(connection, config);

        myAuthenticators = new ConcurrentHashMap<String, Authenticator>();
        myFailures = new ConcurrentHashMap<String, MongoDbException>();

        // With the advent of delegated credentials we must now authenticate
        // with all available credentials immediately.
        final Collection<Credential> credentials = config.getCredentials();
        for (final Credential credential : credentials) {
            final Authenticator authenticator = credential.authenticator();

            authenticator.startAuthentication(credential, connection);

            // Boo! MongoDB does not support concurrent authentication attempts.
            // Block here for the results if more than 1 credential. Boo!
            if (credentials.size() > 1) {
                try {
                    if (!authenticator.result()) {
                        myFailures.put(credential.getDatabase(),
                                new MongoDbAuthenticationException(
                                        "Authentication failed for the "
                                                + credential.getDatabase()
                                                + " database."));
                    }
                }
                catch (final MongoDbException error) {
                    myFailures.put(credential.getDatabase(), error);
                }
            }
            else {
                myAuthenticators.put(credential.getDatabase(), authenticator);
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Makes sure the connection is authenticated for the current database
     * before forwarding to the proxied connection.
     * </p>
     */
    @Override
    public String send(final Message message,
            final Callback<Reply> replyCallback) throws MongoDbException {
        ensureAuthenticated(message);

        return super.send(message, replyCallback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Makes sure the connection is authenticated for the current database
     * before forwarding to the proxied connection.
     * </p>
     */
    @Override
    public String send(final Message message1, final Message message2,
            final Callback<Reply> replyCallback) throws MongoDbException {
        ensureAuthenticated(message1);
        ensureAuthenticated(message2);

        return super.send(message1, message2, replyCallback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the socket information.
     * </p>
     */
    @Override
    public String toString() {
        return "Auth(" + getProxiedConnection() + ")";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to give access to the proxied connections to tests.
     * </p>
     */
    @Override
    protected Connection getProxiedConnection() {
        final Connection proxied = super.getProxiedConnection();

        return proxied;
    }

    /**
     * Ensures the connection has either already authenticated with the server
     * or completes the authentication.
     * 
     * @param message
     *            The message to authenticate for.
     * @throws MongoDbAuthenticationException
     *             On a failure to authenticate with the MongDB server.
     */
    private void ensureAuthenticated(final Message message)
            throws MongoDbAuthenticationException {
        // Check the authentication results are done.
        if (!myAuthenticators.isEmpty()) {
            final Iterator<Map.Entry<String, Authenticator>> iter = myAuthenticators
                    .entrySet().iterator();
            while (iter.hasNext()) {
                final Map.Entry<String, Authenticator> authenticator = iter
                        .next();
                try {
                    if (!authenticator.getValue().result()) {
                        myFailures.put(authenticator.getKey(),
                                new MongoDbAuthenticationException(
                                        "Authentication failed for the "
                                                + authenticator.getKey()
                                                + " database."));
                    }
                }
                catch (final MongoDbException error) {
                    // Just log the error here.
                    LOG.log(Level.WARNING,
                            "Authentication failed: " + error.getMessage(),
                            error);
                    // Re-throw if our DB.
                    myFailures.put(authenticator.getKey(), error);
                }
                finally {
                    iter.remove();
                }
            }
        }

        if (myFailures.containsKey(message.getDatabaseName())) {
            throw new MongoDbAuthenticationException(myFailures.get(message
                    .getDatabaseName()));
        }
    }
}
