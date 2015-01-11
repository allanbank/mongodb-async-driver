/*
 * #%L
 * AuthenticatingConnection.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.client.Message;
import com.allanbank.mongodb.client.callback.ReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * AuthenticatingConnection provides a connection that authenticated with the
 * server for each database before it is used.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticatingConnection
        extends AbstractProxyConnection {

    /** The name of the administration database. */
    public static final String ADMIN_DB_NAME = MongoClientConfiguration.ADMIN_DB_NAME;

    /** The databases that can be used to grant access to other databases. */
    public static final Set<String> DELEGATE_DB_NAMES;

    /**
     * The name of the external database used by Kerberos, plain-sasl and other
     * authenticators.
     */
    public static final String EXTERNAL_DB_NAME = "$external";

    /** The logger for the authenticator. */
    public static final Log LOG = LogFactory
            .getLog(AuthenticatingConnection.class);

    /** Maximum time to wait to re-try authentication: 5 minutes. */
    public static final long MAX_RETRY_INTERVAL_MS = TimeUnit.MINUTES
            .toMillis(5);

    /** How often to re-try authentication. */
    public static final long RETRY_INTERVAL_MS = 10;

    /** A single atomic for all of the connections. */
    private static final AtomicLongFieldUpdater<AuthenticatingConnection> ourTimeSetter;

    static {
        final Set<String> delegateDbNames = new HashSet<String>();
        delegateDbNames.add(ADMIN_DB_NAME);
        delegateDbNames.add(EXTERNAL_DB_NAME);

        DELEGATE_DB_NAMES = Collections.unmodifiableSet(delegateDbNames);

        ourTimeSetter = AtomicLongFieldUpdater.newUpdater(
                AuthenticatingConnection.class, "myAuthenticationTime");
    }

    /**
     * The last time that we started or finished trying to authenticate with the
     * server.
     */
    private volatile long myAuthenticationTime;

    /** Map of the authenticators. */
    private final ConcurrentMap<String, Authenticator> myAuthenticators;

    /** The client configuration. */
    private final MongoClientConfiguration myConfig;

    /** Set of the databases with authentication failures. */
    private final Map<String, MongoDbException> myFailures;

    /** The time to wait until the next authentication retry. */
    private volatile long myRetryInterval;

    /** Map of the authenticators. */
    private final Set<String> myToRetry;

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
        super(connection);

        myAuthenticators = new ConcurrentHashMap<String, Authenticator>();
        myFailures = new ConcurrentHashMap<String, MongoDbException>();
        myToRetry = new ConcurrentSkipListSet<String>();
        myAuthenticationTime = System.currentTimeMillis();
        myConfig = config;
        myRetryInterval = RETRY_INTERVAL_MS;

        // With the advent of delegated credentials we must now authenticate
        // with all available credentials immediately.
        final Collection<Credential> credentials = config.getCredentials();
        for (final Credential credential : credentials) {

            startAuthentication(credential);

            // Boo! MongoDB does not support concurrent authentication attempts.
            // Block here for the results. Boo!
            //
            // Make sure that all of the outstanding authentication attempts are
            // completed.
            checkPendingAuthenticators(/* waitForComplete= */true);
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
    public void send(final Message message1, final Message message2,
            final ReplyCallback replyCallback) throws MongoDbException {
        ensureAuthenticated(message1);
        ensureAuthenticated(message2);

        super.send(message1, message2, replyCallback);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Makes sure the connection is authenticated for the current database
     * before forwarding to the proxied connection.
     * </p>
     */
    @Override
    public void send(final Message message, final ReplyCallback replyCallback)
            throws MongoDbException {
        ensureAuthenticated(message);

        super.send(message, replyCallback);
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
     * Ensures that the results of all of the pending authenticators are
     * complete.
     *
     * @param waitToComplete
     *            If true then this method will block for the authentication to
     *            complete.
     */
    private void checkPendingAuthenticators(final boolean waitToComplete) {
        if (!myAuthenticators.isEmpty()) {
            final Iterator<Map.Entry<String, Authenticator>> iter = myAuthenticators
                    .entrySet().iterator();
            while (iter.hasNext()) {
                final Map.Entry<String, Authenticator> entry = iter.next();

                final String dbName = entry.getKey();
                final Authenticator authenticator = entry.getValue();
                if (waitToComplete || authenticator.finished()) {
                    try {
                        // Get the result to ensure the authentication is
                        // complete.
                        if (!authenticator.result()) {
                            myFailures.put(dbName,
                                    new MongoDbAuthenticationException(
                                            "Authentication failed for the "
                                                    + dbName + " database."));
                        }
                        else {
                            myFailures.remove(dbName);
                        }
                    }
                    catch (final MongoDbException error) {
                        // Just log the error here.
                        LOG.warn(error, "Authentication failed: {}",
                                error.getMessage());
                        // Re-throw if our DB.
                        myFailures.put(dbName, error);
                    }
                    finally {
                        myAuthenticators.remove(dbName, authenticator);
                        myToRetry.remove(dbName);
                        ourTimeSetter.set(this, System.currentTimeMillis());
                    }
                }
            }
        }
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
        // Check if the authentication results are done.
        checkPendingAuthenticators(/* waitForComplete= */false);

        MongoDbException error = myFailures.get(message.getDatabaseName());
        if (error != null) {
            // See if we retry the authentication if it now works.
            retryFailedAuthenticators(message.getDatabaseName());

            // Can fail as we know that the authentication is required to access
            // the database.
            throw new MongoDbAuthenticationException(error);
        }

        for (final String delegateName : DELEGATE_DB_NAMES) {
            error = myFailures.get(delegateName);
            if (error != null) {
                // We have no way of knowing if the credentials are needed for
                // the request so we don't cause a hard failure. We will still
                // see if we retry the authentication if it now works.
                retryFailedAuthenticators(delegateName);
            }
        }
    }

    /**
     * Retries the authentication after a short pause.
     * <p>
     * The pause time is not configurable and set to {@value #RETRY_INTERVAL_MS}
     * ms to match the <i>Server Discovery and Monitoring<i> specification. We
     * back off each retry until the {@value #MAX_RETRY_INTERVAL_MS} ms.
     * </p>
     *
     * @param databaseName
     *            The name of the database that we need to retry the
     *            authentication on.
     *
     * @see <a
     *      href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring">Server
     *      Discovery and Monitoring</a>
     */
    private void retryFailedAuthenticators(final String databaseName) {
        final long now = System.currentTimeMillis();
        final long last = myAuthenticationTime;

        myToRetry.add(databaseName);

        if (((last + myRetryInterval) < now) && myAuthenticators.isEmpty()
                && ourTimeSetter.compareAndSet(this, last, now)) {

            final Collection<Credential> credentials = myConfig
                    .getCredentials();

            // Shuffle the list to randomize the retries otherwise we can get
            // stuck retrying the same database over and over as the retry set
            // is sorted.
            final List<String> toRetry = new ArrayList<String>(myToRetry);
            Collections.shuffle(toRetry);

            for (final String dbName : toRetry) {
                for (final Credential credential : credentials) {
                    if (dbName.equals(credential.getDatabase())) {
                        startAuthentication(credential);

                        myRetryInterval = Math.min(myRetryInterval
                                + RETRY_INTERVAL_MS, MAX_RETRY_INTERVAL_MS);

                        // Let the authentication finish.
                        return;
                    }
                }

                // Did not find a credential for the database. Remove it and
                // move to the next one. This is likely the $external or
                // admin database that we check just to be sure. (Although
                // we should only get here if the first auth attempt failed...)
                myToRetry.remove(dbName);

                // No credentials any more. Remove the failure.
                myFailures.remove(dbName);
            }
        }
    }

    /**
     * Starts the process of getting a user authenticated with MongoDB using a
     * specific credential.
     *
     * @param credential
     *            The credential to use to authenticate against the database.
     */
    private void startAuthentication(final Credential credential) {
        final Authenticator authenticator = credential.authenticator();

        if (myAuthenticators.putIfAbsent(credential.getDatabase(),
                authenticator) == null) {
            authenticator.startAuthentication(credential,
                    getProxiedConnection());
        }
    }
}
