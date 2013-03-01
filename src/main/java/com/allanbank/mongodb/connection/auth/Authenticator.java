/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * Authenticator provides the common interface for all MongoDB authenticators.
 * <p>
 * A single Authenticator instance will only ever be used with a single
 * Connection and set of credentials. The "clone()" method is used to quickly
 * allocate a new instance for a connection allowing for shared
 * "preauthentication" work to be done.
 * </p>
 * <p>
 * The {@link #startAuthentication(Credential, Connection)} method may assume
 * that it is only invoked from a single thread. The "result" method should not
 * make the same assumption.
 * </p>
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public interface Authenticator extends Cloneable {

    /**
     * Privides the ability to clone the authenticator. A new
     * {@link Authenticator} instance is created for each physical connection in
     * use.
     * <p>
     * Using clone allows users to create a "template" version of the
     * authenticator that is then copied prior to use by each connection.
     * </p>
     * 
     * @return The cloned authenticator.
     */
    public Authenticator clone();

    /**
     * Returns the results of the authentication attempt.
     * 
     * @return True if the user is successfully authenticated on the connection,
     *         false if the authentication fails.
     * @throws MongoDbAuthenticationException
     *             On a failure in the protocol to authenticate the user on the
     *             connection.
     */
    public boolean result() throws MongoDbAuthenticationException;

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
    public void startAuthentication(Credential credentials,
            Connection connection) throws MongoDbAuthenticationException;
}
