/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AuthenticatingConnection provides a connection that authenticated with the
 * server for each database before it is used.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticatingConnection extends AbstractProxyConnection {

    /** The name of the administration database. */
    public static final String ADMIN_DB_NAME = MongoClientConfiguration.ADMIN_DB_NAME;

    /** Map containing the Futures for the reply to the authenticate requests. */
    private final ConcurrentMap<String, Future<Reply>> myAuthReplys;

    /** Map of the authentication results. */
    private final ConcurrentMap<String, Boolean> myAuthResponse;

    /** Map containing the Futures for the reply to the get_nonce requests. */
    private final ConcurrentMap<String, Future<Reply>> myAuthTokens;

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
        myAuthTokens = new ConcurrentHashMap<String, Future<Reply>>();
        myAuthReplys = new ConcurrentHashMap<String, Future<Reply>>();
        myAuthResponse = new ConcurrentHashMap<String, Boolean>();
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
     * Converts a {@link NumericElement} into an <tt>int</tt> value. If not a
     * {@link NumericElement} then -1 is returned.
     * 
     * @param element
     *            The element to convert.
     * @return The element's integer value or -1.
     */
    protected int toInt(final Element element) {
        if (element instanceof NumericElement) {
            return ((NumericElement) element).getIntValue();
        }

        return -1;
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
        final String name;
        if (myConfig.isAdminUser()) {
            name = ADMIN_DB_NAME;
        }
        else if (ADMIN_DB_NAME.equals(message.getDatabaseName())) {
            name = myConfig.getDefaultDatabase();
        }
        else {
            name = message.getDatabaseName();
        }

        Boolean current = myAuthResponse.get(name);
        if (current == null) {
            FutureCallback<Reply> replyCallback = null;
            Future<Reply> alreadySent = null;
            try {
                DocumentBuilder builder = BuilderFactory.start();
                builder.addInteger("getnonce", 1);

                replyCallback = new FutureCallback<Reply>();
                alreadySent = myAuthTokens.putIfAbsent(name, replyCallback);
                if (alreadySent == null) {
                    getProxiedConnection().send(
                            new Command(name, builder.build()), replyCallback);
                    alreadySent = replyCallback;
                    replyCallback = null;
                }

                // Read the nonce reply.
                String nonce = "";
                Reply reply = alreadySent.get();
                if (reply.getResults().size() > 0) {
                    final Document doc = reply.getResults().get(0);
                    final List<StringElement> strElem = doc.find(
                            StringElement.class, "nonce");
                    if (strElem.size() > 0) {
                        nonce = strElem.get(0).getValue();
                    }
                    else {
                        // Bad reply. Try again.
                        myAuthTokens.remove(name, alreadySent);
                        throw new MongoDbAuthenticationException(
                                "Bad response from nonce request.");
                    }
                }
                else {
                    // Bad reply. Try again.
                    myAuthTokens.remove(name, alreadySent);
                    throw new MongoDbAuthenticationException(
                            "Bad response from nonce request.");
                }

                // Send an authenticate request.
                final MessageDigest md5 = MessageDigest.getInstance("MD5");
                final byte[] bytes = md5
                        .digest((nonce + myConfig.getUserName() + myConfig
                                .getPasswordHash())
                                .getBytes(MongoClientConfiguration.UTF8));

                builder = BuilderFactory.start();
                builder.addInteger("authenticate", 1);
                builder.addString("user", myConfig.getUserName());
                builder.addString("nonce", nonce);
                builder.addString("key", IOUtils.toHex(bytes));

                replyCallback = new FutureCallback<Reply>();
                alreadySent = myAuthReplys.putIfAbsent(name, replyCallback);
                if (alreadySent == null) {
                    getProxiedConnection().send(
                            new Command(name, builder.build()), replyCallback);
                    alreadySent = replyCallback;
                    replyCallback = null;
                }

                // Read the reply.
                reply = alreadySent.get();
                current = Boolean.FALSE;
                final List<Document> results = reply.getResults();
                if (results.size() == 1) {
                    final Document doc = results.get(0);
                    final Element okElem = doc.get("ok");
                    if (okElem != null) {
                        final int okValue = toInt(okElem);
                        if (okValue == 1) {
                            current = Boolean.TRUE;
                        }

                        myAuthResponse.put(name, current);
                    }
                    else {
                        // Bad reply. Try again.
                        myAuthReplys.remove(name, alreadySent);
                        throw new MongoDbAuthenticationException(
                                "Bad response from authenticate request.");
                    }
                }
                else {
                    // Bad reply. Try again.
                    myAuthReplys.remove(name, alreadySent);
                    throw new MongoDbAuthenticationException(
                            "Bad response from authenticate request.");
                }
            }
            catch (final InterruptedException e) {
                // Bad reply. Try again.
                myAuthTokens.remove(name, alreadySent);
                myAuthReplys.remove(name, alreadySent);

                throw new MongoDbAuthenticationException(e);
            }
            catch (final ExecutionException e) {
                // Bad reply. Try again.
                myAuthTokens.remove(name, alreadySent);
                myAuthReplys.remove(name, alreadySent);

                throw new MongoDbAuthenticationException(e.getCause());
            }
            catch (final NoSuchAlgorithmException e) {
                throw new MongoDbAuthenticationException(e);
            }
            catch (final MongoDbException errorOnSend) {
                if (replyCallback != null) {
                    replyCallback.exception(errorOnSend);
                }
                myAuthTokens.remove(name, replyCallback);
                myAuthReplys.remove(name, replyCallback);

                throw errorOnSend;
            }
        }

        if (!current.booleanValue()) {
            throw new MongoDbAuthenticationException("Authentication to the '"
                    + name + "' database failed.");
        }
    }
}
