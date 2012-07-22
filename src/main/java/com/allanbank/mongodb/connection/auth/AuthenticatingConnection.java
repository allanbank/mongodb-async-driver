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
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * AuthenticatingConnection provides a connection that authenticated with the
 * server for each database before it is used.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticatingConnection extends AbstractProxyConnection {

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
            final MongoDbConfiguration config) {
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
    public void send(final Callback<Reply> reply, final Message... messages)
            throws MongoDbException {
        for (final Message message : messages) {
            ensureAuthenticated(message);
        }
        super.send(reply, messages);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Makes sure the connection is authenticated for the messages databases
     * before forwarding to the proxied connection.
     * </p>
     */
    @Override
    public void send(final Message... messages) throws MongoDbException {
        for (final Message message : messages) {
            ensureAuthenticated(message);
        }
        super.send(messages);
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
     * Creates an exception from the {@link Reply}.
     * 
     * @param reply
     *            The raw reply.
     * @return The exception created.
     */
    protected boolean isOk(final Reply reply) {
        final List<Document> results = reply.getResults();
        if (results.size() == 1) {
            final Document doc = results.get(0);
            final Element okElem = doc.get("ok");
            if (okElem != null) {
                final int okValue = toInt(okElem);
                if (okValue == 1) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Converts a {@link DoubleElement} or {@link IntegerElement} into an
     * <tt>int</tt> value. If not a {@link DoubleElement} of
     * {@link IntegerElement} then -1 is returned.
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
     */
    private void ensureAuthenticated(final Message message)
            throws MongoDbAuthenticationException {
        final String name;
        if (myConfig.isAdminUser()) {
            name = "admin";
        }
        else {
            name = message.getDatabaseName();
        }

        Boolean current = myAuthResponse.get(name);
        if (current == null) {
            try {
                DocumentBuilder builder = BuilderFactory.start();
                builder.addInteger("getnonce", 1);

                FutureCallback<Reply> replyCallback = new FutureCallback<Reply>();
                Future<Reply> alreadySent = myAuthTokens.putIfAbsent(name,
                        replyCallback);
                if (alreadySent == null) {
                    getProxiedConnection().send(replyCallback,
                            new Command(name, builder.build()));
                    alreadySent = replyCallback;
                }

                String nonce = "";
                Reply reply = alreadySent.get();
                if (reply.getResults().size() > 0) {
                    final Document doc = reply.getResults().get(0);
                    final List<StringElement> strElem = doc.queryPath(
                            StringElement.class, "nonce");
                    if (strElem.size() > 0) {
                        nonce = strElem.get(0).getValue();
                    }
                }

                final MessageDigest md5 = MessageDigest.getInstance("MD5");
                final byte[] bytes = md5
                        .digest((nonce + myConfig.getUsername() + myConfig
                                .getPasswordHash())
                                .getBytes(MongoDbConfiguration.UTF8));

                builder = BuilderFactory.start();
                builder.addInteger("authenticate", 1);
                builder.addString("user", myConfig.getUsername());
                builder.addString("nonce", nonce);
                builder.addString("key", MongoDbConfiguration.asHex(bytes));

                replyCallback = new FutureCallback<Reply>();
                alreadySent = myAuthReplys.putIfAbsent(name, replyCallback);
                if (alreadySent == null) {
                    getProxiedConnection().send(replyCallback,
                            new Command(name, builder.build()));
                    alreadySent = replyCallback;
                }

                reply = alreadySent.get();
                current = Boolean.valueOf(isOk(reply));
                myAuthResponse.put(name, current);
            }
            catch (final InterruptedException e) {
                throw new MongoDbAuthenticationException(e);
            }
            catch (final ExecutionException e) {
                throw new MongoDbAuthenticationException(e);
            }
            catch (final NoSuchAlgorithmException e) {
                throw new MongoDbAuthenticationException(e);
            }
        }

        if (!current.booleanValue()) {
            throw new MongoDbAuthenticationException("Authentication to the '"
                    + name + "' database failed.");
        }
    }
}
