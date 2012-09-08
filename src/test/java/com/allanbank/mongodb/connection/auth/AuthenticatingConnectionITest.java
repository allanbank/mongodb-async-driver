/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.auth;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.Insert;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * AuthenticatingConnectionITest provides tests of the authentication against a
 * live MongoDB process.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class AuthenticatingConnectionITest extends ServerTestDriverSupport {

    /**
     * Stop the server we started.
     */
    @After
    public void tearDown() {
        stopStandAlone();
    }

    /**
     * Test method to insert a document into the database and then query it back
     * out.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testInsertQueryAdminAuthenticated() throws Exception {
        startAuthenticated();

        final MongoDbConfiguration config = new MongoDbConfiguration();
        config.authenticateAsAdmin(ADMIN_USER_NAME, PASSWORD);
        config.setDefaultDatabase(USER_DB);

        Connection socketConn = null;
        AuthenticatingConnection authConn = null;
        SocketConnectionFactory socketFactory = null;
        try {
            socketFactory = new SocketConnectionFactory(config);

            final Document doc = BuilderFactory.start()
                    .addObjectId("_id", new ObjectId()).build();

            socketConn = socketFactory.connect(new ServerState(
                    "127.0.0.1:27017"), config);
            authConn = new AuthenticatingConnection(socketConn, config);

            final FutureCallback<Reply> reply = new FutureCallback<Reply>();
            authConn.send(
                    new Insert(USER_DB, "bar", Collections.singletonList(doc),
                            false),
                    new Query(USER_DB, "bar", BuilderFactory.start().build(),
                            null, 1, 1, 0, false, ReadPreference.PRIMARY,
                            false, false, false, false), reply);
            final Reply r = reply.get();

            assertEquals(1, r.getResults().size());
            assertEquals(doc, r.getResults().get(0));
        }
        finally {
            close(authConn);
            close(socketConn);
            close(socketFactory);
        }
    }

    /**
     * Test method to insert a document into the database and then query it back
     * out.
     * 
     * @throws Exception
     *             On a test failure.
     */
    @Test
    public void testInsertQueryNonAdminAuthenticated() throws Exception {
        startAuthenticated();

        final MongoDbConfiguration config = new MongoDbConfiguration();
        config.authenticate(USER_NAME, PASSWORD);
        config.setDefaultDatabase(USER_DB);

        Connection socketConn = null;
        AuthenticatingConnection authConn = null;
        SocketConnectionFactory socketFactory = null;
        try {
            socketFactory = new SocketConnectionFactory(config);

            final Document doc = BuilderFactory.start()
                    .addObjectId("_id", new ObjectId()).build();

            socketConn = socketFactory.connect(new ServerState(
                    "127.0.0.1:27017"), config);
            authConn = new AuthenticatingConnection(socketConn, config);

            final FutureCallback<Reply> reply = new FutureCallback<Reply>();
            authConn.send(
                    new Insert(USER_DB, "bar", Collections.singletonList(doc),
                            false),
                    new Query(USER_DB, "bar", BuilderFactory.start().build(),
                            null, 1, 1, 0, false, ReadPreference.PRIMARY,
                            false, false, false, false), reply);
            final Reply r = reply.get();

            assertEquals(1, r.getResults().size());
            assertEquals(doc, r.getResults().get(0));
        }
        finally {
            close(authConn);
            close(socketConn);
            close(socketFactory);
        }
    }
}
