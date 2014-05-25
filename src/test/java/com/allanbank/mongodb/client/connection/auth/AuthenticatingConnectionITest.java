/*
 * Copyright 2012-2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.connection.auth;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.After;
import org.junit.Test;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.ServerTestDriverSupport;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.client.ClusterType;
import com.allanbank.mongodb.client.callback.FutureReplyCallback;
import com.allanbank.mongodb.client.connection.Connection;
import com.allanbank.mongodb.client.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.client.message.Insert;
import com.allanbank.mongodb.client.message.Query;
import com.allanbank.mongodb.client.message.Reply;
import com.allanbank.mongodb.client.state.Cluster;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AuthenticatingConnectionITest provides tests of the authentication against a
 * live MongoDB process.
 * 
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
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

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addCredential(Credential.builder().userName(ADMIN_USER_NAME)
                .password(PASSWORD).mongodbCR().build());

        Connection socketConn = null;
        AuthenticatingConnection authConn = null;
        SocketConnectionFactory socketFactory = null;
        try {
            socketFactory = new SocketConnectionFactory(config);

            final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
            final Document doc = BuilderFactory.start()
                    .addObjectId("_id", new ObjectId()).build();

            socketConn = socketFactory.connect(
                    cluster.add(new InetSocketAddress("localhost", 27017)),
                    config);
            authConn = new AuthenticatingConnection(socketConn, config);

            final FutureReplyCallback reply = new FutureReplyCallback();
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
            IOUtils.close(authConn);
            IOUtils.close(socketConn);
            IOUtils.close(socketFactory);
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

        final MongoClientConfiguration config = new MongoClientConfiguration();
        config.addCredential(Credential.builder().userName(USER_NAME)
                .password(PASSWORD).database(USER_DB).mongodbCR().build());

        Connection socketConn = null;
        AuthenticatingConnection authConn = null;
        SocketConnectionFactory socketFactory = null;
        try {
            socketFactory = new SocketConnectionFactory(config);

            final Cluster cluster = new Cluster(config, ClusterType.STAND_ALONE);
            final Document doc = BuilderFactory.start()
                    .addObjectId("_id", new ObjectId()).build();

            socketConn = socketFactory.connect(
                    cluster.add(new InetSocketAddress("localhost", 27017)),
                    config);
            authConn = new AuthenticatingConnection(socketConn, config);

            final FutureReplyCallback reply = new FutureReplyCallback();
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
            IOUtils.close(authConn);
            IOUtils.close(socketConn);
            IOUtils.close(socketFactory);
        }
    }
}
