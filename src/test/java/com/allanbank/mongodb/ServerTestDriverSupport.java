/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.net.InetSocketAddress;

import org.junit.AfterClass;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AbstractServerTestDriver provides common methods for starting and stopping
 * MongoDB processes in common configurations.
 * 
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerTestDriverSupport {

    /** A test admin user name. */
    public static final String ADMIN_USER_NAME = "admin";

    /** The default MongoDB port. */
    public static final int DEFAULT_PORT = 27017;

    /** A test password. You really shouldn't use this as a password... */
    public static final String PASSWORD = "password";

    /** A test admin user name. */
    public static final String USER_DB = "db";

    /** A test user name. */
    public static final String USER_NAME = "user";

    /** Support for spinning up clusters. */
    private static final ClusterTestSupport myClusterTestSupport = new ClusterTestSupport();

    /**
     * Stops the servers running in a replica set mode.
     */
    @AfterClass
    public static void stopReplicaSet() {
        myClusterTestSupport.stopAll();
    }

    /**
     * Stops the servers running in a sharded mode.
     */
    @AfterClass
    public static void stopSharded() {
        myClusterTestSupport.stopAll();
    }

    /**
     * Stops the servers running in a sharded cluster of replica sets.
     */
    @AfterClass
    public static void stopShardedReplicaSets() {
        myClusterTestSupport.stopAll();
    }

    /**
     * Stops the servers running in a standalone mode.
     */
    @AfterClass
    public static void stopStandAlone() {
        myClusterTestSupport.stopAll();
    }

    /**
     * Starts a MongoDB instance running in a standalone mode.
     */
    protected static void startAuthenticated() {
        MongoClient mongo = null;
        try {
            startStandAlone();

            // Use the config to compute the hash.
            final MongoClientConfiguration adminConfig = new MongoClientConfiguration();
            adminConfig.authenticateAsAdmin(ADMIN_USER_NAME, PASSWORD);
            adminConfig.addServer(new InetSocketAddress("127.0.0.1", 27017));

            final MongoClientConfiguration config = new MongoClientConfiguration();
            config.addServer(new InetSocketAddress("127.0.0.1", 27017));

            mongo = MongoFactory.createClient(config);
            MongoDatabase db = mongo.getDatabase("admin");
            MongoCollection collection = db.getCollection("system.users");

            DocumentBuilder docBuilder = BuilderFactory.start();
            docBuilder.addString("user", ADMIN_USER_NAME);
            docBuilder.addString("pwd", adminConfig.getPasswordHash());
            docBuilder.addBoolean("readOnly", false);
            try {
                collection.insert(Durability.ACK, docBuilder.build());
            }
            catch (final MongoDbException error) {
                // Check if it is just MongoDB telling us authentication is now
                // active.
                if (!error.getMessage().equals("need to login")) {
                    throw error;
                }
            }
            finally {
                // Shutdown the connection.
                IOUtils.close(mongo);
            }

            // Start a new connection authenticating as the admin user to add
            // the non-admin user...
            mongo = MongoFactory.createClient(adminConfig);
            db = mongo.getDatabase(USER_DB);
            collection = db.getCollection("system.users");

            // Again - Config does the hash for us.
            config.authenticate(USER_NAME, PASSWORD);

            docBuilder = BuilderFactory.start();
            docBuilder.addString("user", USER_NAME);
            docBuilder.addString("pwd", config.getPasswordHash());
            docBuilder.addBoolean("readOnly", false);

            collection.insert(Durability.ACK, docBuilder.build());
        }
        finally {
            IOUtils.close(mongo);
        }
    }

    /**
     * Starts a MongoDB instance running in a replica set mode.
     * 
     * @see ClusterTestSupport#startReplicaSet
     */
    protected static void startReplicaSet() {
        myClusterTestSupport.startReplicaSet();
    }

    /**
     * Starts a MongoDB instance running in a sharded mode.
     * 
     * @see ClusterTestSupport#startSharded
     */
    protected static void startSharded() {
        myClusterTestSupport.startSharded();
    }

    /**
     * Starts a MongoDB instance running in a sharded cluster of replica sets.
     * 
     * @see ClusterTestSupport#startShardedReplicaSets
     */
    protected static void startShardedReplicaSets() {
        myClusterTestSupport.startShardedReplicaSets();
    }

    /**
     * Starts a MongoDB instance running in a standalone mode.
     * 
     * @see ClusterTestSupport#startStandAlone
     */
    protected static void startStandAlone() {
        myClusterTestSupport.startStandAlone();
    }
}
