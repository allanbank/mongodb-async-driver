/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import static org.junit.Assert.assertNull;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;

import org.junit.AfterClass;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.client.connection.auth.MongoDbAuthenticator;
import com.allanbank.mongodb.util.IOUtils;

/**
 * AbstractServerTestDriver provides common methods for starting and stopping
 * MongoDB processes in common configurations.
 * 
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ServerTestDriverSupport {

    /** A test admin user name. */
    public static final String ADMIN_USER_NAME = "admin_user";

    /** The default MongoDB port. */
    public static final int DEFAULT_PORT = 27017;

    /** A test password. You really shouldn't use this as a password... */
    public static final char[] PASSWORD = "password".toCharArray();

    /** A test admin user name. */
    public static final String USER_DB = "db";

    /** A test user name. */
    public static final String USER_NAME = "user";

    /** Support for spinning up clusters. */
    protected static final ClusterTestSupport ourClusterTestSupport = new ClusterTestSupport();

    /**
     * Stops the servers running in a replica set mode.
     */
    @AfterClass
    public static void stopReplicaSet() {
        ourClusterTestSupport.stopAll();
    }

    /**
     * Stops the servers running in a sharded mode.
     */
    @AfterClass
    public static void stopSharded() {
        ourClusterTestSupport.stopAll();
    }

    /**
     * Stops the servers running in a sharded cluster of replica sets.
     */
    @AfterClass
    public static void stopShardedReplicaSets() {
        ourClusterTestSupport.stopAll();
    }

    /**
     * Stops the servers running in a standalone mode.
     */
    @AfterClass
    public static void stopStandAlone() {
        ourClusterTestSupport.stopAll();
    }

    /**
     * Repairs a MongoDB instance running in a replica set mode. This verifies
     * all of the members are running and that there is a primary.
     * 
     * @see ClusterTestSupport#startReplicaSet
     */
    protected static void repairReplicaSet() {
        ourClusterTestSupport.repairReplicaSet();
    }

    /**
     * Starts a MongoDB instance running in a standalone mode.
     */
    protected static void startAuthenticated() {
        MongoClient mongo = null;
        try {
            startStandAlone();

            // Use the authenticator to compute the hash.
            final MongoDbAuthenticator authenticator = new MongoDbAuthenticator();

            final Credential adminCredentials = Credential.builder()
                    .userName(ADMIN_USER_NAME).password(PASSWORD)
                    .database(Credential.ADMIN_DB)
                    .authenticationType(Credential.MONGODB_CR).build();
            final String adminHash = authenticator
                    .passwordHash(adminCredentials);

            final MongoClientConfiguration config = new MongoClientConfiguration();
            config.addServer(new InetSocketAddress("127.0.0.1", 27017));

            mongo = MongoFactory.createClient(config);
            MongoDatabase db = mongo.getDatabase("admin");
            MongoCollection collection = db.getCollection("system.users");

            DocumentBuilder docBuilder = BuilderFactory.start();
            docBuilder.addString("user", ADMIN_USER_NAME);
            docBuilder.addString("pwd", adminHash);
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
            final MongoClientConfiguration adminConfig = new MongoClientConfiguration();
            adminConfig.addCredential(adminCredentials);
            adminConfig.addServer(new InetSocketAddress("127.0.0.1", 27017));

            mongo = MongoFactory.createClient(adminConfig);
            db = mongo.getDatabase(USER_DB);
            collection = db.getCollection("system.users");

            // Again - Authenticator does the hash for us.
            final Credential userCredentials = Credential.builder()
                    .userName(USER_NAME).password(PASSWORD).database("any")
                    .authenticationType(Credential.MONGODB_CR).build();

            docBuilder = BuilderFactory.start();
            docBuilder.addString("user", USER_NAME);
            docBuilder.addString("pwd",
                    authenticator.passwordHash(userCredentials));
            docBuilder.addBoolean("readOnly", false);

            collection.insert(Durability.ACK, docBuilder.build());
        }
        catch (final IllegalArgumentException e) {
            assertNull(e);
        }
        catch (final NoSuchAlgorithmException e) {
            assertNull(e);
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
        ourClusterTestSupport.startReplicaSet();
    }

    /**
     * Starts a MongoDB instance running in a sharded mode.
     * 
     * @see ClusterTestSupport#startSharded
     */
    protected static void startSharded() {
        ourClusterTestSupport.startSharded();
    }

    /**
     * Starts a MongoDB instance running in a sharded cluster of replica sets.
     * 
     * @see ClusterTestSupport#startShardedReplicaSets
     */
    protected static void startShardedReplicaSets() {
        ourClusterTestSupport.startShardedReplicaSets();
    }

    /**
     * Starts a MongoDB instance running in a standalone mode.
     * 
     * @see ClusterTestSupport#startStandAlone
     */
    protected static void startStandAlone() {
        ourClusterTestSupport.startStandAlone();
    }
}
