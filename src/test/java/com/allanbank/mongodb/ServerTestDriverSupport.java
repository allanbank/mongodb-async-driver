/*
 * #%L
 * ServerTestDriverSupport.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

package com.allanbank.mongodb;

import static org.junit.Assert.assertNull;

import java.io.File;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;
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
    public static final String USER_DB = "user_db";

    /** A test user name. */
    public static final String USER_NAME = "user";

    private static File latestWorkingDirectory = null;

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

    public static void stopAllWithoutDeleteDirectories() {
        ourClusterTestSupport.stopAllWithoutDeleteDirectories();
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
            db.getCollection("system.version").insert(BuilderFactory.start().add("_id","authSchema").add("currentVersion" , 3));

            stopAllWithoutDeleteDirectories();

            System.out.println("===> restart mongo server.");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            startStandAloneLatestWorkingDirectory();

            MongoIterator<Document> documents = db.getCollection("system.version").find(BuilderFactory.start());
            MongoCollection collection = db.getCollection("system.users");

            DocumentBuilder docBuilder = BuilderFactory.start();
            docBuilder.add("_id", "admin." + ADMIN_USER_NAME);
            docBuilder.addString("user", ADMIN_USER_NAME);
            docBuilder.addString("db", "admin");
            docBuilder.add("credentials", BuilderFactory.start().add("MONGODB-CR", adminHash).build());
            DocumentBuilder[] roles = { BuilderFactory.start().add("role", "readWrite").add("db", "admin") };
            docBuilder.add("roles", roles);
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
            db = mongo.getDatabase("admin");
            collection = db.getCollection("system.users");

            // Again - Authenticator does the hash for us.
            final Credential userCredentials = Credential.builder()
                    .userName(USER_NAME).password(PASSWORD).database(USER_DB)
                    .authenticationType(Credential.MONGODB_CR).build();

            //////
            docBuilder = BuilderFactory.start();
            docBuilder.add("_id", USER_DB + "." + USER_NAME);
            docBuilder.addString("user", USER_NAME);
            docBuilder.addString("db", USER_DB);
            docBuilder.add("credentials", BuilderFactory.start().add("MONGODB-CR", authenticator.passwordHash(userCredentials)).build());
            DocumentBuilder[] roles1 = { BuilderFactory.start().add("role", "readWrite").add("db", USER_DB) };
            docBuilder.add("roles", roles1);
            /////


//            docBuilder = BuilderFactory.start();
//            docBuilder.addString("user", USER_NAME);
//            docBuilder.addString("pwd", authenticator.passwordHash(userCredentials));
//            docBuilder.add("roles", roles);
//            docBuilder.addBoolean("readOnly", false);

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
        latestWorkingDirectory = ourClusterTestSupport.getWorkingDirectory();
    }

    /**
     * Starts a MongoDB instance running in a standalone mode.
     *
     * @see ClusterTestSupport#startStandAlone
     */
    protected static void startStandAloneLatestWorkingDirectory() {
        ourClusterTestSupport.startStandAloneWithWD(latestWorkingDirectory);
    }
}
