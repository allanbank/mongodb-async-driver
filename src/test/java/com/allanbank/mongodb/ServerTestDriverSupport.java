/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.AfterClass;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

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

    /** Script for starting/stopping the MongoDB in a replica set mode. */
    public static final File REPLICA_SET_SCRIPT;

    /** Script for starting/stopping the MongoDB in a sharded mode. */
    public static final File SHARDED_SCRIPT;

    /** Script for starting/stopping the MongoDB in a standalone mode. */
    public static final File STAND_ALONE_SCRIPT;

    /** A test admin user name. */
    public static final String USER_DB = "db";

    /** A test user name. */
    public static final String USER_NAME = "user";

    /** A builder for executing background process scripts. */
    protected static ProcessBuilder ourBuilder = null;

    /** The directory containing the scripts. */
    protected static final File SCRIPT_DIR;

    static {
        SCRIPT_DIR = new File("src/test/scripts");
        REPLICA_SET_SCRIPT = new File(SCRIPT_DIR, "replica_set.sh");
        SHARDED_SCRIPT = new File(SCRIPT_DIR, "sharded.sh");
        STAND_ALONE_SCRIPT = new File(SCRIPT_DIR, "standalone.sh");
    }

    /**
     * Creates a builder for executing background process scripts.
     * 
     * @return The {@link ProcessBuilder}.
     */
    public static ProcessBuilder createBuilder() {
        if (ourBuilder == null) {
            ourBuilder = new ProcessBuilder();
            ourBuilder.directory(SCRIPT_DIR);
        }
        return ourBuilder;
    }

    /**
     * Stops the servers running in a replica set mode.
     */
    @AfterClass
    public static void stopReplicaSet() {
        Process stop = null;
        try {
            final ProcessBuilder builder = createBuilder();
            builder.command(REPLICA_SET_SCRIPT.getAbsolutePath(), "stop");
            stop = builder.start();
            stop.waitFor();
        }
        catch (final IOException ioe) {
            // Ignore - best effort.
        }
        catch (final InterruptedException e) {
            // Ignore - best effort.
        }
    }

    /**
     * Stops the servers running in a sharded mode.
     */
    @AfterClass
    public static void stopSharded() {
        Process stop = null;
        try {
            final ProcessBuilder builder = createBuilder();
            builder.command(SHARDED_SCRIPT.getAbsolutePath(), "stop");
            stop = builder.start();
            stop.waitFor();
        }
        catch (final IOException ioe) {
            // Ignore - best effort.
        }
        catch (final InterruptedException e) {
            // Ignore - best effort.
        }
    }

    /**
     * Stops the servers running in a standalone mode.
     */
    @AfterClass
    public static void stopStandAlone() {
        Process stop = null;
        try {
            final ProcessBuilder builder = createBuilder();
            builder.command(STAND_ALONE_SCRIPT.getAbsolutePath(), "stop");
            stop = builder.start();
            stop.waitFor();
        }
        catch (final IOException ioe) {
            // Ignore - best effort.
        }
        catch (final InterruptedException e) {
            // Ignore - best effort.
        }
    }

    /**
     * Closes the connection squashing and exception.
     * 
     * @param conn
     *            The connection to close.
     */
    protected static void close(final Closeable conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        }
        catch (final IOException ignore) {
            // Ignored - best effort.
        }
    }

    /**
     * Starts a MongoDB instance running in a standalone mode.
     */
    protected static void startAuthenticated() {
        Mongo mongo = null;
        try {
            final ProcessBuilder builder = createBuilder();
            builder.command(STAND_ALONE_SCRIPT.getAbsolutePath(), "start",
                    "--auth");
            final Process start = builder.start();
            start.waitFor();

            // Use the config to compute the hash.
            final MongoDbConfiguration adminConfig = new MongoDbConfiguration();
            adminConfig.authenticateAsAdmin(ADMIN_USER_NAME, PASSWORD);
            adminConfig.addServer(new InetSocketAddress("127.0.0.1", 27017));

            final MongoDbConfiguration config = new MongoDbConfiguration();
            config.addServer(new InetSocketAddress("127.0.0.1", 27017));

            mongo = MongoFactory.create(config);
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
                close(mongo);
            }

            // Start a new connection authenticating as the admin user to add
            // the non-admin user...
            mongo = MongoFactory.create(adminConfig);
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
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
        finally {
            close(mongo);
        }
    }

    /**
     * Starts a MongoDB instance running in a replica set mode.
     */
    protected static void startReplicaSet() {
        try {
            final ProcessBuilder builder = createBuilder();
            builder.command(REPLICA_SET_SCRIPT.getAbsolutePath(), "start");
            final Process start = builder.start();
            start.waitFor();
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Starts a MongoDB instance running in a sharded mode.
     */
    protected static void startSharded() {
        try {
            final ProcessBuilder builder = createBuilder();
            builder.command(SHARDED_SCRIPT.getAbsolutePath(), "start");
            final Process start = builder.start();
            start.waitFor();
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Starts a MongoDB instance running in a standalone mode.
     */
    protected static void startStandAlone() {
        try {
            final ProcessBuilder builder = createBuilder();
            builder.command(STAND_ALONE_SCRIPT.getAbsolutePath(), "start");
            final Process start = builder.start();
            start.waitFor();
        }
        catch (final IOException ioe) {
            final AssertionError error = new AssertionError(ioe.getMessage());
            error.initCause(ioe);
            throw error;
        }
        catch (final InterruptedException e) {
            final AssertionError error = new AssertionError(e.getMessage());
            error.initCause(e);
            throw error;
        }
    }
}
