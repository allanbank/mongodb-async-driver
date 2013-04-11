/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.sharded;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.ReadPreference;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.AdminCommand;
import com.allanbank.mongodb.connection.message.Command;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterPinger;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.LatencyServerSelector;
import com.allanbank.mongodb.connection.state.ServerSelector;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Provides the ability to create connections to a shard configuration via
 * mongos servers.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnectionFactory implements ConnectionFactory {

    /** The logger for the {@link ShardedConnectionFactory}. */
    protected static final Logger LOG = Logger
            .getLogger(ShardedConnectionFactory.class.getCanonicalName());

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The state of the cluster. */
    private final ShardedClusterState myClusterState;

    /** The MongoDB client configuration. */
    private final MongoClientConfiguration myConfig;

    /** Pings the servers in the cluster collecting latency and tags. */
    private final ClusterPinger myPinger;

    /** The slector for the mongos instance to use. */
    private final ServerSelector mySelector;

    /**
     * Creates a new {@link ShardedConnectionFactory}.
     * 
     * @param factory
     *            The factory to create proxied connections.
     * @param config
     *            The initial configuration.
     */
    public ShardedConnectionFactory(final ProxiedConnectionFactory factory,
            final MongoClientConfiguration config) {
        myConnectionFactory = factory;
        myConfig = config;
        myClusterState = new ShardedClusterState(config);
        mySelector = new LatencyServerSelector(myClusterState, true);
        myPinger = new ClusterPinger(myClusterState, ClusterType.SHARDED,
                factory, config);

        for (final String address : config.getServers()) {
            final ServerState state = myClusterState.add(address);

            // In a sharded environment we assume that all of the mongos servers
            // are writable.
            myClusterState.markWritable(state);
        }

        bootstrap();
    }

    /**
     * Bootstrap into the cluster. This is two functions. The first locates the
     * configuration servers based on the command line for the mongs servers.
     * The second locates all of the mongos servers based on the config
     * database.
     * <p>
     * Lastly the bootstrap starts the cluster pinger to monitor the cluster's
     * state.
     * </p>
     */
    public void bootstrap() {
        boolean doneConfig = false;
        boolean doneMongos = !myConfig.isAutoDiscoverServers();
        for (final String addr : myConfig.getServers()) {
            Connection conn = null;
            try {
                // Send the request...
                conn = myConnectionFactory.connect(myClusterState.add(addr),
                        myConfig);
                doneConfig = findConfigServers(conn);
                if (!doneMongos) {
                    doneMongos = findMongosServers(conn);
                }

                if (doneConfig && doneMongos) {
                    break;
                }
            }
            catch (final IOException ioe) {
                LOG.log(Level.WARNING, "I/O error during sharded bootstrap to "
                        + addr + ".", ioe);
            }
            catch (final MongoDbException me) {
                LOG.log(Level.WARNING,
                        "MongoDB error during sharded bootstrap to " + addr
                                + ".", me);
            }
            catch (final InterruptedException e) {
                LOG.log(Level.WARNING,
                        "Interrupted during sharded bootstrap to " + addr + ".",
                        e);
            }
            catch (final ExecutionException e) {
                LOG.log(Level.WARNING, "Error during sharded bootstrap to "
                        + addr + ".", e);
            }
            finally {
                IOUtils.close(conn, Level.WARNING,
                        "I/O error shutting down sharded bootstrap connection to "
                                + addr + ".");
            }
        }

        // Last thing is to start the ping of servers. This will get the tags
        // and latencies updated.
        myPinger.initialSweep();
        myPinger.start();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the cluster state and the
     * {@link ProxiedConnectionFactory}.
     * </p>
     */
    @Override
    public void close() {
        IOUtils.close(myPinger);
        IOUtils.close(myClusterState);
        IOUtils.close(myConnectionFactory);
    }

    /**
     * Creates a new connection to the shared mongos servers.
     * 
     * @see ConnectionFactory#connect()
     */
    @Override
    public Connection connect() throws IOException {
        IOException lastError = null;
        for (final ServerState primary : myClusterState.getWritableServers()) {
            try {
                final Connection primaryConn = myConnectionFactory.connect(
                        primary, myConfig);

                return new ShardedConnection(primaryConn, myConfig);
            }
            catch (final IOException e) {
                lastError = e;
            }
        }

        if (lastError != null) {
            throw lastError;
        }

        throw new IOException(
                "Could not determine a shard server to connect to.");
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return {@link ClusterType#SHARDED} cluster type.
     * </p>
     */
    @Override
    public ClusterType getClusterType() {
        return ClusterType.SHARDED;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the delegates strategy but replace his state and
     * selector with our own.
     * </p>
     */
    @Override
    public ReconnectStrategy getReconnectStrategy() {
        final ReconnectStrategy delegates = myConnectionFactory
                .getReconnectStrategy();

        delegates.setState(myClusterState);
        delegates.setSelector(mySelector);
        delegates.setConnectionFactory(myConnectionFactory);

        return delegates;
    }

    /**
     * Performs a {@code getCommandLineOpts} command to the mongos to locate the
     * config db servers.
     * <p>
     * The command looks like: <blockquote>
     * 
     * <pre>
     * <code>
     * { 'getCmdLineOpts' : 1 }
     * </code>
     * </pre>
     * 
     * </blockquote> It returns a document like:<blockquote>
     * 
     * <pre>
     * <code>
     * {
     *     'argv' : { 
     *        // ... 
     *     }
     *     'parsed' : {
     *         // ...
     *         'configdb' : "&lt;config db location&gt;",
     *         // ...
     *     }
     *     'ok' : 1
     * }
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param conn
     *            The connection to request from.
     * @return True if the configuration servers have been determined.
     * @throws ExecutionException
     *             On a failure to recover the response from the server.
     * @throws InterruptedException
     *             On a failure to receive a response from the server.
     */
    protected boolean findConfigServers(final Connection conn)
            throws InterruptedException, ExecutionException {

        boolean found = (myClusterState.getConfigDatabases() == null);
        if (found) {
            // Determine the location of the configuration database servers.
            final Command getCmdLineOpts = new AdminCommand(BuilderFactory
                    .start().add("getCmdLineOpts", 1).build());

            final FutureCallback<Reply> future = new FutureCallback<Reply>();
            conn.send(getCmdLineOpts, future);

            // Receive the response.
            final Reply reply = future.get();

            // Validate and pull out the response information.
            final List<Document> docs = reply.getResults();
            for (final Document doc : docs) {
                final StringElement idElem = doc.findFirst(StringElement.class,
                        "parsed", "configdb");
                if (idElem != null) {
                    myClusterState.setConfigDatabases(idElem.getValue());
                    LOG.fine("Located Config Server URL: " + idElem.getValue());
                    found = true;
                }
            }
        }
        return found;
    }

    /**
     * Performs a find on the <tt>config</tt> database's <tt>mongos</tt>
     * collection to return the id for all of the mongos servers in the cluster.
     * <p>
     * A single mongos entry looks like: <blockquote>
     * 
     * <pre>
     * <code>
     * { 
     *     "_id" : "mongos.example.com:27017", 
     *     "ping" : ISODate("2011-12-05T23:54:03.122Z"), 
     *     "up" : 330 
     * }
     * </code>
     * </pre>
     * 
     * </blockquote>
     * 
     * @param conn
     *            The connection to request from.
     * @return True if the configuration servers have been determined.
     * @throws ExecutionException
     *             On a failure to recover the response from the server.
     * @throws InterruptedException
     *             On a failure to receive a response from the server.
     */
    protected boolean findMongosServers(final Connection conn)
            throws InterruptedException, ExecutionException {
        boolean found = false;

        // Create a query to pull all of the mongos servers out of the
        // config database.
        final Query query = new Query("config", "mongos", BuilderFactory
                .start().build(), /* fields= */null, /* batchSize= */0,
        /* limit= */0, /* numberToSkip= */0, /* tailable= */false,
                ReadPreference.PRIMARY, /* noCursorTimeout= */false,
                /* awaitData= */false, /* exhaust= */false, /* partial= */
                false);

        // Send the request...
        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        conn.send(query, future);

        // Receive the response.
        final Reply reply = future.get();

        // Validate and pull out the response information.
        final List<Document> docs = reply.getResults();
        for (final Document doc : docs) {
            final Element idElem = doc.get("_id");
            if (idElem instanceof StringElement) {
                final StringElement id = (StringElement) idElem;

                myClusterState.markWritable(myClusterState.add(id.getValue()));
                LOG.fine("Adding shard mongos: " + id.getValue());
                found = true;
            }
        }

        return found;
    }

    /**
     * Returns the clusterState value.
     * 
     * @return The clusterState value.
     */
    protected ClusterState getClusterState() {
        return myClusterState;
    }
}
