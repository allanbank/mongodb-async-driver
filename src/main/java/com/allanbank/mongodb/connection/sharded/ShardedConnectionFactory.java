/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.sharded;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.message.Query;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnection;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.LatencyServerSelector;
import com.allanbank.mongodb.connection.state.ServerSelector;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * Provides the ability to create connections to a shard configuration via
 * mongos servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnectionFactory implements ConnectionFactory {

    /** The logger for the {@link ShardedConnectionFactory}. */
    protected static final Logger LOG = Logger
            .getLogger(ShardedConnectionFactory.class.getCanonicalName());

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The state of the cluster. */
    private final ClusterState myClusterState;

    /** The MongoDB client configuration. */
    private final MongoDbConfiguration myConfig;

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
            final MongoDbConfiguration config) {
        myConnectionFactory = factory;
        myConfig = config;
        myClusterState = new ClusterState();
        mySelector = new LatencyServerSelector(myClusterState, true);
        for (final InetSocketAddress address : config.getServers()) {
            final ServerState state = myClusterState.add(address.getAddress()
                    .getHostName() + ":" + address.getPort());

            // In a sharded environment we assume that all of the mongos servers
            // are writable.
            myClusterState.markWritable(state);
        }

        bootstrap();
    }

    /**
     * Finds the mongos servers.
     * <p>
     * Performs a find on the <tt>config</tt> database's <tt>mongos</tt>
     * collection to return the id for all of the mongos servers in the cluster.
     * </p>
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
     */
    public void bootstrap() {
        if (myConfig.isAutoDiscoverServers()) {
            // Create a query to pull all of the mongos servers out of the
            // config database.
            final Query query = new Query("config", "mongos", BuilderFactory
                    .start().build(), /* fields= */null, /* batchSize= */0,
            /* limit= */0, /* numberToSkip= */0, /* tailable= */false,
            /* replicaOk= */false, /* noCursorTimeout= */false,
            /* awaitData= */false, /* exhaust= */false, /* partial= */false);

            for (final InetSocketAddress addr : myConfig.getServers()) {
                SocketConnection conn = null;
                final FutureCallback<Reply> future = new FutureCallback<Reply>();
                try {
                    // Send the request...
                    conn = new SocketConnection(addr, myConfig);
                    conn.send(future, query);

                    // Receive the response.
                    final Reply reply = future.get();

                    // Validate and pull out the response information.
                    final List<Document> docs = reply.getResults();
                    for (final Document doc : docs) {
                        final Element idElem = doc.get("_id");
                        if (idElem instanceof StringElement) {
                            final StringElement id = (StringElement) idElem;

                            myClusterState.markWritable(myClusterState.add(id
                                    .getValue()));
                            LOG.fine("Adding shard mongos: " + id.getValue());
                        }
                    }
                }
                catch (final IOException ioe) {
                    LOG.log(Level.WARNING,
                            "I/O error during sharded bootstrap to " + addr
                                    + ".", ioe);
                }
                catch (final InterruptedException e) {
                    LOG.log(Level.WARNING,
                            "Interrupted during sharded bootstrap to " + addr
                                    + ".", e);
                }
                catch (final ExecutionException e) {
                    LOG.log(Level.WARNING, "Error during sharded bootstrap to "
                            + addr + ".", e);
                }
                finally {
                    try {
                        if (conn != null) {
                            conn.close();
                        }
                    }
                    catch (final IOException okay) {
                        LOG.log(Level.WARNING,
                                "I/O error shutting down sharded bootstrap connection to "
                                        + addr + ".", okay);
                    }
                }
            }
        }
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
                        primary.getServer(), myConfig);

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
     * Overridden to return the delegates strategy but replace his state and
     * selector with our own.
     * </p>
     */
    @Override
    public ReconnectStrategy<? extends Connection> getReconnectStrategy() {
        final ReconnectStrategy<? extends Connection> delegates = myConnectionFactory
                .getReconnectStrategy();

        delegates.setState(myClusterState);
        delegates.setSelector(mySelector);

        return delegates;
    }
}
