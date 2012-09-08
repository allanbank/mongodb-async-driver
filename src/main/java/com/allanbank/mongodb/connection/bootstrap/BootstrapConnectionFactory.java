/*
 * Copyright 2011-2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.bootstrap;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.ClusterType;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.ReconnectStrategy;
import com.allanbank.mongodb.connection.auth.AuthenticationConnectionFactory;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.ServerStatus;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.rs.ReplicaSetConnectionFactory;
import com.allanbank.mongodb.connection.sharded.ShardedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;
import com.allanbank.mongodb.connection.state.ServerState;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Provides the ability to bootstrap into the appropriate
 * {@link ConnectionFactory} based on the configuration of the server(s)
 * connected to.
 * 
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2011-2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class BootstrapConnectionFactory implements ConnectionFactory {

    /** The logger for the {@link BootstrapConnectionFactory}. */
    protected static final Logger LOG = Logger
            .getLogger(BootstrapConnectionFactory.class.getCanonicalName());

    /** The configuration for the connections to be created. */
    private final MongoDbConfiguration myConfig;

    /** The delegate connection factory post */
    private ConnectionFactory myDelegate = null;

    /**
     * Creates a {@link BootstrapConnectionFactory}
     * 
     * @param config
     *            The configuration to use in discovering the server
     *            configuration.
     */
    public BootstrapConnectionFactory(final MongoDbConfiguration config) {
        myConfig = config;
    }

    /**
     * Re-bootstraps the environment. Normally this method is only called once
     * during the constructor of the factory to initialize the delegate but
     * users can reset the delegate by manually invoking this method.
     * <p>
     * A bootstrap will issue one commands to the first working MongoDB process.
     * The reply to the {@link ServerStatus} command is used to detect
     * connecting to a mongos <tt>process</tt> and by extension a Sharded
     * configuration.
     * </p>
     * <p>
     * If not using a Sharded configuration then the server status is checked
     * for a <tt>repl</tt> element. If present a Replication Set configuration
     * is assumed.
     * </p>
     * <p>
     * If neither a Sharded or Replication Set is being used then a plain socket
     * connection factory is used.
     * </p>
     */
    public void bootstrap() {
        ProxiedConnectionFactory factory = new SocketConnectionFactory(myConfig);
        // Authentication has to be right on top of the physical
        // connection.
        if (myConfig.isAuthenticating()) {
            factory = new AuthenticationConnectionFactory(factory, myConfig);
        }
        try {
            for (final String addr : myConfig.getServers()) {
                Connection conn = null;
                final FutureCallback<Reply> future = new FutureCallback<Reply>();
                try {
                    conn = factory.connect(new ServerState(addr), myConfig);

                    conn.send(new ServerStatus(), future);
                    final Reply reply = future.get();

                    // Close the connection now that we have the reply.
                    IOUtils.close(conn);

                    final List<Document> results = reply.getResults();
                    if (!results.isEmpty()) {
                        final Document doc = results.get(0);

                        if (isMongos(doc)) {
                            LOG.info("Sharded bootstrap to " + addr + ".");
                            myDelegate = new ShardedConnectionFactory(factory,
                                    myConfig);
                        }
                        else if (isReplicationSet(doc)) {
                            LOG.info("Replica-set bootstrap to " + addr + ".");
                            myDelegate = new ReplicaSetConnectionFactory(
                                    factory, myConfig);
                        }
                        else {
                            LOG.info("Simple MongoDB bootstrap to " + addr
                                    + ".");
                            myDelegate = factory;
                        }
                        factory = null; // Don't close.
                        return;
                    }
                }
                catch (final IOException ioe) {
                    LOG.log(Level.WARNING, "I/O error during bootstrap to "
                            + addr + ".", ioe);
                }
                catch (final InterruptedException e) {
                    LOG.log(Level.WARNING, "Interrupted during bootstrap to "
                            + addr + ".", e);
                }
                catch (final ExecutionException e) {
                    LOG.log(Level.WARNING, "Error during bootstrap to " + addr
                            + ".", e);
                }
                finally {
                    IOUtils.close(conn, Level.WARNING,
                            "I/O error shutting down bootstrap connection to "
                                    + addr + ".");
                }
            }
        }
        finally {
            IOUtils.close(factory);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to close the delegate {@link ConnectionFactory}.
     * </p>
     */
    @Override
    public void close() {
        IOUtils.close(myDelegate);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Delegates the connection to the setup delegate.
     * </p>
     */
    @Override
    public Connection connect() throws IOException {
        return getDelegate().connect();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the cluster type of the delegate
     * {@link ConnectionFactory}.
     * </p>
     */
    @Override
    public ClusterType getClusterType() {
        return getDelegate().getClusterType();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the delegates strategy.
     * </p>
     */
    @Override
    public ReconnectStrategy getReconnectStrategy() {
        return getDelegate().getReconnectStrategy();
    }

    /**
     * Returns the underlying delegate factory.
     * 
     * @return The underlying delegate factory.
     */
    protected ConnectionFactory getDelegate() {
        if (myDelegate == null) {
            bootstrap();
            if (myDelegate == null) {
                LOG.log(Level.WARNING,
                        "Could not bootstrap connection factory.");
            }
        }
        return myDelegate;
    }

    /**
     * Sets the underlying delegate factory.
     * 
     * @param delegate
     *            The underlying delegate factory.
     */
    protected void setDelegate(final ConnectionFactory delegate) {
        myDelegate = delegate;
    }

    /**
     * Returns true if the document contains a "process" element that is a
     * string and contains the value "mongos".
     * 
     * @param doc
     *            The document to validate.
     * @return True if the document contains a "process" element that is a
     *         string and contains the value "mongos".
     */
    private boolean isMongos(final Document doc) {

        final Element processName = doc.get("process");
        if (processName instanceof StringElement) {
            return "mongos".equals(((StringElement) processName).getValue());
        }

        return false;
    }

    /**
     * Returns true if the document contains a "repl" element that is a
     * sub-document.
     * 
     * @param doc
     *            The document to validate.
     * @return True if the document contains a "repl" element that is a
     *         sub-document.
     */
    private boolean isReplicationSet(final Document doc) {
        return (doc.get("repl") instanceof DocumentElement);
    }
}
