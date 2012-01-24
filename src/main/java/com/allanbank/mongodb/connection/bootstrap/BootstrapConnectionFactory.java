/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.bootstrap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.messsage.Reply;
import com.allanbank.mongodb.connection.messsage.ServerStatus;
import com.allanbank.mongodb.connection.rs.ReplicaSetConnectionFactory;
import com.allanbank.mongodb.connection.sharded.ShardedConnectionFactory;
import com.allanbank.mongodb.connection.socket.SocketConnection;
import com.allanbank.mongodb.connection.socket.SocketConnectionFactory;

/**
 * Provides the ability to bootstrap into the appropriate
 * {@link ConnectionFactory} based on the configuration of the server(s)
 * connected to.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
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
     * 
     * @param config
     *            The configuration to use in discovering the server
     *            configuration.
     * 
     * @throws IOException
     *             If the bootstrap fails.
     */
    public void bootstrap() throws IOException {
        for (final InetSocketAddress addr : myConfig.getServers()) {
            SocketConnection conn = null;
            final FutureCallback<Reply> future = new FutureCallback<Reply>();
            try {
                conn = new SocketConnection(addr, myConfig);
                conn.send(future, new ServerStatus());
                final Reply reply = future.get();
                final List<Document> results = reply.getResults();
                if (!results.isEmpty()) {
                    final Document doc = results.get(0);

                    if (isMongos(doc)) {
                        LOG.info("Sharded bootstrap to " + addr + ".");
                        myDelegate = new ShardedConnectionFactory(
                                new SocketConnectionFactory(myConfig), myConfig);
                    }
                    else if (isReplicationSet(doc)) {
                        LOG.info("Replica-set bootstrap to " + addr + ".");
                        myDelegate = new ReplicaSetConnectionFactory(
                                new SocketConnectionFactory(myConfig), myConfig);
                    }
                    else {
                        LOG.info("Simple MongoDB bootstrap to " + addr + ".");
                        myDelegate = new SocketConnectionFactory(myConfig);
                    }
                    return;
                }
            }
            catch (final IOException ioe) {
                LOG.log(Level.WARNING, "I/O error during bootstrap to " + addr
                        + ".", ioe);
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
                try {
                    if (conn != null) {
                        conn.close();
                    }
                }
                catch (final IOException okay) {
                    LOG.log(Level.WARNING,
                            "I/O error shutting down bootstrap connection to "
                                    + addr + ".", okay);
                }
            }
        }
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
     * Returns the underlying delegate factory.
     * 
     * @return The underlying delegate factory.
     */
    protected ConnectionFactory getDelegate() {
        if (myDelegate == null) {
            try {
                bootstrap();
            }
            catch (final IOException ioe) {
                LOG.log(Level.WARNING,
                        "Could not bootstrap connection factory.", ioe);
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
