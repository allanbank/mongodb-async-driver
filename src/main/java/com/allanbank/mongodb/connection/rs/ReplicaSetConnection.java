/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.rs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.allanbank.mongodb.Callback;
import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.messsage.IsMaster;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Reply;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;

/**
 * Provides a {@link Connection} implementation for connecting to a replica-set
 * environment.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ReplicaSetConnection extends AbstractProxyConnection {

    /** A connection to a Secondary Replica. */
    private Connection mySecondaryConnection;

    /**
     * Creates a new {@link ReplicaSetConnection}.
     * 
     * @param factory
     *            The factory to create proxied connections.
     * @param clusterState
     *            The state of the cluster.
     * @param config
     *            The MongoDB client configuration.
     */
    public ReplicaSetConnection(final ProxiedConnectionFactory factory,
            final ClusterState clusterState, final MongoDbConfiguration config) {
        super(factory, clusterState, config);
        mySecondaryConnection = null;
    }

    /**
     * Closes the underlying connection.
     * 
     * @see Connection#close()
     */
    @Override
    public void close() throws IOException {
        try {
            if (mySecondaryConnection != null) {
                mySecondaryConnection.close();
                mySecondaryConnection = null;
            }
        }
        finally {
            super.close();
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Checks if all if the messages are queries and can use a secondary
     * connection. If so then if a secondary connection is present it is used to
     * send the messages. Otherwise (or in the case of an error from the
     * secondary connection) the primary connection is used.
     * </p>
     */
    @Override
    public void send(final Callback<Reply> reply, final Message... messages)
            throws MongoDbException {

        boolean canUseSecondary = true;
        for (final Message message : messages) {
            if (message instanceof Query) {
                canUseSecondary &= ((Query) message).isSlaveOk();
            }
            else {
                canUseSecondary = false;
            }
        }

        final Connection secondary = mySecondaryConnection;
        if (canUseSecondary && (secondary != null)) {
            try {
                secondary.send(reply, messages);
            }
            catch (final MongoDbException error) {
                // Failed. Try the primary.
                mySecondaryConnection = null;
                try {
                    secondary.close();
                }
                catch (final IOException ignore) {
                    // TODO - Log the connection error to the secondary.
                }
            }
        }

        super.send(reply, messages);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Forwards the call to the {@link Connection} returned from
     * {@link #ensureConnected()}.
     * </p>
     */
    @Override
    public void send(final Message... messages) throws MongoDbException {
        send(null, messages);
    }

    /**
     * {@inheritDoc}
     * <p>
     * True if the connection is being used as the secondary connection.
     * </p>
     * 
     * @param connection
     *            The connection to possibly keep open.
     * @return True if the derived class is keeping the connection for other
     *         purposes.
     */
    @Override
    protected boolean keepConnection(final Connection connection) {
        return (connection == mySecondaryConnection);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Issues a { ismaster : 1 } command on the 'admin' database and updates the
     * cluster state with the results.
     * </p>
     * 
     * @return True if the connection is to the primary replica and false for a
     *         secondary replica.
     */
    @Override
    protected boolean verifyConnection(final Connection connection)
            throws MongoDbException {

        final FutureCallback<Reply> future = new FutureCallback<Reply>();
        try {

            connection.send(future, new IsMaster());

            final Reply reply = future.get();
            final List<Document> results = reply.getResults();
            if (!results.isEmpty()) {
                final Document doc = results.get(0);

                if (myConfig.isAutoDiscoverServers()) {

                    // Add all of the hosts to the state.
                    final Element hosts = doc.get("hosts");
                    if (hosts instanceof ArrayElement) {
                        final ArrayElement hostsArray = (ArrayElement) hosts;
                        for (final Element hostElement : hostsArray
                                .getEntries()) {
                            if (hostElement instanceof StringElement) {
                                myClusterState
                                        .add(((StringElement) hostElement)
                                                .getValue());
                            }
                        }
                    }
                }

                // Get the name of the primary server.
                final Element primary = doc.get("primary");
                if (primary instanceof StringElement) {
                    myClusterState.markWritable(myClusterState
                            .get(((StringElement) primary).getValue()));
                }

                // See if we are the primary.
                if (isPrimary(doc)) {
                    return true;
                }
                // See if we can use this as a secondary connection.
                else if (mySecondaryConnection == null) {
                    if (isSecondary(doc)) {
                        mySecondaryConnection = connection;
                    }
                }
            }
        }
        catch (final InterruptedException e) {
            throw new MongoDbException(e);
        }
        catch (final ExecutionException e) {
            if (e.getCause() instanceof MongoDbException) {
                throw (MongoDbException) e.getCause();
            }
            throw new MongoDbException(e.getCause());
        }

        return false;
    }

    /**
     * Returns true if the connection is to the primary server in the replica
     * set.
     * 
     * @param isMasterReply
     *            The reply from an <tt>ismaster</tt> command.
     * @return True if the reply indicates that the server is a
     *         <tt>ismaster</tt>. e.g., the document contains a
     *         <tt>ismaster</tt> boolean element set to true.
     */
    private boolean isPrimary(final Document isMasterReply) {
        final Element isSecondaryFlag = isMasterReply.get("ismaster");
        if (isSecondaryFlag instanceof BooleanElement) {
            return ((BooleanElement) isSecondaryFlag).getValue();
        }
        return false;
    }

    /**
     * Returns true if the connection can be used for "slaveOk" queries.
     * 
     * @param isMasterReply
     *            The reply from an <tt>ismaster</tt> command.
     * @return True if the reply indicates that the server is a
     *         <tt>secondary</tt>. e.g., the document contains a
     *         <tt>secondary</tt> boolean element set to true.
     */
    private boolean isSecondary(final Document isMasterReply) {
        final Element isSecondaryFlag = isMasterReply.get("secondary");
        if (isSecondaryFlag instanceof BooleanElement) {
            return ((BooleanElement) isSecondaryFlag).getValue();
        }
        return false;
    }
}
