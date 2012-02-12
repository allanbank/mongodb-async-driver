/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.connection.sharded;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.MongoDbException;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.FutureCallback;
import com.allanbank.mongodb.connection.message.Reply;
import com.allanbank.mongodb.connection.message.ServerStatus;
import com.allanbank.mongodb.connection.proxy.AbstractProxyConnection;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;

/**
 * Provides a {@link Connection} implementation for connecting to a sharded
 * environment via mongos servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnection extends AbstractProxyConnection {

    /**
     * Creates a new {@link ShardedConnection}.
     * 
     * @param factory
     *            The factory to create proxied connections.
     * @param clusterState
     *            The state of the cluster.
     * @param config
     *            The MongoDB client configuration.
     */
    public ShardedConnection(final ProxiedConnectionFactory factory,
            final ClusterState clusterState, final MongoDbConfiguration config) {
        super(factory, clusterState, config);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Issues a { serverStatus : 1 } command on the 'admin' database and
     * verifies that the response is from a mongos.
     * </p>
     */
    @Override
    protected boolean verifyConnection(final Connection connection)
            throws MongoDbException {

        final FutureCallback<Reply> future = new FutureCallback<Reply>();

        try {
            connection.send(future, new ServerStatus());

            final Reply reply = future.get();
            final List<Document> results = reply.getResults();
            if (!results.isEmpty()) {
                final Document doc = results.get(0);

                return isMongos(doc);
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
}
