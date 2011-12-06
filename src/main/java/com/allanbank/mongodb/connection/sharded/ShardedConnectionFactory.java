/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb.connection.sharded;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.allanbank.mongodb.MongoDbConfiguration;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.connection.Connection;
import com.allanbank.mongodb.connection.ConnectionFactory;
import com.allanbank.mongodb.connection.Message;
import com.allanbank.mongodb.connection.messsage.Query;
import com.allanbank.mongodb.connection.messsage.Reply;
import com.allanbank.mongodb.connection.proxy.ProxiedConnectionFactory;
import com.allanbank.mongodb.connection.state.ClusterState;
import com.allanbank.mongodb.connection.state.ServerState;

/**
 * Provides the ability to create connections to a shard configuration via
 * mongos servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ShardedConnectionFactory implements ConnectionFactory {

    /** The factory to create proxied connections. */
    protected final ProxiedConnectionFactory myConnectionFactory;

    /** The state of the cluster. */
    private final ClusterState myClusterState;

    /** The MongoDB client configuration. */
    private final MongoDbConfiguration myConfig;

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
        for (final InetSocketAddress address : config.getServers()) {
            final ServerState state = myClusterState.add(address.toString());

            // In a sharded environment we assume that all of the mongos servers
            // are writable.
            myClusterState.markWritable(state);
        }
    }

    /**
     * Creates a new connection to the shared mongos servers.
     * 
     * @see ConnectionFactory#connect()
     */
    @Override
    public Connection connect() throws IOException {
        return new ShardedConnection(myConnectionFactory, myClusterState,
                myConfig);
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
     * @param connection
     *            The connection to use to perform the query.
     * @return The ids for all of the mongos servers.
     */
    public List<String> findServers(final Connection connection) {
        final List<String> results = new ArrayList<String>();

        // Create a query to pull all of the mongos servers out of the config
        // database.
        final Query query = new Query("config", "mongos", BuilderFactory
                .start().get(), null, 1000, 0, false, false, false, false,
                false, false);

        // Send the request...
        final int requestId = connection.send(query);

        // Receive the response.
        final Message replyMsg = connection.receive();

        // Validate and pull out the response information.
        if (replyMsg instanceof Reply) {
            final Reply reply = (Reply) replyMsg;
            if (reply.getResponseToId() == requestId) {
                final List<Document> docs = reply.getResults();
                for (final Document doc : docs) {
                    final Element idElem = doc.get("_id");
                    if (idElem instanceof StringElement) {
                        final StringElement id = (StringElement) idElem;

                        results.add(id.getValue());
                    }
                }
            }
        }
        return results;
    }
}
