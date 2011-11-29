/*
 * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Contains the configuration for the connection(s) to the MongoDB servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbConfiguration implements Cloneable, Serializable {

    /** The serialization version for the class. */
    private static final long serialVersionUID = 2964127883934086509L;

    /**
     * Determines if additional servers are auto discovered or if connections
     * are limited to the ones manually configured.
     * <p>
     * Defaults to true, e.g., auto-discover.
     * </p>
     */
    private boolean myAutoDiscoverServers = true;

    /**
     * Determines how long to wait (in milliseconds) for a socket connection to
     * complete.
     * <p>
     * Defaults to 10,000 ms.
     * </p>
     */
    private int myConnectTimeout = 10000;

    /**
     * Determines the maximum number of connections to use.
     * <p>
     * Defaults to 3.
     * </p>
     * <p>
     * <em>Note:</em> In the case of connecting to a replica set this setting
     * limits the number of connections to the primary server. The driver will
     * create single connections to the secondary servers if queries are issued
     * with "slaveOk" set to true.
     * </p>
     */
    private int myMaxConnectionCount = 3;

    /**
     * Determines the maximum number of pending operations to allow per
     * connection. The higher the value the more "asynchronous" the driver can
     * be but risks more operations being in an unknown state on a connection
     * error. When the connection has this many pending connections additional
     * requests will block.
     * <p>
     * Defaults to 100.
     * </p>
     * <p>
     * <em>Note:</em> In the case of an connection error it is impossible to
     * determine which pending operations completed and which did not.
     * </p>
     */
    private int myMaxPendingOperationsPerConnection = 100;

    /**
     * Determines how long to wait (in milliseconds) for a socket read to
     * complete.
     * <p>
     * Defaults to 60,000 ms.
     * </p>
     */
    private int myReadTimeout = 60000;

    /**
     * The list of servers to initially attempt to connect to. Not final for
     * clone.
     */
    private ArrayList<InetSocketAddress> myServers = new ArrayList<InetSocketAddress>();

    /**
     * Determines if the {@link java.net.Socket#setKeepAlive(boolean)
     * SO_KEEPALIVE} socket option is set.
     * <p>
     * Defaults to true, e.g., use SO_KEEPALIVE.
     * </p>
     */
    private boolean myUsingSoKeepalive = true;

    /**
     * Creates a new MongoDbConfiguration.
     */
    public MongoDbConfiguration() {
        super();
    }

    /**
     * Creates a new MongoDbConfiguration.
     * 
     * @param servers
     *            The initial set of servers to connect to.
     */
    public MongoDbConfiguration(final InetSocketAddress... servers) {
        this();

        myServers.addAll(Arrays.asList(servers));
    }

    /**
     * Creates a new MongoDbConfiguration.
     * 
     * @param other
     *            The configuration to copy.
     */
    public MongoDbConfiguration(final MongoDbConfiguration other) {
        this();

        setServers(other.getServers());

        myAutoDiscoverServers = other.isAutoDiscoverServers();
        myMaxConnectionCount = other.getMaxConnectionCount();
        myMaxPendingOperationsPerConnection = other
                .getMaxPendingOperationsPerConnection();
        myUsingSoKeepalive = other.isUsingSoKeepalive();
    }

    /**
     * Adds a server to initially attempt to connect to.
     * 
     * @param server
     *            The server to add.
     */
    public void addServer(final InetSocketAddress server) {
        myServers.add(server);
    }

    /**
     * Creates a copy of this MongoDbConfiguration.
     * <p>
     * Note: This is not a traditional clone to ensure a deep copy of all
     * information.
     * </p>
     */
    @Override
    public MongoDbConfiguration clone() {
        MongoDbConfiguration clone = null;
        try {
            clone = (MongoDbConfiguration) super.clone();
            clone.myServers = new ArrayList<InetSocketAddress>(getServers());
        }
        catch (final CloneNotSupportedException shouldNotHappen) {
            clone = new MongoDbConfiguration(this);
        }
        return clone;
    }

    /**
     * Returns how long to wait (in milliseconds) for a socket connection to
     * complete.
     * <p>
     * Defaults to 10,000 ms.
     * </p>
     * 
     * @return The time to wait (in milliseconds) for a socket connection to
     *         complete.
     */
    public int getConnectTimeout() {
        return myConnectTimeout;
    }

    /**
     * Returns the maximum number of connections to use.
     * <p>
     * Defaults to 3.
     * </p>
     * <p>
     * <em>Note:</em> In the case of connecting to a replica set this setting
     * limits the number of connections to the primary server. The driver will
     * create single connections to the secondary servers if queries are issued
     * with "slaveOk" set to true.
     * </p>
     * 
     * @return The maximum connections to use.
     */
    public int getMaxConnectionCount() {
        return myMaxConnectionCount;
    }

    /**
     * Returns the maximum number of pending operations to allow per connection.
     * The higher the value the more "asynchronous" the driver can be but risks
     * more operations being in an unknown state on a connection error. When the
     * connection has this many pending connections additional requests will
     * block.
     * <p>
     * Defaults to 100.
     * </p>
     * <p>
     * <em>Note:</em> In the case of an connection error it is impossible to
     * determine which pending operations completed and which did not. Setting
     * this value to 1 results in synchronous operations that wait for
     * responses.
     * </p>
     * 
     * @return The maximum number of pending operations to allow per connection.
     */
    public int getMaxPendingOperationsPerConnection() {
        return myMaxPendingOperationsPerConnection;
    }

    /**
     * Returns how long to wait (in milliseconds) for a socket read to complete.
     * <p>
     * Defaults to 60,000 ms.
     * </p>
     * 
     * @return The time to wait (in milliseconds) for a socket read to complete.
     */
    public int getReadTimeout() {
        return myReadTimeout;
    }

    /**
     * Returns the list of servers to initially attempt to connect to.
     * 
     * @return The list of servers to initially attempt to connect to.
     */
    public List<InetSocketAddress> getServers() {
        return myServers;
    }

    /**
     * Returns if additional servers are auto discovered or if connections are
     * limited to the ones manually configured.
     * <p>
     * Defaults to true, e.g., auto-discover.
     * </p>
     * 
     * @return True if additional servers are auto discovered
     */
    public boolean isAutoDiscoverServers() {
        return myAutoDiscoverServers;
    }

    /**
     * Returns if the {@link java.net.Socket#setKeepAlive(boolean) SO_KEEPALIVE}
     * socket option is set.
     * <p>
     * Defaults to true, e.g., use SO_KEEPALIVE.
     * </p>
     * 
     * @return True if the {@link java.net.Socket#setKeepAlive(boolean)
     *         SO_KEEPALIVE} socket option is set.
     */
    public boolean isUsingSoKeepalive() {
        return myUsingSoKeepalive;
    }

    /**
     * Sets if additional servers are auto discovered or if connections are
     * limited to the ones manually configured.
     * <p>
     * Defaults to true, e.g., auto-discover.
     * </p>
     * 
     * @param autoDiscoverServers
     *            The new value for auto-discovering servers.
     */
    public void setAutoDiscoverServers(final boolean autoDiscoverServers) {
        myAutoDiscoverServers = autoDiscoverServers;
    }

    /**
     * Sets how long to wait (in milliseconds) for a socket connection to
     * complete.
     * 
     * @param connectTimeout
     *            The time to wait (in milliseconds) for a socket connection to
     *            complete.
     */
    public void setConnectTimeout(final int connectTimeout) {
        myConnectTimeout = connectTimeout;
    }

    /**
     * Sets the maximum number of connections to use.
     * <p>
     * Defaults to 3.
     * </p>
     * <p>
     * <em>Note:</em> In the case of connecting to a replica set this setting
     * limits the number of connections to the primary server. The driver will
     * create single connections to the secondary servers if queries are issued
     * with "slaveOk" set to true.
     * </p>
     * 
     * @param maxConnectionCount
     *            New maximum number of connections to use.
     */
    public void setMaxConnectionCount(final int maxConnectionCount) {
        myMaxConnectionCount = maxConnectionCount;
    }

    /**
     * Sets the maximum number of pending operations to allow per connection.
     * The higher the value the more "asynchronous" the driver can be but risks
     * more operations being in an unknown state on a connection error. When the
     * connection has this many pending connections additional requests will
     * block.
     * 
     * @param maxPendingOperationsPerConnection
     *            The new maximum number of pending operations to allow per
     *            connection.
     */
    public void setMaxPendingOperationsPerConnection(
            final int maxPendingOperationsPerConnection) {
        myMaxPendingOperationsPerConnection = maxPendingOperationsPerConnection;
    }

    /**
     * @param readTimeout
     *            The time to wait (in milliseconds) for a socket read to
     *            complete.
     */
    public void setReadTimeout(final int readTimeout) {
        myReadTimeout = readTimeout;
    }

    /**
     * Sets the servers to initially attempt to connect to.
     * 
     * @param servers
     *            The servers to connect to.
     */
    public void setServers(final List<InetSocketAddress> servers) {
        myServers.clear();
        if (servers != null) {
            myServers.addAll(servers);
        }
    }

    /**
     * Sets if the {@link java.net.Socket#setKeepAlive(boolean) SO_KEEPALIVE}
     * socket option is set.
     * <p>
     * Defaults to true, e.g., use SO_KEEPALIVE.
     * </p>
     * 
     * @param usingSoKeepalive
     *            The new value for using SO_KEEPALIVE.
     */
    public void setUsingSoKeepalive(final boolean usingSoKeepalive) {
        myUsingSoKeepalive = usingSoKeepalive;
    }

}
