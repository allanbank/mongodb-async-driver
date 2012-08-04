/*
the  * Copyright 2011, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Logger;

import com.allanbank.mongodb.error.MongoDbAuthenticationException;

/**
 * Contains the configuration for the connection(s) to the MongoDB servers.
 * 
 * @copyright 2011, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbConfiguration implements Cloneable, Serializable {

    /** The name of the administration database. */
    public static final String ADMIN_DB_NAME = "admin";

    /** The default MongoDB port. */
    public static final int DEFAULT_PORT = 27017;

    /** The ASCII character encoding. */
    public static final Charset UTF8 = Charset.forName("UTF-8");

    /** Hex encoding characters. */
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    /** The logger for the {@link MongoDbConfiguration}. */
    private static final Logger LOG = Logger
            .getLogger(MongoDbConfiguration.class.getCanonicalName());

    /** The serialization version for the class. */
    private static final long serialVersionUID = 2964127883934086509L;

    /**
     * Hex encodes a byte array.
     * 
     * @param buf
     *            The bytes to encode.
     * @return The hex encoded string.
     */
    public static String asHex(final byte[] buf) {
        final char[] chars = new char[2 * buf.length];
        for (int i = 0; i < buf.length; ++i) {
            chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
            chars[(2 * i) + 1] = HEX_CHARS[buf[i] & 0x0F];
        }
        return new String(chars);
    }

    /**
     * Parse the name into a {@link InetSocketAddress}. If a port component is
     * not provided then port 27017 is assumed.
     * 
     * @param server
     *            The server[:port] string.
     * @return The {@link InetSocketAddress} parsed from the server string.
     */
    public static InetSocketAddress parseAddress(final String server) {
        String name = server;
        int port = DEFAULT_PORT;

        final int colonIndex = server.lastIndexOf(':');
        if (colonIndex > 0) {
            final String portString = server.substring(colonIndex + 1);
            try {
                port = Integer.parseInt(portString);
                name = server.substring(0, colonIndex);
            }
            catch (final NumberFormatException nfe) {
                // Not a port after the colon. Move on.
                port = DEFAULT_PORT;

            }
        }

        return new InetSocketAddress(name, port);
    }

    /** If true then the user should be authenticated as an anministrative user. */
    private boolean myAdminUser = false;

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
     * Defaults to 0 or forever.
     * </p>
     */
    private int myConnectTimeout = 0;

    /**
     * The default durability for write operations on the server.
     * <p>
     * Defaults to {@link Durability#NONE}.
     * </p>
     */
    private Durability myDefaultDurability = Durability.NONE;

    /**
     * The default read preference for a query.
     * <p>
     * Defaults to {@link ReadPreference#PRIMARY}.
     * </p>
     */
    private ReadPreference myDefaultReadPreference = ReadPreference.PRIMARY;

    /** The factory for creating threads to handle connections. */
    private transient ThreadFactory myFactory = null;

    /**
     * Determines the maximum number of connections to use.
     * <p>
     * Defaults to 3.
     * </p>
     * <p>
     * <em>Note:</em> In the case of connecting to a replica set this setting
     * limits the number of connections to the primary server. The driver will
     * create single connections to the secondary servers if queries are issued
     * with a {@link ReadPreference} other than {@link ReadPreference#PRIMARY}.
     * </p>
     */
    private int myMaxConnectionCount = 3;

    /**
     * Determines the maximum number of pending operations to allow per
     * connection. The higher the value the more "asynchronous" the driver can
     * be but risks more operations being in an unknown state on a connection
     * error. When the connection has this many pending requests additional
     * requests will block.
     * <p>
     * Defaults to 1024.
     * </p>
     * <p>
     * <em>Note:</em> In the case of an connection error it is impossible to
     * determine which pending operations completed and which did not.
     * </p>
     */
    private int myMaxPendingOperationsPerConnection = 1024;

    /** The password for authentication with the servers. */
    private String myPasswordHash = null;

    /**
     * Determines how long to wait (in milliseconds) for a socket read to
     * complete.
     * <p>
     * Defaults to 0 or never.
     * </p>
     */
    private int myReadTimeout = 0;

    /**
     * Determines how long to wait (in milliseconds) for a broken connection to
     * reconnect.
     * <p>
     * Defaults to 0 or forever.
     * </p>
     */
    private int myReconnectTimeout = 0;

    /**
     * The list of servers to initially attempt to connect to. Not final for
     * clone.
     */
    private ArrayList<InetSocketAddress> myServers = new ArrayList<InetSocketAddress>();

    /** The username for authentication with the servers. */
    private String myUsername = null;

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

        myFactory = Executors.defaultThreadFactory();
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
     * Creates a new {@link MongoDbConfiguration} instance using a MongoDB style
     * URL to initialize its state. Further configuration is possible once the
     * {@link MongoDbConfiguration} has been instantiated.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @throws IllegalArgumentException
     *             If the <tt>mongoDbUri</tt> is not a properly formated MongoDB
     *             style URL.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     */
    public MongoDbConfiguration(final MongoDbUri mongoDbUri)
            throws IllegalArgumentException {
        for (final String host : mongoDbUri.getHosts()) {
            addServer(host);
        }
        if (myServers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Must provide at least 1 host to connect to.");
        }

        if (mongoDbUri.getUserName() != null) {
            final String database = mongoDbUri.getDatabase();
            if (database.isEmpty() || database.equals(ADMIN_DB_NAME)) {
                authenticateAsAdmin(mongoDbUri.getUserName(),
                        mongoDbUri.getPassword());
            }
            else {
                authenticate(mongoDbUri.getUserName(), mongoDbUri.getPassword());
            }
        }

        boolean safe = false;
        int w = -1;
        boolean fsync = false;
        boolean journal = false;
        int wtimeout = 0;

        final StringTokenizer tokenizer = new StringTokenizer(
                mongoDbUri.getOptions(), "?;&");
        while (tokenizer.hasMoreTokens()) {
            String property;
            String value;

            final String propertyAndValue = tokenizer.nextToken();
            final int position = propertyAndValue.indexOf('=');
            if (position >= 0) {
                property = propertyAndValue.substring(0, position);
                value = propertyAndValue.substring(position + 1);
            }
            else {
                property = propertyAndValue;
                value = Boolean.TRUE.toString();
            }

            try {
                if ("replicaSet".equalsIgnoreCase(property)) {
                    LOG.info("Not validating the replica set name is '" + value
                            + "'.");
                }
                else if ("slaveOk".equalsIgnoreCase(property)) {
                    if (Boolean.parseBoolean(value)) {
                        myDefaultReadPreference = ReadPreference.SECONDARY;
                    }
                    else {
                        myDefaultReadPreference = ReadPreference.PRIMARY;
                    }
                }
                else if ("safe".equalsIgnoreCase(property)) {
                    safe = Boolean.parseBoolean(value);
                }
                else if ("w".equalsIgnoreCase(property)) {
                    safe = true;
                    w = Integer.parseInt(value);
                }
                else if ("wtimeout".equalsIgnoreCase(property)) {
                    safe = true;
                    wtimeout = Integer.parseInt(value);
                    if (w < 1) {
                        w = 1;
                    }
                }
                else if ("fsync".equalsIgnoreCase(property)) {
                    fsync = Boolean.parseBoolean(value);
                    if (fsync) {
                        journal = false;
                        safe = true;
                    }
                }
                else if ("journal".equalsIgnoreCase(property)) {
                    journal = Boolean.parseBoolean(value);
                    if (journal) {
                        fsync = false;
                        safe = true;
                    }
                }
                else if ("connectTimeoutMS".equalsIgnoreCase(property)) {
                    myConnectTimeout = Integer.parseInt(value);
                }
                else if ("socketTimeoutMS".equalsIgnoreCase(property)) {
                    myReadTimeout = Integer.parseInt(value);
                }

                // Extensions
                else if ("autoDiscoverServers".equalsIgnoreCase(property)) {
                    myAutoDiscoverServers = Boolean.parseBoolean(value);
                }
                else if ("maxConnectionCount".equalsIgnoreCase(property)) {
                    myMaxConnectionCount = Integer.parseInt(value);
                }
                else if ("maxPendingOperationsPerConnection"
                        .equalsIgnoreCase(property)) {
                    myMaxPendingOperationsPerConnection = Integer
                            .parseInt(value);
                }
                else if ("reconnectTimeoutMS".equalsIgnoreCase(property)) {
                    myReconnectTimeout = Integer.parseInt(value);
                }
                else if ("useSoKeepalive".equalsIgnoreCase(property)) {
                    myUsingSoKeepalive = Boolean.parseBoolean(value);
                }
                else {
                    LOG.info("Unknown property '" + property + "' and value '"
                            + value + "'.");
                }
            }
            catch (final NumberFormatException nfe) {
                throw new IllegalArgumentException("The '" + property
                        + "' parameter must have a numeric value not '" + value
                        + "'.", nfe);
            }
        }

        // Figure out the intended durability.
        if (safe) {
            if (fsync) {
                myDefaultDurability = Durability.fsyncDurable(wtimeout);
            }
            else if (journal) {
                myDefaultDurability = Durability.journalDurable(wtimeout);
            }
            else if (w > 0) {
                myDefaultDurability = Durability.replicaDurable(w, wtimeout);
            }
            else {
                myDefaultDurability = Durability.ACK;
            }
        }
        else {
            myDefaultDurability = Durability.NONE;
        }
    }

    /**
     * Creates a new {@link MongoDbConfiguration} instance using a MongoDB style
     * URL to initialize its state. Further configuration is possible once the
     * {@link MongoDbConfiguration} has been instantiated.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @throws IllegalArgumentException
     *             If the <tt>mongoDbUri</tt> is not a properly formated MongoDB
     *             style URL.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     */
    public MongoDbConfiguration(final String mongoDbUri)
            throws IllegalArgumentException {
        this(new MongoDbUri(mongoDbUri));
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
     * Adds a server to initially attempt to connect to.
     * 
     * @param server
     *            The server to add.
     */
    public void addServer(final String server) {
        addServer(parseAddress(server));
    }

    /**
     * Sets up the instance to authenticate with the MongoDB servers. This
     * should be done before using this configuration to instantiate a
     * {@link Mongo} instance.
     * 
     * @param username
     *            The username.
     * @param password
     *            the password.
     * @throws MongoDbAuthenticationException
     *             On a failure initializing the authentication information.
     */
    public void authenticate(final String username, final String password)
            throws MongoDbAuthenticationException {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("MD5");
            final byte[] digest = md5.digest((username + ":mongo:" + password)
                    .getBytes(UTF8));

            myAdminUser = false;
            myUsername = username;
            myPasswordHash = asHex(digest);
        }
        catch (final NoSuchAlgorithmException e) {
            throw new MongoDbAuthenticationException(e);
        }
    }

    /**
     * Sets up the instance to authenticate with the MongoDB servers. This
     * should be done before using this configuration to instantiate a
     * {@link Mongo} instance.
     * 
     * @param username
     *            The username.
     * @param password
     *            the password.
     * @throws MongoDbAuthenticationException
     *             On a failure initializing the authentication information.
     */
    public void authenticateAsAdmin(final String username, final String password)
            throws MongoDbAuthenticationException {
        authenticate(username, password);
        myAdminUser = true;
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
     * Defaults to 0 or forever.
     * </p>
     * 
     * @return The time to wait (in milliseconds) for a socket connection to
     *         complete.
     */
    public int getConnectTimeout() {
        return myConnectTimeout;
    }

    /**
     * Returns the default durability for write operations on the server.
     * <p>
     * Defaults to {@link Durability#NONE}.
     * </p>
     * 
     * @return The default durability for write operations on the server.
     */
    public Durability getDefaultDurability() {
        return myDefaultDurability;
    }

    /**
     * Returns the default read preference for a query.
     * <p>
     * Defaults to {@link ReadPreference#PRIMARY}.
     * </p>
     * 
     * @return The default read preference for a query.
     */
    public ReadPreference getDefaultReadPreference() {
        return myDefaultReadPreference;
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
     * with a {@link ReadPreference} other than {@link ReadPreference#PRIMARY}.
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
     * Defaults to 1024.
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
     * Gets the password hash for authentication with the database.
     * 
     * @return The password hash for authentication with the database.
     */
    public String getPasswordHash() {
        return myPasswordHash;
    }

    /**
     * Returns how long to wait (in milliseconds) for a socket read to complete.
     * <p>
     * Defaults to 0 or never.
     * </p>
     * 
     * @return The time to wait (in milliseconds) for a socket read to complete.
     */
    public int getReadTimeout() {
        return myReadTimeout;
    }

    /**
     * Returns how long to wait (in milliseconds) for a broken connection to be
     * reconnected.
     * <p>
     * Defaults to 0 or forever.
     * </p>
     * 
     * @return The time to wait (in milliseconds) for a broken connection to be
     *         reconnected.
     */
    public int getReconnectTimeout() {
        return myReconnectTimeout;
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
     * Returns the thread factory for managing connections.
     * 
     * @return The thread factory for managing connections.
     */
    public ThreadFactory getThreadFactory() {
        return myFactory;
    }

    /**
     * Gets the username for authenticating with the database.
     * 
     * @return The username for authenticating with the database.
     */
    public String getUsername() {
        return myUsername;
    }

    /**
     * Returns true if the user should authenticate as an administrative user.
     * 
     * @return True if the user should authenticate as an administrative user.
     */
    public boolean isAdminUser() {
        return myAdminUser;
    }

    /**
     * Returns true if the connection is authenticating.
     * 
     * @return True if the connections should authenticate with the server.
     */
    public boolean isAuthenticating() {
        return (myPasswordHash != null);
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
     * Sets the default durability for write operations on the server to the new
     * value.
     * 
     * @param defaultDurability
     *            The default durability for write operations on the server.
     */
    public void setDefaultDurability(final Durability defaultDurability) {
        myDefaultDurability = defaultDurability;
    }

    /**
     * Sets the value of the default read preference for a query.
     * <p>
     * Defaults to {@link ReadPreference#PRIMARY} if <code>null</code> is set.
     * </p>
     * 
     * @param defaultReadPreference
     *            The default read preference for a query.
     */
    public void setDefaultReadPreference(
            final ReadPreference defaultReadPreference) {
        if (defaultReadPreference == null) {
            myDefaultReadPreference = ReadPreference.PRIMARY;
        }
        else {
            myDefaultReadPreference = defaultReadPreference;
        }
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
     * with a {@link ReadPreference} other than {@link ReadPreference#PRIMARY}.
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
     * Sets how long to wait (in milliseconds) for a broken connection to
     * reconnect.
     * 
     * @param connectTimeout
     *            The time to wait (in milliseconds) for a broken connection to
     *            reconnect.
     */
    public void setReconnectTimeout(final int connectTimeout) {
        myReconnectTimeout = connectTimeout;
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
     * Sets the thread factory for managing connections to the new value.
     * 
     * @param factory
     *            The thread factory for managing connections.
     */
    public void setThreadFactory(final ThreadFactory factory) {
        myFactory = factory;
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
