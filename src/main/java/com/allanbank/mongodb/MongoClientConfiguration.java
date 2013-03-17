/*
 * Copyright 2011-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.net.SocketFactory;

import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.ServerNameUtils;

/**
 * Contains the configuration for the connection(s) to the MongoDB servers.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientConfiguration implements Cloneable, Serializable {

    /** The name of the administration database. */
    public static final String ADMIN_DB_NAME = "admin";

    /** The default database. */
    public static final String DEFAULT_DB_NAME = "local";

    /** The ASCII character encoding. */
    public static final Charset UTF8 = Charset.forName("UTF-8");

    /** The logger for the {@link MongoClientConfiguration}. */
    private static final Logger LOG = Logger
            .getLogger(MongoClientConfiguration.class.getCanonicalName());

    /** The serialization version for the class. */
    private static final long serialVersionUID = 2964127883934086500L;

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

    /** The credentials for the user. */
    private final ConcurrentHashMap<String, Credential> myCredentials;

    /**
     * The default database for the connection. This is used as the database to
     * authenticate against if the user is not an administrative user.
     * <p>
     * Defaults to {@value #DEFAULT_DB_NAME}.
     * </p>
     */
    private String myDefaultDatabase = DEFAULT_DB_NAME;

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

    /** The executor for responses from the database. */
    private transient Executor myExecutor = null;

    /**
     * Determines the type of hand off lock to use between threads in the core
     * of the driver.
     * <p>
     * Defaults to {@link LockType#MUTEX}.
     * </p>
     */
    private LockType myLockType = LockType.MUTEX;

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

    /**
     * Determines the maximum number of milliseconds that a secondary can be
     * behind the primary before they will be excluded from being used for
     * queries on secondaries.
     * <p>
     * Defaults to 5 minutes (300,000).
     * </p>
     */
    private long myMaxSecondaryLag = TimeUnit.MINUTES.toMillis(5);

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
    private List<InetSocketAddress> myServers = new ArrayList<InetSocketAddress>();

    /** The socket factory for creating sockets. */
    private transient SocketFactory mySocketFactory = null;

    /** The factory for creating threads to handle connections. */
    private transient ThreadFactory myThreadFactory = null;

    /**
     * Determines if the {@link java.net.Socket#setKeepAlive(boolean)
     * SO_KEEPALIVE} socket option is set.
     * <p>
     * Defaults to true, e.g., use SO_KEEPALIVE.
     * </p>
     */
    private boolean myUsingSoKeepalive = true;

    /**
     * Creates a new MongoClientConfiguration.
     */
    public MongoClientConfiguration() {
        super();

        myThreadFactory = Executors.defaultThreadFactory();
        myCredentials = new ConcurrentHashMap<String, Credential>();
    }

    /**
     * Creates a new MongoClientConfiguration.
     * 
     * @param servers
     *            The initial set of servers to connect to.
     */
    public MongoClientConfiguration(final InetSocketAddress... servers) {
        this();

        for (final InetSocketAddress server : servers) {
            addServer(server);
        }
    }

    /**
     * Creates a new MongoClientConfiguration.
     * 
     * @param other
     *            The configuration to copy.
     */
    public MongoClientConfiguration(final MongoClientConfiguration other) {
        this();

        myServers.addAll(other.getServerAddresses());

        myAutoDiscoverServers = other.isAutoDiscoverServers();
        myMaxConnectionCount = other.getMaxConnectionCount();
        myMaxPendingOperationsPerConnection = other
                .getMaxPendingOperationsPerConnection();
        myUsingSoKeepalive = other.isUsingSoKeepalive();
    }

    /**
     * Creates a new {@link MongoClientConfiguration} instance using a MongoDB
     * style URL to initialize its state. Further configuration is possible once
     * the {@link MongoClientConfiguration} has been instantiated.
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
    public MongoClientConfiguration(final MongoDbUri mongoDbUri)
            throws IllegalArgumentException {
        this(mongoDbUri, Durability.ACK);
    }

    /**
     * Creates a new {@link MongoClientConfiguration} instance using a MongoDB
     * style URL to initialize its state. Further configuration is possible once
     * the {@link MongoClientConfiguration} has been instantiated.
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
    public MongoClientConfiguration(final String mongoDbUri)
            throws IllegalArgumentException {
        this(new MongoDbUri(mongoDbUri));
    }

    /**
     * Creates a new {@link MongoClientConfiguration} instance using a MongoDB
     * style URL to initialize its state. Further configuration is possible once
     * the {@link MongoClientConfiguration} has been instantiated.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @param defaultDurability
     *            The default durability.
     * @throws IllegalArgumentException
     *             If the <tt>mongoDbUri</tt> is not a properly formated MongoDB
     *             style URL.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     */
    protected MongoClientConfiguration(final MongoDbUri mongoDbUri,
            final Durability defaultDurability) throws IllegalArgumentException {
        this();

        for (final String host : mongoDbUri.getHosts()) {
            addServer(host);
        }
        if (myServers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Must provide at least 1 host to connect to.");
        }

        final String database = mongoDbUri.getDatabase();
        if (!database.isEmpty()) {
            setDefaultDatabase(database);
        }

        if (mongoDbUri.getUserName() != null) {
            if (database.isEmpty()) {
                setCredentials(Arrays.asList(new Credential(mongoDbUri
                        .getUserName(), mongoDbUri.getPassword().toCharArray(),
                        Credential.MONGODB_CR)));
            }
            else {
                setCredentials(Arrays.asList(new Credential(mongoDbUri
                        .getUserName(), mongoDbUri.getPassword().toCharArray(),
                        database, Credential.MONGODB_CR)));
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
            myDefaultDurability = defaultDurability;
        }
    }

    /**
     * Adds the specified credentials to the configuration.
     * 
     * @param credentials
     *            The credentials to use when accessing the MongoDB server.
     * @throws IllegalArgumentException
     *             If the credentials refer to an unknown authentication type or
     *             the configuration already has a set of credentials for the
     *             credentials specified database.
     */
    public void addCredential(final Credential credentials)
            throws IllegalArgumentException {
        try {
            credentials.loadAuthenticator();

            final Credential previous = myCredentials.putIfAbsent(
                    credentials.getDatabase(), credentials);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "There can only be one set of credentials for each database.");
            }
        }
        catch (final ClassNotFoundException cnfe) {
            throw new IllegalArgumentException(
                    "Could not load the credentials authenticator.", cnfe);
        }
        catch (final InstantiationException ie) {
            throw new IllegalArgumentException(
                    "Could not load the credentials authenticator.", ie);
        }
        catch (final IllegalAccessException iae) {
            throw new IllegalArgumentException(
                    "Could not load the credentials authenticator.", iae);
        }
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
        myServers.add(ServerNameUtils.parse(server));
    }

    /**
     * Sets up the instance to authenticate with the MongoDB servers. This
     * should be done before using this configuration to instantiate a
     * {@link Mongo} instance.
     * 
     * @param userName
     *            The user name.
     * @param password
     *            the password.
     * @throws MongoDbAuthenticationException
     *             On a failure initializing the authentication information.
     * @deprecated Replaced with the more general {@link Credential} capability.
     *             Will be removed after the 1.3.0 release.
     */
    @Deprecated
    public void authenticate(final String userName, final String password)
            throws MongoDbAuthenticationException {
        addCredential(new Credential(userName, password.toCharArray(),
                getDefaultDatabase(), Credential.MONGODB_CR));
    }

    /**
     * Sets up the instance to authenticate with the MongoDB servers. This
     * should be done before using this configuration to instantiate a
     * {@link Mongo} instance.
     * 
     * @param userName
     *            The user name.
     * @param password
     *            the password.
     * @throws MongoDbAuthenticationException
     *             On a failure initializing the authentication information.
     * @deprecated Replaced with the more general {@link Credential} capability.
     *             Will be removed after the 1.3.0 release.
     */
    @Deprecated
    public void authenticateAsAdmin(final String userName, final String password)
            throws MongoDbAuthenticationException {
        addCredential(new Credential(userName, password.toCharArray(),
                ADMIN_DB_NAME, Credential.MONGODB_CR));
    }

    /**
     * Creates a copy of this MongoClientConfiguration.
     * <p>
     * Note: This is not a traditional clone to ensure a deep copy of all
     * information.
     * </p>
     */
    @Override
    public MongoClientConfiguration clone() {
        MongoClientConfiguration clone = null;
        try {
            clone = (MongoClientConfiguration) super.clone();
            clone.myServers = new ArrayList<InetSocketAddress>(
                    getServerAddresses());
        }
        catch (final CloneNotSupportedException shouldNotHappen) {
            clone = new MongoClientConfiguration(this);
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
     * Returns the map of database names to credentials to use to access that
     * database on the server.
     * 
     * @return The map of database names to credentials to use to access that
     *         database on the server.
     */
    public Collection<Credential> getCredentials() {
        return Collections.unmodifiableCollection(myCredentials.values());
    }

    /**
     * Returns the default database for the connection.
     * <p>
     * This is used as the database to authenticate against if the user is not
     * an administrative user.
     * </p>
     * <p>
     * Defaults to {@value #DEFAULT_DB_NAME}.
     * </p>
     * 
     * @return The default database value.
     * @deprecated Replaced with the more general {@link Credential} capability.
     *             Will be removed after the 1.3.0 release.
     */
    @Deprecated
    public String getDefaultDatabase() {
        return myDefaultDatabase;
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
     * Returns the executor to use when processing responses from the server.
     * <p>
     * By default the executor is <code>null</code> which will cause the reply
     * handling to execute on the socket's receive thread.
     * </p>
     * <p>
     * Care should be taken to ensure that the executor does not drop requests.
     * This implies that the
     * {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy} or
     * similar should be used as the
     * {@link java.util.concurrent.RejectedExecutionHandler}.
     * </p>
     * 
     * @return The executor value.
     */
    public Executor getExecutor() {
        return myExecutor;
    }

    /**
     * Returns the type of hand off lock to use between threads in the core of
     * the driver.
     * <p>
     * Defaults to {@link LockType#MUTEX}.
     * </p>
     * 
     * @return The type of hand off lock used.
     */
    public LockType getLockType() {
        return myLockType;
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
     * Returns the maximum number of milliseconds that a secondary can be behind
     * the primary before they will be excluded from being used for queries on
     * secondaries.
     * <p>
     * Defaults to 5 minutes (300,000).
     * </p>
     * 
     * @return The maximum number of milliseconds that a secondary can be behind
     *         the primary before they will be excluded from being used for
     *         queries on secondaries.
     */
    public long getMaxSecondaryLag() {
        return myMaxSecondaryLag;
    }

    /**
     * Gets the password hash for authentication with the database.
     * 
     * @return The password hash for authentication with the database.
     * @deprecated Replaced with the more general {@link Credential} capability.
     *             Will be removed after the 1.3.0 release.
     */
    @Deprecated
    public String getPasswordHash() {
        if (!myCredentials.isEmpty()) {
            final Credential credentials = myCredentials.entrySet().iterator()
                    .next().getValue();
            try {
                final MessageDigest md5 = MessageDigest.getInstance("MD5");
                final byte[] digest = md5.digest((credentials.getUserName()
                        + ":mongo:" + new String(credentials.getPassword()))
                        .getBytes(UTF8));

                return IOUtils.toHex(digest);
            }
            catch (final NoSuchAlgorithmException e) {
                throw new MongoDbAuthenticationException(e);
            }

        }
        return null;
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
    public List<InetSocketAddress> getServerAddresses() {
        return Collections.unmodifiableList(myServers);
    }

    /**
     * Returns the list of servers to initially attempt to connect to.
     * 
     * @return The list of servers to initially attempt to connect to.
     */
    public List<String> getServers() {
        final List<String> servers = new ArrayList<String>(myServers.size());
        for (final InetSocketAddress addr : myServers) {
            servers.add(ServerNameUtils.normalize(addr));
        }
        return servers;
    }

    /**
     * Returns the socket factory to use in making connections to the MongoDB
     * server.
     * <p>
     * Defaults to {@link SocketFactory#getDefault() SocketFactory.getDefault()}
     * .
     * </p>
     * 
     * @return The socketFactory value.
     * @see #setSocketFactory(SocketFactory) setSocketFactory(...) or usage
     *      examples and suggestions.
     */
    public SocketFactory getSocketFactory() {
        if (mySocketFactory == null) {
            mySocketFactory = SocketFactory.getDefault();
        }
        return mySocketFactory;
    }

    /**
     * Returns the thread factory for managing connections.
     * 
     * @return The thread factory for managing connections.
     */
    public ThreadFactory getThreadFactory() {
        return myThreadFactory;
    }

    /**
     * Gets the user name for authenticating with the database.
     * 
     * @return The user name for authenticating with the database.
     * @deprecated Replaced with the more general {@link Credential} capability.
     *             Will be removed after the 1.3.0 release.
     */
    @Deprecated
    public String getUserName() {
        if (!myCredentials.isEmpty()) {
            final Credential credentials = myCredentials.entrySet().iterator()
                    .next().getValue();
            return credentials.getUserName();
        }
        return null;
    }

    /**
     * Returns true if the user should authenticate as an administrative user.
     * 
     * @return True if the user should authenticate as an administrative user.
     * @deprecated Replaced with the more general {@link Credential} capability.
     *             Will be removed after the 1.3.0 release.
     */
    @Deprecated
    public boolean isAdminUser() {
        return myCredentials.containsKey(ADMIN_DB_NAME);
    }

    /**
     * Returns true if the connection is authenticating. If any credentials have
     * been added to this configuration then all connections will use
     * authentication.
     * 
     * @return True if the connections should authenticate with the server.
     */
    public boolean isAuthenticating() {
        return !myCredentials.isEmpty();
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
     * Sets the credentials to use to access the server. This removes all
     * existing credentials.
     * 
     * @param credentials
     *            The credentials to use to access the server..
     * @throws IllegalArgumentException
     *             If the credentials refer to an unknown authentication type or
     *             the configuration already has a set of credentials for the
     *             credentials specified database.
     */
    public void setCredentials(final Collection<Credential> credentials) {
        myCredentials.clear();
        for (final Credential credential : credentials) {
            addCredential(credential);
        }
    }

    /**
     * Sets the default database for the connection.
     * <p>
     * This is used as the database to authenticate against if the user is not
     * an administrative user.
     * </p>
     * <p>
     * Defaults to {@value #DEFAULT_DB_NAME}.
     * </p>
     * 
     * @param defaultDatabase
     *            The new default database value.
     * @deprecated Replaced with the more general {@link Credential} capability.
     *             Will be removed after the 1.3.0 release.
     */
    @Deprecated
    public void setDefaultDatabase(final String defaultDatabase) {
        myDefaultDatabase = defaultDatabase;
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
     * Sets the value of executor for replies from the server.
     * <p>
     * By default the executor is <code>null</code> which will cause the reply
     * handling to execute on the socket's receive thread.
     * </p>
     * <p>
     * Care should be taken to ensure that the executor does not drop requests.
     * This implies that the
     * {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy} or
     * similar should be used as the
     * {@link java.util.concurrent.RejectedExecutionHandler}.
     * </p>
     * 
     * @param executor
     *            The new value for the executor.
     */
    public void setExecutor(final Executor executor) {
        myExecutor = executor;
    }

    /**
     * Sets the type of hand off lock to use between threads in the core of the
     * driver.
     * <p>
     * Defaults to {@link LockType#MUTEX}.
     * </p>
     * 
     * @param lockType
     *            The new value for the type of hand off lock used.
     */
    public void setLockType(final LockType lockType) {
        myLockType = lockType;
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
     * Sets the maximum number of milliseconds that a secondary can be behind
     * the primary before they will be excluded from being used for queries on
     * secondaries.
     * <p>
     * Defaults to 5 minutes (300,000).
     * </p>
     * 
     * @param maxSecondaryLag
     *            The new value for the maximum number of milliseconds that a
     *            secondary can be behind the primary before they will be
     *            excluded from being used for queries on secondaries.
     */
    public void setMaxSecondaryLag(final long maxSecondaryLag) {
        myMaxSecondaryLag = maxSecondaryLag;
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
            for (final InetSocketAddress server : servers) {
                addServer(server);
            }
        }
    }

    /**
     * Sets the socket factory to use in making connections to the MongoDB
     * server. Setting the SocketFactory to null resets the factory to the
     * default.
     * <p>
     * Defaults to {@link SocketFactory#getDefault()
     * SocketFactory.getDefault().}
     * </p>
     * <p>
     * For SSL based connections this can be an appropriately configured
     * {@link javax.net.ssl.SSLSocketFactory}.
     * </p>
     * <p>
     * Other {@link Socket} and {@link InetSocketAddress} implementations with
     * an appropriate {@link SocketFactory} implementation can be used with the
     * driver. The driver only ever calls the
     * {@link SocketFactory#createSocket()} method and then connects the socket
     * passing the server's {@link InetSocketAddress}.
     * </p>
     * <p>
     * See the <a href="http://code.google.com/p/junixsocket">junixsocket
     * Project</a> for an example of a {@link Socket} and
     * {@link InetSocketAddress} implementations for UNIX Domain Sockets that
     * can be wrapped with SocketFactory similar to the following:<blockquote>
     * <code><pre>
     *  public class AFUNIXSocketFactory extends SocketFactory {
     *      public Socket createSocket() throws java.io.IOException {
     *          return new org.newsclub.net.unix.AFUNIXSocket.newInstance();
     *      }
     *      
     *      public Socket createSocket(String host, int port) throws SocketException {
     *          throw new SocketException("AFUNIX socket does not support connections to a host/port");
     *      }
     *  
     *      public Socket createSocket(InetAddress host, int port) throws SocketException {
     *          throw new SocketException("AFUNIX socket does not support connections to a host/port");
     *      }
     *  
     *      public Socket createSocket(String host, int port, InetAddress localHost,
     *              int localPort) throws SocketException {
     *          throw new SocketException("AFUNIX socket does not support connections to a host/port");
     *      }
     *  
     *      public Socket createSocket(InetAddress address, int port,
     *              InetAddress localAddress, int localPort) throws SocketException {
     *          throw new SocketException("AFUNIX socket does not support connections to a host/port");
     *      }
     *  }
     * </pre></code></blockquote>
     * </p>
     * 
     * @param socketFactory
     *            The socketFactory value.
     * 
     * @see <a href="http://code.google.com/p/junixsocket">junixsocket
     *      Project</a>
     */
    public void setSocketFactory(final SocketFactory socketFactory) {
        if (socketFactory == null) {
            mySocketFactory = SocketFactory.getDefault();
        }
        else {
            mySocketFactory = socketFactory;
        }
    }

    /**
     * Sets the thread factory for managing connections to the new value.
     * 
     * @param factory
     *            The thread factory for managing connections.
     */
    public void setThreadFactory(final ThreadFactory factory) {
        myThreadFactory = factory;
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
