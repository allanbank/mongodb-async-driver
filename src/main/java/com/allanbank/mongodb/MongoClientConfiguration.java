/*
 * #%L
 * MongoClientConfiguration.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import com.allanbank.mongodb.bson.io.StringEncoderCache;
import com.allanbank.mongodb.error.MongoDbAuthenticationException;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.ServerNameUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Contains the configuration for the connection(s) to the MongoDB servers.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2011-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoClientConfiguration implements Cloneable, Serializable {

    /** The name of the administration database. */
    public static final String ADMIN_DB_NAME = "admin";

    /** The default database. */
    public static final String DEFAULT_DB_NAME = "local";

    /**
     * The name of the logger used to log the messages when
     * {@link #setLogMessagesEnabled(boolean) message logging} is enabled.
     */
    public static final String MESSAGE_LOGGER_NAME = "com.allanbank.mongodb.messages";

    /**
     * The name of the logger used to log the periodic metrics and the final
     * metrics when the client is closed.
     */
    public static final String METRICS_LOGGER_NAME = "com.allanbank.mongodb.metrics";

    /** The ASCII character encoding. */
    public static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * The default maximum number of strings to keep in the string encoder and
     * decoder cache.
     */
    protected static final int DEFAULT_MAX_STRING_CACHE_ENTRIES = 1024;

    /** The default maximum length byte array / string to cache. */
    protected static final int DEFAULT_MAX_STRING_CACHE_LENGTH = StringEncoderCache.DEFAULT_MAX_CACHE_LENGTH;

    /** The logger for the {@link MongoClientConfiguration}. */
    private static final Log LOG = LogFactory
            .getLog(MongoClientConfiguration.class);

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
     * Determines the model the driver uses for managing connections.
     * <p>
     * Defaults to {@link ConnectionModel#RECEIVER_THREAD}.
     * </p>
     */
    private ConnectionModel myConnectionModel = ConnectionModel.RECEIVER_THREAD;

    /**
     * Determines how long to wait (in milliseconds) for a socket connection to
     * complete.
     * <p>
     * Defaults to 0 or forever.
     * </p>
     */
    private int myConnectTimeout = 0;

    /**
     * The credentials for the user. This should be final but for support for
     * the clone() method.
     */
    private ConcurrentHashMap<String, Credential> myCredentials;

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
     * Defaults to {@link Durability#ACK}.
     * </p>
     */
    private Durability myDefaultDurability = Durability.ACK;

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
     * The legacy credentials created via {@link #authenticate(String, String)}
     * and {@link #setDefaultDatabase(String)}.
     */
    private Credential myLegacyCredential;

    /**
     * Determines the type of hand off lock to use between threads in the core
     * of the driver.
     * <p>
     * Defaults to {@link LockType#MUTEX}.
     * </p>
     */
    private LockType myLockType = LockType.MUTEX;

    /**
     * Determines if the driver logs each message that is sent or received by
     * the client. Logged messages will be at the DEBUG level on the logger
     * {@value #MESSAGE_LOGGER_NAME}. {@link #isMetricsEnabled()} must be true
     * for message logging to be supported.
     * <p>
     * <b>WARNING</>: Enabling message logging will have a sever impact on
     * performance.
     * <p>
     * Defaults to false, to not log messages.
     * </p>
     */
    private boolean myLogMessagesEnabled = false;

    /**
     * The maximum number of strings that may have their encoded form cached.
     * <p>
     * Defaults to {@value #DEFAULT_MAX_STRING_CACHE_ENTRIES}.
     * </p>
     */
    private int myMaxCachedStringEntries = DEFAULT_MAX_STRING_CACHE_ENTRIES;

    /**
     * The maximum length for a string that the stream is allowed to cache.This
     * can be used to stop a single long string from pushing useful values out
     * of the cache. Setting this value to zero turns off the caching.
     * <p>
     * Defaults to {@value #DEFAULT_MAX_STRING_CACHE_LENGTH}.
     * </p>
     */
    private int myMaxCachedStringLength = DEFAULT_MAX_STRING_CACHE_LENGTH;

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
     * Determines the number of read timeouts (a tick) before closing the
     * connection.
     * <p>
     * Defaults to {@link Integer#MAX_VALUE}.
     * </p>
     */
    private int myMaxIdleTickCount = Integer.MAX_VALUE;

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
     * Returns true if the driver collects metrics on the messages it sends and
     * receives to the MongoDB cluster.
     * <p>
     * If enabled and the JVM supports JMX then the metrics will be exposed via
     * JMX. Regardless of JMX support the driver will periodically create a log
     * message containing the current metrics. Logged messages will be at the
     * {@link MongoClientConfiguration#setMetricsLogLevel configured} level
     * using the logger {@value #METRICS_LOGGER_NAME}.
     * <p>
     * Metrics must be enables for message logging to be supported.
     * </p>
     * <p>
     * Defaults to true to collect metrics.
     * </p>
     * <p>
     * This value must be set prior to constructing the {@link MongoClient} and
     * any changes after a {@code MongoClient} is constructed will have no
     * effect for that {@code MongoClient}.
     * </p>
     */
    private boolean myMetricsEnabled = true;

    /**
     * Returns the level that the driver logs metrics. The value is based on the
     * {@link Level#intValue()}. Common values include:
     * <ul>
     * <li>FINE/DEBUG - 500
     * <li>INFO - 800
     * <li>WARNING - 900
     * <li>ERROR/SEVERE - 1000
     * </ul>
     * <p>
     * Defaults to DEBUG/500.
     * </p>
     */
    private int myMetricsLogLevel = Level.FINE.intValue();

    /**
     * Determines the minimum number of connections to try and keep open.
     * <p>
     * Defaults to 0.
     * </p>
     */
    private int myMinConnectionCount = 0;

    /**
     * Support for emitting property change events to listeners. Not final for
     * clone.
     */
    private PropertyChangeSupport myPropSupport;

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
     * The list of servers to initially attempt to connect to. This should be
     * final but for support for the clone() method.
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
        myPropSupport = new PropertyChangeSupport(this);
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

        myAutoDiscoverServers = other.isAutoDiscoverServers();
        myConnectionModel = other.getConnectionModel();
        myConnectTimeout = other.getConnectTimeout();
        myDefaultDatabase = other.getDefaultDatabase();
        myDefaultDurability = other.getDefaultDurability();
        myDefaultReadPreference = other.getDefaultReadPreference();
        myExecutor = other.getExecutor();
        myLockType = other.getLockType();
        myMaxCachedStringEntries = other.getMaxCachedStringEntries();
        myMaxCachedStringLength = other.getMaxCachedStringLength();
        myMaxConnectionCount = other.getMaxConnectionCount();
        myMaxIdleTickCount = other.getMaxIdleTickCount();
        myMaxPendingOperationsPerConnection = other
                .getMaxPendingOperationsPerConnection();
        myMaxSecondaryLag = other.getMaxSecondaryLag();
        myMinConnectionCount = other.getMinConnectionCount();
        myReadTimeout = other.getReadTimeout();
        myReconnectTimeout = other.getReconnectTimeout();
        mySocketFactory = other.getSocketFactory();
        myThreadFactory = other.getThreadFactory();
        myUsingSoKeepalive = other.isUsingSoKeepalive();

        for (final Credential credential : other.getCredentials()) {
            addCredential(credential);
        }
        for (final InetSocketAddress addr : other.getServerAddresses()) {
            addServer(addr);
        }
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

        myDefaultDurability = defaultDurability;
        for (final String host : mongoDbUri.getHosts()) {
            addServer(host);
        }
        if (myServers.isEmpty()) {
            throw new IllegalArgumentException(
                    "Must provide at least 1 host to connect to.");
        }
        if (!mongoDbUri.getDatabase().isEmpty()) {
            setDefaultDatabase(mongoDbUri.getDatabase());
        }

        final Map<String, String> parameters = mongoDbUri.getParsedOptions();
        final Map<String, String> renames = new HashMap<String, String>();

        // Renames for the standard names.
        final Iterator<Map.Entry<String, String>> iter = parameters.entrySet()
                .iterator();
        while (iter.hasNext()) {
            final Map.Entry<String, String> entry = iter.next();
            final String name = entry.getKey();
            if ("sockettimeoutms".equals(entry.getKey())) {
                renames.put("readtimeout", entry.getValue());
                iter.remove();
            }
            else if ("usesokeepalive".equals(name)) {
                renames.put("usingsokeepalive", entry.getValue());
                iter.remove();
            }
            else if ("minpoolsize".equals(name)) {
                renames.put("minconnectioncount", entry.getValue());
                iter.remove();
            }
            else if ("maxpoolsize".equals(name)) {
                renames.put("maxconnectioncount", entry.getValue());
                iter.remove();
            }
            else if (name.endsWith("ms")) {
                final String newName = name.substring(0,
                        name.length() - "ms".length());
                renames.put(newName, entry.getValue());
                iter.remove();
            }
        }
        parameters.putAll(renames);

        // Remove the parameters for the Credentials and durability and use the
        // full URI for the text to feed to the property editor.
        parameters.keySet().removeAll(CredentialEditor.MONGODB_URI_FIELDS);
        parameters.put("credentials", mongoDbUri.toString());

        if (parameters.keySet().removeAll(DurabilityEditor.MONGODB_URI_FIELDS)) {
            parameters.put("defaultdurability", mongoDbUri.toString());
        }
        if (parameters.keySet().removeAll(
                ReadPreferenceEditor.MONGODB_URI_FIELDS)) {
            parameters.put("defaultreadpreference", mongoDbUri.toString());
        }

        // Special handling for the SSL parameter.
        final String sslValue = parameters.remove("ssl");
        if (mongoDbUri.isUseSsl() || (sslValue != null)) {
            if (mongoDbUri.isUseSsl() || Boolean.parseBoolean(sslValue)) {
                try {
                    setSocketFactory((SocketFactory) Class
                            .forName(
                                    "com.allanbank.mongodb.extensions.tls.TlsSocketFactory")
                            .newInstance());
                }
                catch (final Throwable error) {
                    setSocketFactory(SSLSocketFactory.getDefault());
                    LOG.warn("Using the JVM default SSL Socket Factory. "
                            + "This may allow man-in-the-middle attacks. "
                            + "See http://www.allanbank.com/mongodb-async-driver/userguide/tls.html");
                }
            }
            else {
                setSocketFactory(null);
            }
        }

        try {
            final BeanInfo info = Introspector
                    .getBeanInfo(MongoClientConfiguration.class);
            final PropertyDescriptor[] descriptors = info
                    .getPropertyDescriptors();
            final Map<String, PropertyDescriptor> descriptorsByName = new HashMap<String, PropertyDescriptor>();
            for (final PropertyDescriptor descriptor : descriptors) {
                descriptorsByName
                        .put(descriptor.getName().toLowerCase(Locale.US),
                                descriptor);
            }

            for (final Map.Entry<String, String> param : parameters.entrySet()) {
                final String propName = param.getKey();
                final String propValue = param.getValue();

                final PropertyDescriptor descriptor = descriptorsByName
                        .get(propName);
                if (descriptor != null) {
                    try {
                        updateFieldValue(descriptor, propName, propValue);
                    }
                    catch (final NumberFormatException nfe) {
                        throw new IllegalArgumentException("The '" + propName
                                + "' parameter must have a numeric value not '"
                                + propValue + "'.", nfe);
                    }
                    catch (final Exception e) {
                        throw new IllegalArgumentException("The '" + propName
                                + "' parameter's editor could not be set '"
                                + propValue + "'.", e);
                    }
                }
                else if ("uuidrepresentation".equals(propName)) {
                    LOG.info("Changing the UUID representation is not supported.");
                }
                else if ("replicaset".equals(propName)) {
                    LOG.info("Not validating the replica set name is '{}'.",
                            propValue);
                }
                else {
                    LOG.info("Unknown property '{}' and value '{}'.", propName,
                            propValue);
                }
            }
        }
        catch (final IntrospectionException e) {
            throw new IllegalArgumentException(
                    "Could not introspect on the MongoClientConfiguration.");
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
        final List<Credential> old = new ArrayList<Credential>(
                myCredentials.values());

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

        myPropSupport.firePropertyChange("credentials", old,
                new ArrayList<Credential>(myCredentials.values()));
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
    public void addCredential(final Credential.Builder credentials)
            throws IllegalArgumentException {
        addCredential(credentials.build());
    }

    /**
     * Add a {@link PropertyChangeListener} from the configuration. The listener
     * will receive notification of all changes to the configuration.
     * 
     * @param listener
     *            The {@link PropertyChangeListener} to be added
     */
    public synchronized void addPropertyChangeListener(
            final PropertyChangeListener listener) {
        myPropSupport.addPropertyChangeListener(listener);
    }

    /**
     * Add a {@link PropertyChangeListener} from the configuration. The listener
     * will receive notification of all changes to the configuration's specified
     * property.
     * 
     * @param propertyName
     *            The name of the property to listen on.
     * @param listener
     *            The {@link PropertyChangeListener} to be added
     */

    public synchronized void addPropertyChangeListener(
            final String propertyName, final PropertyChangeListener listener) {
        myPropSupport.addPropertyChangeListener(propertyName, listener);
    }

    /**
     * Adds a server to initially attempt to connect to.
     * 
     * @param server
     *            The server to add.
     */
    public void addServer(final InetSocketAddress server) {
        final List<InetSocketAddress> old = new ArrayList<InetSocketAddress>(
                myServers);

        myServers.add(server);

        myPropSupport.firePropertyChange("servers", old,
                Collections.unmodifiableList(myServers));
    }

    /**
     * Adds a server to initially attempt to connect to.
     * 
     * @param server
     *            The server to add.
     */
    public void addServer(final String server) {
        addServer(ServerNameUtils.parse(server));
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
        if (myLegacyCredential != null) {
            myCredentials.remove(myLegacyCredential.getDatabase());
        }
        myLegacyCredential = Credential.builder().userName(userName)
                .password(password.toCharArray())
                .database(getDefaultDatabase()).mongodbCR().build();

        addCredential(myLegacyCredential);
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

        addCredential(Credential.builder().userName(userName)
                .password(password.toCharArray()).database(ADMIN_DB_NAME)
                .mongodbCR().build());
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

            clone.myCredentials = new ConcurrentHashMap<String, Credential>();
            for (final Credential credential : getCredentials()) {
                clone.addCredential(credential);
            }

            clone.myServers = new ArrayList<InetSocketAddress>();
            for (final InetSocketAddress addr : getServerAddresses()) {
                clone.addServer(addr);
            }

            clone.myPropSupport = new PropertyChangeSupport(clone);
        }
        catch (final CloneNotSupportedException shouldNotHappen) {
            clone = new MongoClientConfiguration(this);
        }
        return clone;
    }

    /**
     * Returns the model the driver uses for managing connections.
     * <p>
     * Defaults to {@link ConnectionModel#RECEIVER_THREAD}.
     * </p>
     * 
     * @return The model used for managing connections.
     */
    public ConnectionModel getConnectionModel() {
        return myConnectionModel;
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
     * Defaults to {@link Durability#ACK}.
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
     * Returns the maximum number of strings that may have their encoded form
     * cached.
     * <p>
     * Defaults to {@value #DEFAULT_MAX_STRING_CACHE_ENTRIES}.
     * </p>
     * <p>
     * Note: The caches are maintained per connection and there is a cache for
     * the encoder and another for the decoder. The results is that caching 25
     * string with 10 connections can result in 500 cache entries (2 * 25 * 10).
     * </p>
     * 
     * @return The maximum number of strings that may have their encoded form
     *         cached.
     */
    public int getMaxCachedStringEntries() {
        return myMaxCachedStringEntries;
    }

    /**
     * Returns the maximum length for a string that the stream is allowed to
     * cache.This can be used to stop a single long string from pushing useful
     * values out of the cache. Setting this value to zero turns off the
     * caching.
     * <p>
     * Defaults to {@value #DEFAULT_MAX_STRING_CACHE_LENGTH}.
     * </p>
     * <p>
     * Note: The caches are maintained per connection and there is a cache for
     * the encoder and another for the decoder. The results is that caching 25
     * string with 10 connections can result in 500 cache entries (2 * 25 * 10).
     * </p>
     * 
     * @return The maximum length for a string that the stream is allowed to
     *         cache.
     */
    public int getMaxCachedStringLength() {
        return myMaxCachedStringLength;
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
     * Returns the number of read timeouts (a tick) before closing the
     * connection.
     * <p>
     * Defaults to {@link Integer#MAX_VALUE}.
     * </p>
     * 
     * @return The number of read timeouts (a tick) before closing the
     *         connection.
     */
    public int getMaxIdleTickCount() {
        return myMaxIdleTickCount;
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
     * Returns the level that the driver logs metrics. The value is based on the
     * {@link Level#intValue()}. Common values include:
     * <ul>
     * <li>FINE/DEBUG - 500
     * <li>INFO - 800
     * <li>WARNING - 900
     * <li>ERROR/SEVERE - 1000
     * </ul>
     * <p>
     * Defaults to DEBUG/500.
     * </p>
     * 
     * @return The level to do a periodic metrics log.
     */
    public int getMetricsLogLevel() {
        return myMetricsLogLevel;
    }

    /**
     * Returns the minimum number of connections to try and keep open.
     * <p>
     * Defaults to 0.
     * </p>
     * 
     * @return The minimum number of connections to try and keep open.
     */
    public int getMinConnectionCount() {
        return myMinConnectionCount;
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
     * Returns true if the driver logs each message that is sent or received by
     * the client. Logged messages will be at the DEBUG level on the logger
     * {@value #MESSAGE_LOGGER_NAME}. {@link #isMetricsEnabled()} must be true
     * for message logging to be supported.
     * <p>
     * <b>WARNING</>: Enabling message logging will have a sever impact on
     * performance.
     * <p>
     * Defaults to false, to not log messages.
     * </p>
     * 
     * @return True if logging of messages is enabled.
     */
    public boolean isLogMessagesEnabled() {
        return myLogMessagesEnabled;
    }

    /**
     * Returns true if the driver collects metrics on the messages it sends and
     * receives to the MongoDB cluster.
     * <p>
     * If enabled and the JVM supports JMX then the metrics will be exposed via
     * JMX. Regardless of JMX support the driver will periodically create a log
     * message containing the current metrics. Logged messages will be at the
     * {@link MongoClientConfiguration#setMetricsLogLevel configured} level
     * using the logger {@value #METRICS_LOGGER_NAME}.
     * <p>
     * Metrics must be enables for message logging to be supported.
     * </p>
     * <p>
     * Defaults to true to collect metrics.
     * </p>
     * <p>
     * This value must be set prior to constructing the {@link MongoClient} and
     * any changes after a {@code MongoClient} is constructed will have no
     * effect for that {@code MongoClient}.
     * </p>
     * 
     * @return True if metrics collection is enabled.
     */
    public boolean isMetricsEnabled() {
        return myMetricsEnabled;
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
     * Removes a {@link PropertyChangeListener} from the configuration. The
     * listener will no longer receive notification of all changes to the
     * configuration.
     * 
     * @param listener
     *            The {@link PropertyChangeListener} to be removed
     */
    public synchronized void removePropertyChangeListener(
            final PropertyChangeListener listener) {
        myPropSupport.removePropertyChangeListener(listener);
    }

    /**
     * Removes a {@link PropertyChangeListener} from the configuration. The
     * listener will no longer receive notification of all changes to the
     * configuration's specified property.
     * 
     * @param propertyName
     *            The name of the property that was listened on.
     * @param listener
     *            The {@link PropertyChangeListener} to be removed
     */

    public synchronized void removePropertyChangeListener(
            final String propertyName, final PropertyChangeListener listener) {
        myPropSupport.removePropertyChangeListener(propertyName, listener);
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
        final boolean old = myAutoDiscoverServers;

        myAutoDiscoverServers = autoDiscoverServers;

        myPropSupport.firePropertyChange("autoDiscoverServers", old,
                myAutoDiscoverServers);
    }

    /**
     * Sets the model the driver uses for managing connections.
     * <p>
     * Defaults to {@link ConnectionModel#RECEIVER_THREAD}.
     * </p>
     * 
     * @param connectionModel
     *            The new value for the model the driver uses for managing
     *            connections.
     */
    public void setConnectionModel(final ConnectionModel connectionModel) {
        final ConnectionModel old = myConnectionModel;

        myConnectionModel = connectionModel;

        myPropSupport.firePropertyChange("connectionModel", old,
                myConnectionModel);
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
        final int old = myConnectTimeout;

        myConnectTimeout = connectTimeout;

        myPropSupport.firePropertyChange("connectTimeout", old,
                myConnectTimeout);
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
        final List<Credential> old = new ArrayList<Credential>(
                myCredentials.values());

        myCredentials.clear();
        for (final Credential credential : credentials) {
            addCredential(credential);
        }

        myPropSupport.firePropertyChange("credentials", old,
                new ArrayList<Credential>(myCredentials.values()));
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
        final String old = myDefaultDatabase;

        myDefaultDatabase = defaultDatabase;

        if (myLegacyCredential != null) {
            final List<Credential> oldCredentials = new ArrayList<Credential>(
                    myCredentials.values());
            myCredentials.remove(myLegacyCredential.getDatabase());
            myPropSupport.firePropertyChange("credentials", oldCredentials,
                    new ArrayList<Credential>(myCredentials.values()));

            myLegacyCredential = Credential.builder()
                    .userName(myLegacyCredential.getUserName())
                    .password(myLegacyCredential.getPassword())
                    .database(defaultDatabase).mongodbCR().build();
            addCredential(myLegacyCredential);
        }

        myPropSupport.firePropertyChange("defaultDatabase", old,
                myDefaultDatabase);
    }

    /**
     * Sets the default durability for write operations on the server to the new
     * value.
     * 
     * @param defaultDurability
     *            The default durability for write operations on the server.
     */
    public void setDefaultDurability(final Durability defaultDurability) {
        final Durability old = myDefaultDurability;

        myDefaultDurability = defaultDurability;

        myPropSupport.firePropertyChange("defaultDurability", old,
                myDefaultDurability);
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
        final ReadPreference old = myDefaultReadPreference;

        if (defaultReadPreference == null) {
            myDefaultReadPreference = ReadPreference.PRIMARY;
        }
        else {
            myDefaultReadPreference = defaultReadPreference;
        }

        myPropSupport.firePropertyChange("defaultReadPreference", old,
                myDefaultReadPreference);
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
        final Executor old = myExecutor;

        myExecutor = executor;

        myPropSupport.firePropertyChange("executor", old, myExecutor);
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
        final LockType old = myLockType;

        myLockType = lockType;

        myPropSupport.firePropertyChange("lockType", old, myLockType);
    }

    /**
     * Sets if the driver logs each message that is sent or received by the
     * client. Logged messages will be at the DEBUG level on the logger
     * {@value #MESSAGE_LOGGER_NAME}. {@link #isMetricsEnabled()} must be true
     * for message logging to be supported.
     * <p>
     * <b>WARNING</>: Enabling message logging will have a sever impact on
     * performance.
     * <p>
     * Defaults to false to not log messages.
     * </p>
     * 
     * @param loggingMessages
     *            The new value for if messages should be logged.
     */
    public void setLogMessagesEnabled(final boolean loggingMessages) {
        final boolean old = myLogMessagesEnabled;

        myLogMessagesEnabled = loggingMessages;

        myPropSupport.firePropertyChange("logMessagesEnabled", old,
                myLogMessagesEnabled);
    }

    /**
     * Sets the value of maximum number of strings that may have their encoded
     * form cached.
     * <p>
     * Defaults to {@value #DEFAULT_MAX_STRING_CACHE_ENTRIES}.
     * </p>
     * <p>
     * Note: The caches are maintained per {@link MongoClient} instance.
     * </p>
     * 
     * @param maxCacheEntries
     *            The new value for the maximum number of strings that may have
     *            their encoded form cached.
     */
    public void setMaxCachedStringEntries(final int maxCacheEntries) {
        final int old = myMaxCachedStringEntries;

        myMaxCachedStringEntries = maxCacheEntries;

        myPropSupport.firePropertyChange("maxCachedStringEntries", old,
                myMaxCachedStringEntries);
    }

    /**
     * Sets the value of length for a string that may be cached. This can be
     * used to stop a single long string from pushing useful values out of the
     * cache. Setting this value to zero turns off the caching.
     * <p>
     * Defaults to {@value #DEFAULT_MAX_STRING_CACHE_LENGTH}.
     * </p>
     * <p>
     * Note: The caches are maintained per {@link MongoClient} instance.
     * </p>
     * 
     * @param maxlength
     *            The new value for the length for a string that the encoder is
     *            allowed to cache.
     */
    public void setMaxCachedStringLength(final int maxlength) {
        final int old = myMaxCachedStringLength;

        myMaxCachedStringLength = maxlength;

        myPropSupport.firePropertyChange("maxCachedStringLength", old,
                myMaxCachedStringLength);
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
        final int old = myMaxConnectionCount;

        myMaxConnectionCount = maxConnectionCount;

        myPropSupport.firePropertyChange("maxConnectionCount", old,
                myMaxConnectionCount);
    }

    /**
     * Sets the value of the number of read timeouts (a tick) before closing the
     * connection.
     * 
     * @param idleTickCount
     *            The new value for the number of read timeouts (a tick) before
     *            closing the connection.
     */
    public void setMaxIdleTickCount(final int idleTickCount) {
        final int old = myMaxIdleTickCount;

        myMaxIdleTickCount = idleTickCount;

        myPropSupport.firePropertyChange("maxIdleTickCount", old,
                myMaxIdleTickCount);
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
        final int old = myMaxPendingOperationsPerConnection;

        myMaxPendingOperationsPerConnection = maxPendingOperationsPerConnection;

        myPropSupport.firePropertyChange("maxPendingOperationsPerConnection",
                old, myMaxPendingOperationsPerConnection);
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
        final long old = myMaxSecondaryLag;

        myMaxSecondaryLag = maxSecondaryLag;

        myPropSupport.firePropertyChange("maxSecondaryLag", Long.valueOf(old),
                Long.valueOf(myMaxSecondaryLag));
    }

    /**
     * Sets if the driver collects metrics on the messages it sends and receives
     * to the MongoDB cluster.
     * <p>
     * If enabled and the JVM supports JMX then the metrics will be exposed via
     * JMX. Regardless of JMX support the driver will periodically create a log
     * message containing the current metrics. Logged messages will be at the
     * {@link MongoClientConfiguration#setMetricsLogLevel configured} level
     * using the logger {@value #METRICS_LOGGER_NAME}.
     * <p>
     * Metrics must be enables for message logging to be supported.
     * </p>
     * <p>
     * Defaults to true to collect metrics.
     * </p>
     * <p>
     * This value must be set prior to constructing the {@link MongoClient} and
     * any changes after a {@code MongoClient} is constructed will have no
     * effect for that {@code MongoClient}.
     * </p>
     * 
     * @param metricsEnabled
     *            The new value for if metrics should be enabled.
     */
    public void setMetricsEnabled(final boolean metricsEnabled) {
        final boolean old = myMetricsEnabled;

        myMetricsEnabled = metricsEnabled;

        myPropSupport.firePropertyChange("metricsEnabled", old,
                myMetricsEnabled);
    }

    /**
     * Sets the level that the driver logs metrics. The value is based on the
     * {@link Level#intValue()}. Common values include:
     * <ul>
     * <li>FINE/DEBUG - 500
     * <li>INFO - 800
     * <li>WARNING - 900
     * <li>ERROR/SEVERE - 1000
     * </ul>
     * <p>
     * Defaults to DEBUG/500.
     * </p>
     * 
     * @param metricsLogLevel
     *            The new value for the level to log periodic metrics.
     */
    public void setMetricsLogLevel(final int metricsLogLevel) {
        final int old = myMetricsLogLevel;

        myMetricsLogLevel = metricsLogLevel;

        myPropSupport.firePropertyChange("metricsLogLevel", old,
                myMetricsLogLevel);
    }

    /**
     * Sets the value of the minimum number of connections to try and keep open.
     * 
     * @param minimumConnectionCount
     *            The new value for the minimum number of connections to try and
     *            keep open.
     */
    public void setMinConnectionCount(final int minimumConnectionCount) {
        final int old = myMinConnectionCount;

        myMinConnectionCount = minimumConnectionCount;

        myPropSupport.firePropertyChange("minConnectionCount", old,
                myMinConnectionCount);
    }

    /**
     * @param readTimeout
     *            The time to wait (in milliseconds) for a socket read to
     *            complete.
     */
    public void setReadTimeout(final int readTimeout) {
        final int old = myReadTimeout;

        myReadTimeout = readTimeout;

        myPropSupport.firePropertyChange("readTimeout", old, myReadTimeout);
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
        final int old = myReconnectTimeout;

        myReconnectTimeout = connectTimeout;

        myPropSupport.firePropertyChange("reconnectTimeout", old,
                myReconnectTimeout);
    }

    /**
     * Sets the servers to initially attempt to connect to.
     * 
     * @param servers
     *            The servers to connect to.
     */
    public void setServers(final List<InetSocketAddress> servers) {
        final List<InetSocketAddress> old = new ArrayList<InetSocketAddress>(
                myServers);

        myServers.clear();
        if (servers != null) {
            for (final InetSocketAddress server : servers) {
                addServer(server);
            }
        }

        myPropSupport.firePropertyChange("servers", old,
                Collections.unmodifiableList(myServers));
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
        final SocketFactory old = mySocketFactory;

        if (socketFactory == null) {
            mySocketFactory = SocketFactory.getDefault();
        }
        else {
            mySocketFactory = socketFactory;
        }

        myPropSupport.firePropertyChange("socketFactory", old, mySocketFactory);
    }

    /**
     * Sets the thread factory for managing connections to the new value.
     * 
     * @param factory
     *            The thread factory for managing connections.
     */
    public void setThreadFactory(final ThreadFactory factory) {
        final ThreadFactory old = myThreadFactory;

        myThreadFactory = factory;

        myPropSupport.firePropertyChange("threadFactory", old, myThreadFactory);
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
        final boolean old = myUsingSoKeepalive;

        myUsingSoKeepalive = usingSoKeepalive;

        myPropSupport.firePropertyChange("usingSoKeepalive", old,
                myUsingSoKeepalive);
    }

    /**
     * Reads the serialized configuration and sets the transient field to known
     * values.
     * 
     * @param stream
     *            The stream to read from.
     * @throws IOException
     *             On a failure reading from the stream.
     * @throws ClassNotFoundException
     *             On a failure locating a type in the stream.
     */
    private void readObject(final java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
        stream.defaultReadObject();

        myExecutor = null;
        mySocketFactory = null;
        myThreadFactory = null;
    }

    /**
     * Updates this configurations value based on the field's descriptor, name
     * and value.
     * 
     * @param descriptor
     *            The field's descriptor.
     * @param propName
     *            The name of the field.
     * @param propValue
     *            The value for the field.
     * @throws IllegalArgumentException
     *             On a number of errors.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void updateFieldValue(final PropertyDescriptor descriptor,
            final String propName, final String propValue)
            throws IllegalArgumentException {

        try {
            final Class<?> fieldType = descriptor.getPropertyType();
            final Method writer = descriptor.getWriteMethod();
            PropertyEditor editor;
            final Class<?> editorClass = descriptor.getPropertyEditorClass();
            if (editorClass != null) {
                editor = (PropertyEditor) editorClass.newInstance();
            }
            else {
                editor = PropertyEditorManager.findEditor(fieldType);
            }

            if (editor != null) {
                final Method reader = descriptor.getReadMethod();
                editor.setValue(reader.invoke(this));
                editor.setAsText(propValue);
                writer.invoke(this, editor.getValue());
            }
            else if (fieldType.isEnum()) {
                final Class<? extends Enum> c = (Class<? extends Enum>) fieldType;
                writer.invoke(this, Enum.valueOf(c, propValue));
            }
            else {
                throw new IllegalArgumentException("The '" + propName
                        + "' parameter could not be set " + "to the value '"
                        + propValue + "'. No editor available.");

            }
        }
        catch (final InstantiationException e) {
            throw new IllegalArgumentException(e);
        }
        catch (final IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
        catch (final InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
