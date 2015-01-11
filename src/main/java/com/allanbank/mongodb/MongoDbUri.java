/*
 * #%L
 * MongoDbUri.java - mongodb-async-driver - Allanbank Consulting, Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.element.UuidElement;

/**
 * MongoDbUri provides the ability to parse a MongoDB URI into fields.
 * <p>
 * The set of possible options is a combination of the <a
 * href="http://docs.mongodb.org/manual/reference/connection-string/">standard
 * MongoDB URI options</a> and the complete set of fields supported by the
 * {@link MongoClientConfiguration} class. The driver uses the Java beans
 * standard to define the label for each field.
 * </p>
 * See the <a
 * href="http://docs.mongodb.org/manual/reference/connection-string/">Connection
 * String</a> documentation for information on the standard options. The
 * following standard options are not supported by the driver:
 * <dl>
 * <dt>replicaSet</dt>
 * <dd>The driver does automatic cluster type discovery so this options is not
 * needed or used.</dd>
 * <dt>maxIdleTimeMS</dt>
 * <dd>The driver does not use a specific wait time. Instead the connection is
 * considered idle when it experiences a defined number of idle ticks. A tick is
 * defined by the {@link MongoClientConfiguration#setReadTimeout(int)
 * readTimeout} and the number of ticks is defined by
 * {@link MongoClientConfiguration#setMaxIdleTickCount(int) maxIdleTickCount}.</dd>
 * <dt>waitQueueMultiple</dt>
 * <dd>The driver does not queue requests waiting for a socket since it is
 * asynchronous. The closest option would be the
 * {@link MongoClientConfiguration#setMaxPendingOperationsPerConnection(int)
 * maxPendingOperationsPerConnection} which can be used to control how
 * aggressively the driver will apply back pressure.</dd>
 * <dt>waitQueueTimeoutMS</dt>
 * <dd>Similar to {@code waitQueueMultiple} this option does not make sense for
 * an asynchronous driver.</dd>
 * <dt>uuidRepresentation</dt>
 * <dd>The UUID representation can only be controlled via construction. See the
 * {@link UuidElement#UuidElement(String, byte, java.util.UUID)} and
 * {@link UuidElement#LEGACY_UUID_SUBTTYPE}.</dd>
 * </dl>
 * <p>
 * </p>
 * <p>
 * The current set of auto-mapped fields is listed below. See the linked
 * documentation for details on the fields values.
 * <ul>
 * <li>{@link MongoClientConfiguration#setAutoDiscoverServers(boolean)
 * autoDiscoverServers}</li>
 * <li>{@link MongoClientConfiguration#setConnectionModel(ConnectionModel)
 * connectionModel}</li>
 * <li>{@link MongoClientConfiguration#setConnectTimeout(int) connectTimeout}</li>
 * <li>{@link MongoClientConfiguration#setLockType(LockType) lockType}</li>
 * <li>{@link MongoClientConfiguration#setMaxCachedStringEntries(int)
 * maxCachedStringEntries}</li>
 * <li>{@link MongoClientConfiguration#setMaxCachedStringLength(int)
 * maxCachedStringLength}</li>
 * <li>{@link MongoClientConfiguration#setMaxConnectionCount(int)
 * maxConnectionCount}</li>
 * <li>{@link MongoClientConfiguration#setMaxIdleTickCount(int)
 * maxIdleTickCount}</li>
 * <li>
 * {@link MongoClientConfiguration#setMaxPendingOperationsPerConnection(int)
 * maxPendingOperationsPerConnection}</li>
 * <li>
 * {@link MongoClientConfiguration#setMaxSecondaryLag(long) maxSecondaryLag}</li>
 * <li>
 * {@link MongoClientConfiguration#setMinConnectionCount(int)
 * minConnectionCount}</li>
 * <li>
 * {@link MongoClientConfiguration#setReadTimeout(int) readTimeout}</li>
 * <li>
 * {@link MongoClientConfiguration#setReconnectTimeout(int) reconnectTimeout}</li>
 * <li>
 * {@link MongoClientConfiguration#setUsingSoKeepalive(boolean)
 * usingSoKeepalive}</li>
 * </ul>
 * </p>
 * <p>
 * Any credentials, default read preference, and default durability will also be
 * determined via the URI. See the {@link CredentialEditor},
 * {@link ReadPreferenceEditor}, and {@link DurabilityEditor} for details
 * </p>
 *
 * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
 *      Connections</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2014, Allanbank Consulting, Inc., All Rights Reserved
 */
@Immutable
@ThreadSafe
public class MongoDbUri {
    /** The prefix for a MongoDB URI. */
    public static final String MONGODB_URI_PREFIX = "mongodb://";

    /** The prefix for a MongoDB URI that uses SSL. */
    public static final String MONGODBS_URI_PREFIX = "mongodbs://";

    /**
     * Tests if the {@code mongoDbUri} starts with the correct prefix to be a
     * MongoDB URI.
     *
     * @param mongoDbUri
     *            The presumed MongoDB URI.
     * @return True if the {@code mongoDbUri} starts with
     *         {@value #MONGODB_URI_PREFIX} of {@value #MONGODBS_URI_PREFIX}.
     */
    public static boolean isUri(final String mongoDbUri) {
        final String lower = mongoDbUri.toLowerCase(Locale.US);

        return lower.startsWith(MONGODB_URI_PREFIX)
                || lower.startsWith(MONGODBS_URI_PREFIX);
    }

    /** The database contained in the URI. */
    private final String myDatabase;

    /** The hosts contained in the URI. */
    private final List<String> myHosts;

    /** The options contained in the URI. */
    private final String myOptions;

    /** The database contained in the URI. */
    private final String myOriginalText;

    /** The password contained in the URI. */
    private final String myPassword;

    /** The user name contained in the URI. */
    private final String myUserName;

    /**
     * Set to true if the URL uses the {@value #MONGODBS_URI_PREFIX} prefix.
     */
    private final boolean myUseSsl;

    /**
     * Creates a new MongoDbUri.
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
    public MongoDbUri(final String mongoDbUri) throws IllegalArgumentException {
        myOriginalText = mongoDbUri;

        String remaining;
        boolean useSsl = false;
        if (mongoDbUri == null) {
            throw new IllegalArgumentException(
                    "The MongoDB URI cannot be null.");
        }
        else if (mongoDbUri.substring(0, MONGODB_URI_PREFIX.length())
                .equalsIgnoreCase(MONGODB_URI_PREFIX)) {
            remaining = mongoDbUri.substring(MONGODB_URI_PREFIX.length());
        }
        else if (mongoDbUri.substring(0, MONGODBS_URI_PREFIX.length())
                .equalsIgnoreCase(MONGODBS_URI_PREFIX)) {
            remaining = mongoDbUri.substring(MONGODBS_URI_PREFIX.length());
            useSsl = true;
        }
        else {
            throw new IllegalArgumentException(
                    "The MongoDB URI must start with '" + MONGODB_URI_PREFIX
                            + "'.");
        }

        String userNamePassword;
        String hosts;

        int position = remaining.indexOf('@');
        if (position >= 0) {
            userNamePassword = remaining.substring(0, position);
            remaining = remaining.substring(position + 1);
        }
        else {
            userNamePassword = "";
        }

        // Figure out the DB - if any.
        position = remaining.indexOf('/');
        if (position >= 0) {
            hosts = remaining.substring(0, position);
            remaining = remaining.substring(position + 1);

            position = remaining.indexOf('?');
            if (position >= 0) {
                myDatabase = remaining.substring(0, position);
                myOptions = remaining.substring(position + 1);
            }
            else {
                myDatabase = remaining;
                myOptions = "";
            }
        }
        else {
            myDatabase = "";
            position = remaining.indexOf('?');
            if (position >= 0) {
                hosts = remaining.substring(0, position);
                myOptions = remaining.substring(position + 1);
            }
            else {
                hosts = remaining;
                myOptions = "";
            }
        }

        // Now the hosts.
        final StringTokenizer tokenizer = new StringTokenizer(hosts, ",");
        myHosts = new ArrayList<String>(tokenizer.countTokens());
        while (tokenizer.hasMoreTokens()) {
            myHosts.add(tokenizer.nextToken());
        }
        if (myHosts.isEmpty()) {
            throw new IllegalArgumentException(
                    "Must provide at least 1 host to connect to.");
        }

        // User name and password?
        if (!userNamePassword.isEmpty()) {
            position = userNamePassword.indexOf(':');
            if (position >= 0) {
                myUserName = userNamePassword.substring(0, position);
                myPassword = userNamePassword.substring(position + 1);
            }
            else {
                throw new IllegalArgumentException(
                        "The password for the user '" + userNamePassword
                                + "' must be provided.");
            }
        }
        else {
            myUserName = null;
            myPassword = null;
        }

        myUseSsl = useSsl;
    }

    /**
     * Returns the database contained in the URI.
     *
     * @return The database contained in the URI.
     */
    public String getDatabase() {
        return myDatabase;
    }

    /**
     * Returns the hosts contained in the URI.
     *
     * @return The hosts contained in the URI.
     */
    public List<String> getHosts() {
        return Collections.unmodifiableList(myHosts);
    }

    /**
     * Returns the options contained in the URI. Will never be null bu may be an
     * empty string.
     *
     * @return The options contained in the URI.
     */
    public String getOptions() {
        return myOptions;
    }

    /**
     * Returns the options contained in the URI parsed into a map of token
     * values.
     *
     * @return The options contained in the URI.
     */
    public Map<String, String> getParsedOptions() {
        final Map<String, String> parsedOptions = new LinkedHashMap<String, String>();
        final StringTokenizer tokenizer = new StringTokenizer(getOptions(),
                "?;&");
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

            // Normalize the names..
            property = property.toLowerCase(Locale.US);

            // Put the option at the end.
            parsedOptions.remove(property);
            parsedOptions.put(property, value);
        }

        return parsedOptions;
    }

    /**
     * Returns the password contained in the URI. May be <code>null</code>.
     *
     * @return The password contained in the URI.
     */
    public String getPassword() {
        return myPassword;
    }

    /**
     * Returns the user name contained in the URI. May be <code>null</code>.
     *
     * @return The user name contained in the URI.
     */
    public String getUserName() {
        return myUserName;
    }

    /**
     * Returns the values for a single property. This allows for duplicate
     * values.
     *
     * @param field
     *            The field to return all values for.
     * @return The options contained in the URI.
     */
    public List<String> getValuesFor(final String field) {
        final List<String> values = new ArrayList<String>();
        final StringTokenizer tokenizer = new StringTokenizer(getOptions(),
                "?;&");
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

            if (field.equalsIgnoreCase(property)) {
                values.add(value);
            }
        }

        return values;
    }

    /**
     * Returns true if the URL uses the {@value #MONGODBS_URI_PREFIX} prefix.
     *
     * @return True if the URL uses the {@value #MONGODBS_URI_PREFIX} prefix.
     */
    public boolean isUseSsl() {
        return myUseSsl;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the string used to construct the URI.
     * </p>
     */
    @Override
    public String toString() {
        return myOriginalText;
    }

}
