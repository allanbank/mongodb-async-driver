/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/**
 * MongoDbUri provides the ability to parse a MongoDB URI into fields.
 * 
 * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
 *      Connections</a>
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoDbUri {
    /** The prefix for a MongoDB URI. */
    public static final String MONGODB_URI_PREFIX = "mongodb://";

    /** The database contained in the URI. */
    private final String myDatabase;

    /** The hosts contained in the URI. */
    private final List<String> myHosts;

    /** The options contained in the URI. */
    private final String myOptions;

    /** The password contained in the URI. */
    private final String myPassword;

    /** The user name contained in the URI. */
    private final String myUserName;

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
        if (mongoDbUri == null) {
            throw new IllegalArgumentException(
                    "The MongoDB URI cannot be null.");
        }
        else if (!mongoDbUri.substring(0, MONGODB_URI_PREFIX.length())
                .equalsIgnoreCase(MONGODB_URI_PREFIX)) {
            throw new IllegalArgumentException(
                    "The MongoDB URI must start with '" + MONGODB_URI_PREFIX
                            + "'.");
        }

        String remaining = mongoDbUri.substring(MONGODB_URI_PREFIX.length());
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

}
