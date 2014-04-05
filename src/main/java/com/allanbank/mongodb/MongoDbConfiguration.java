/*
 * Copyright 2011-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import java.net.InetSocketAddress;

/**
 * Contains the configuration for the connection(s) to the MongoDB servers. This
 * class is the same as and extends the {@link MongoClientConfiguration} except
 * it defaults the {@link Durability} to {@link Durability#NONE} instead of
 * {@link Durability#ACK}.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @deprecated Please us the {@link MongoClientConfiguration} instead. This
 *             class will be removed on or after the 1.3.0 release.
 * @copyright 2011-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@Deprecated
public class MongoDbConfiguration extends MongoClientConfiguration {

    /** Serialization version for the class. */
    private static final long serialVersionUID = -3986785427935492378L;

    /**
     * Creates a new MongoDbConfiguration.
     */
    public MongoDbConfiguration() {
        super();

        setDefaultDurability(Durability.NONE);
    }

    /**
     * Creates a new MongoDbConfiguration.
     *
     * @param servers
     *            The initial set of servers to connect to.
     */
    public MongoDbConfiguration(final InetSocketAddress... servers) {
        super(servers);

        setDefaultDurability(Durability.NONE);
    }

    /**
     * Creates a new MongoDbConfiguration.
     *
     * @param other
     *            The configuration to copy.
     */
    public MongoDbConfiguration(final MongoDbConfiguration other) {
        super(other);
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
        super(mongoDbUri, Durability.NONE);
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
     * Creates a copy of this MongoClientConfiguration.
     * <p>
     * Note: This is not a traditional clone to ensure a deep copy of all
     * information.
     * </p>
     */
    @Override
    public MongoDbConfiguration clone() {
        return (MongoDbConfiguration) super.clone();
    }
}
