/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */
package com.allanbank.mongodb;

import com.allanbank.mongodb.client.MongoClientImpl;

/**
 * MongoFactory provides the bootstrap point for creating a connection
 * represented via a {@link Mongo} instance) to a MongoDB cluster. Both explicit
 * construction with a pre-instantiated {@link MongoClientConfiguration} and via
 * a MongoDB URI are supported.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class MongoFactory {

    /**
     * Creates a new {@link Mongo} instance using the
     * {@link MongoClientConfiguration}.
     * 
     * @param config
     *            The configuration for the connection to MongoDB.
     * @return The {@link Mongo} representation of the connections to MongoDB.
     * @deprecated Use the {@link #createClient(MongoClientConfiguration)}
     *             instead. This method will be removed on or after the 1.3.0
     *             release.
     */
    @Deprecated
    public static Mongo create(final MongoDbConfiguration config) {
        return new com.allanbank.mongodb.client.MongoImpl(config);
    }

    /**
     * Creates a new {@link Mongo} instance using a MongoDB style URL.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @return The {@link Mongo} representation of the connections to MongoDB.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     * @deprecated Use the {@link #createClient(MongoDbUri)} instead. This
     *             method will be removed on or after the 1.3.0 release.
     */
    @Deprecated
    public static Mongo create(final MongoDbUri mongoDbUri) {
        return create(new MongoDbConfiguration(mongoDbUri));
    }

    /**
     * Creates a new {@link Mongo} instance using a MongoDB style URL.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @return The {@link Mongo} representation of the connections to MongoDB.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     * @deprecated Use the {@link #createClient(String)} instead. This method
     *             will be removed on or after the 1.3.0 release.
     */
    @Deprecated
    public static Mongo create(final String mongoDbUri) {
        return create(new MongoDbUri(mongoDbUri));
    }

    /**
     * Creates a new {@link Mongo} instance using the
     * {@link MongoClientConfiguration}.
     * 
     * @param config
     *            The configuration for the connection to MongoDB.
     * @return The {@link Mongo} representation of the connections to MongoDB.
     */
    public static MongoClient createClient(final MongoClientConfiguration config) {
        return new MongoClientImpl(config);
    }

    /**
     * Creates a new {@link Mongo} instance using a MongoDB style URL.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @return The {@link Mongo} representation of the connections to MongoDB.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     */
    public static MongoClient createClient(final MongoDbUri mongoDbUri) {
        return createClient(new MongoClientConfiguration(mongoDbUri));
    }

    /**
     * Creates a new {@link Mongo} instance using a MongoDB style URL.
     * 
     * @param mongoDbUri
     *            The configuration for the connection to MongoDB expressed as a
     *            MongoDB URL.
     * @return The {@link Mongo} representation of the connections to MongoDB.
     * 
     * @see <a href="http://www.mongodb.org/display/DOCS/Connections"> MongoDB
     *      Connections</a>
     */
    public static MongoClient createClient(final String mongoDbUri) {
        return createClient(new MongoDbUri(mongoDbUri));
    }

    /**
     * Stop creation of a new MongoFactory.
     */
    private MongoFactory() {
        super();
    }
}
