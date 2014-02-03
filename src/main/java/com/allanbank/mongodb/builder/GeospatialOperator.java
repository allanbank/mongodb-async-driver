/*
 * Copyright 2012-2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import com.allanbank.mongodb.Version;

/**
 * GeospatialOperator provides the enumeration of geospatial operators.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum GeospatialOperator implements Operator {

    /**
     * Operator to return documents that are within a GeoJSON shape.
     * 
     * @since MongoDB 2.4
     */
    GEO_WITHIN("$geoWithin", Version.VERSION_2_4),

    /**
     * Operator to return documents that intersect the GeoJSON shape.
     * 
     * @since MongoDB 2.4
     */
    INTERSECT("$geoIntersects", Version.VERSION_2_4),

    /**
     * The modifier for the {@link #NEAR} operator to limit the documents
     * returned based on their distance from the the center point.
     */
    MAX_DISTANCE_MODIFIER("$maxDistance"),

    /** Operator to return documents near a given point. */
    NEAR("$near"),

    /** Operator to return documents near a given point. */
    NEAR_SPHERE("$nearSphere"),

    /** Operator to return documents within a bounding shape. */
    WITHIN("$within");

    /** The name for the rectangular region with a {@link #WITHIN} query. */
    public static final String BOX = "$box";

    /** The name for the circular region with a {@link #WITHIN} query. */
    public static final String CIRCLE = "$center";

    /** The name for the GeoJSON region with a {@link #INTERSECT} query. */
    public static final String GEOMETRY = "$geometry";

    /** The name for the polygon region with a {@link #WITHIN} query. */
    public static final String POLYGON = "$polygon";

    /**
     * The name for the circular region on a sphere with a {@link #WITHIN}
     * query.
     */
    public static final String SPHERICAL_CIRCLE = "$centerSphere";

    /**
     * The modifier for the {@link #WITHIN} operator to determine if duplicate
     * documents should be returned.
     * 
     * @deprecated Support for {@value} was removed in MongoDB 2.6.
     */
    @Deprecated
    public static final String UNIQUE_DOCS_MODIFIER = "$uniqueDocs";

    /**
     * The version (2.5) of the MongoDB server that removed support for the
     * {@value #UNIQUE_DOCS_MODIFIER} modifier.
     */
    public static final Version UNIQUE_DOCS_REMOVED_VERSION = Version
            .parse("2.5");

    /** The operator's token to use when sending to the server. */
    private final String myToken;

    /** The first MongoDB version to support the operator. */
    private final Version myVersion;

    /**
     * Creates a new GeospatialOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     */
    private GeospatialOperator(final String token) {
        this(token, null);
    }

    /**
     * Creates a new GeospatialOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     * @param version
     *            The first MongoDB version to support the operator.
     */
    private GeospatialOperator(final String token, final Version version) {
        myToken = token;
        myVersion = version;
    }

    /**
     * The token for the operator that can be sent to the server.
     * 
     * @return The token for the operator.
     */
    @Override
    public String getToken() {
        return myToken;
    }

    /**
     * Returns the first MongoDB version to support the operator.
     * 
     * @return The first MongoDB version to support the operator. If
     *         <code>null</code> then the version is not known and can be
     *         assumed to be all currently supported versions.
     */
    @Override
    public Version getVersion() {
        return myVersion;
    }
}
