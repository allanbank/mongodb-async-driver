/*
 * Copyright 2012, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

/**
 * GeospatialOperator provides the enumeration of geospatial operators.
 * 
 * @api.yes This enumeration is part of the driver's API. Public and protected
 *          members will be deprecated for at least 1 non-bugfix release
 *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
 *          before being removed or modified.
 * @copyright 2012, Allanbank Consulting, Inc., All Rights Reserved
 */
public enum GeospatialOperator implements Operator {

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
     */
    public static final String UNIQUE_DOCS_MODIFIER = "$uniqueDocs";

    /** The operator's token to use when sending to the server. */
    private final String myToken;

    /**
     * Creates a new GeospatialOperator.
     * 
     * @param token
     *            The token to use when sending to the server.
     */
    private GeospatialOperator(final String token) {
        myToken = token;
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
}
