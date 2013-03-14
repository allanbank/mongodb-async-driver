/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.util.Assertions.assertNotEmpty;
import static com.allanbank.mongodb.util.Assertions.assertNotNull;

import java.awt.geom.Point2D;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * AggregationGeoNear provides the options for the {@code $geoNear} pipeline
 * stage of an aggregation.
 * 
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 * 
 * @since MongoDB 2.4
 */
public class AggregationGeoNear implements DocumentAssignable {

    /**
     * Creates a new builder for an {@link AggregationGeoNear}.
     * 
     * @return The builder to construct an {@link AggregationGeoNear}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The name of the field to place the distance from the source
     * {@link #getLocation() location}.
     */
    private final String myDistanceField;

    /**
     * The distance multiplier to use in the {@code $geoNear}, if set.
     * <code>null</code> otherwise.
     */
    private final Double myDistanceMultiplier;

    /**
     * The maximum number of documents to return, if set. <code>null</code>
     * otherwise.
     */
    private final Long myLimit;

    /** The location to find documents near. */
    private final Point2D myLocation;

    /**
     * The name of the field to place the location information from the
     * document, if set. <code>null</code> otherwise.
     */
    private final String myLocationField;

    /**
     * The maximum distance to return documents from the specified location, if
     * set. <code>null</code> otherwise.
     */
    private final Double myMaxDistance;

    /**
     * The optional query for further refining the documents to add to the
     * pipeline.
     */
    private final Document myQuery;

    /**
     * If true the {@code $geoNear} should compute distances using spherical
     * coordinates instead of planar coordinates. Defaults to false.
     */
    private final boolean mySpherical;

    /**
     * If true the {@code $geoNear} should only return documents once. Defaults
     * to true.
     */
    private final boolean myUniqueDocs;

    /**
     * Creates a new AggregationGeoNear.
     * 
     * @param builder
     *            he builder for the AggregationGeoNear stage.
     * @throws IllegalArgumentException
     *             If the {@link #getLocation() location} or
     *             {@link #getDistanceField() distance field} have not been set.
     */
    protected AggregationGeoNear(final Builder builder)
            throws IllegalArgumentException {

        assertNotNull(builder.myLocation, "You must specify a location for "
                + "a geoNear in an aggregation pipeline.");
        assertNotEmpty(builder.myDistanceField,
                "You must specify a distance field locations for "
                        + "a geoNear in an aggregation pipeline.");

        myDistanceField = builder.myDistanceField;
        myDistanceMultiplier = builder.myDistanceMultiplier;
        myLocationField = builder.myLocationField;
        myLimit = builder.myLimit;
        myLocation = builder.myLocation;
        myMaxDistance = builder.myMaxDistance;
        myQuery = builder.myQuery;
        mySpherical = builder.mySpherical;
        myUniqueDocs = builder.myUniqueDocs;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return the $geoNear aggregation pipeline's options
     * document. This does not include the $geoNear operator, just the options
     * document.
     * </p>
     */
    @Override
    public Document asDocument() {
        final DocumentBuilder builder = BuilderFactory.start();

        GeoJson.addRaw(builder.pushArray("near"), myLocation);
        builder.add("distanceField", myDistanceField);
        builder.add("spherical", mySpherical);
        builder.add("uniqueDocs", myUniqueDocs);

        if (myLimit != null) {
            builder.add("limit", myLimit.longValue());
        }
        if (myMaxDistance != null) {
            builder.add("maxDistance", myMaxDistance);
        }
        if (myQuery != null) {
            builder.add("query", myQuery);
        }
        if (myDistanceMultiplier != null) {
            builder.add("distanceMultiplier",
                    myDistanceMultiplier.doubleValue());
        }
        if (myLocationField != null) {
            builder.add("includeLocs", myLocationField);
        }

        return builder.build();
    }

    /**
     * Returns the name of the field to place the distance from the source
     * {@link #getLocation() location}.
     * 
     * @return The name of the field to place the distance from the source
     *         {@link #getLocation() location}.
     */
    public String getDistanceField() {
        return myDistanceField;
    }

    /**
     * If set returns the distance multiplier to use in the {@code $geoNear}.
     * 
     * @return The distance multiplier to use in the {@code $geoNear}, if set.
     *         <code>null</code> otherwise.
     */
    public Double getDistanceMultiplier() {
        return myDistanceMultiplier;
    }

    /**
     * If set returns the maximum number of documents to return.
     * 
     * @return The maximum number of documents to return, if set.
     *         <code>null</code> otherwise.
     */
    public Long getLimit() {
        return myLimit;
    }

    /**
     * Returns the location to find documents near.
     * 
     * @return The location to find documents near.
     */
    public Point2D getLocation() {
        return myLocation;
    }

    /**
     * If set returns the name of the field to place the location information
     * from the document.
     * 
     * @return The name of the field to place the location information from the
     *         document, if set. <code>null</code> otherwise.
     */
    public String getLocationField() {
        return myLocationField;
    }

    /**
     * If set returns the maximum distance to return documents from the
     * specified location
     * 
     * @return The maximum distance to return documents from the specified
     *         location, if set. <code>null</code> otherwise.
     */
    public Double getMaxDistance() {
        return myMaxDistance;
    }

    /**
     * If set returns the optional query for further refining the documents to
     * add to the pipeline.
     * 
     * @return The optional query for further refining the documents to add to
     *         the pipeline, if set. <code>null</code> otherwise.
     */
    public Document getQuery() {
        return myQuery;
    }

    /**
     * Returns true if the {@code $geoNear} should compute distances using
     * spherical coordinates instead of planar coordinates. Defaults to false.
     * 
     * @return True if the {@code $geoNear} should compute distances using
     *         spherical coordinates instead of planar coordinates.
     */
    public boolean isSpherical() {
        return mySpherical;
    }

    /**
     * Returns true if the {@code $geoNear} should only return documents once.
     * Defaults to true.
     * 
     * @return True if the {@code $geoNear} should only return documents once.
     */
    public boolean isUniqueDocs() {
        return myUniqueDocs;
    }

    /**
     * Helper for creating immutable {@link Find} queries.
     * 
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static final class Builder {
        /**
         * The name of the field to place the distance from the source
         * {@link #getLocation() location}.
         */
        protected String myDistanceField;

        /**
         * The distance multiplier to use in the {@code $geoNear}, if set.
         * <code>null</code> otherwise.
         */
        protected Double myDistanceMultiplier;

        /**
         * The maximum number of documents to return, if set. <code>null</code>
         * otherwise.
         */
        protected Long myLimit;

        /** The location to find documents near. */
        protected Point2D myLocation;

        /**
         * The name of the field to place the location information from the
         * document, if set. <code>null</code> otherwise.
         */
        protected String myLocationField;

        /**
         * The maximum distance to return documents from the specified location,
         * if set. <code>null</code> otherwise.
         */
        protected Double myMaxDistance;

        /**
         * The optional query for further refining the documents to add to the
         * pipeline.
         */
        protected Document myQuery;

        /**
         * If true the {@code $geoNear} should compute distances using spherical
         * coordinates instead of planar coordinates. Defaults to false.
         */
        protected boolean mySpherical;

        /**
         * If true the {@code $geoNear} should only return documents once.
         * Defaults to true.
         */
        protected boolean myUniqueDocs;

        /**
         * Creates a new Builder.
         */
        public Builder() {
            reset();
        }

        /**
         * Constructs a new {@link AggregationGeoNear} object from the state of
         * the builder.
         * 
         * @return The new {@link AggregationGeoNear} object.
         * @throws IllegalArgumentException
         *             If the {@link #setLocation(Point2D) location} or
         *             {@link #setDistanceField(String) distance field} have not
         *             been set.
         */
        public AggregationGeoNear build() {
            return new AggregationGeoNear(this);
        }

        /**
         * Sets the name of the field to place the distance from the source
         * {@link #getLocation() location}.
         * <p>
         * This method delegates to {@link #setDistanceField(String)}.
         * </p>
         * 
         * @param distanceField
         *            The new name of the field to place the distance from the
         *            source {@link #setLocation(Point2D) location}.
         * @return This builder for chaining method calls.
         */
        public Builder distanceField(final String distanceField) {
            return setDistanceField(distanceField);
        }

        /**
         * Sets the distance multiplier to use in the {@code $geoNear}.
         * <p>
         * This method delegates to {@link #setDistanceMultiplier(double)}.
         * </p>
         * 
         * @param distanceMultiplier
         *            The new distance multiplier to use in the {@code $geoNear}
         *            .
         * @return This builder for chaining method calls.
         */
        public Builder distanceMultiplier(final double distanceMultiplier) {
            return setDistanceMultiplier(distanceMultiplier);
        }

        /**
         * Sets the maximum number of documents to return.
         * <p>
         * This method delegates to {@link #setLimit(long)}.
         * </p>
         * 
         * @param limit
         *            The new maximum number of documents to return.
         * @return This builder for chaining method calls.
         */
        public Builder limit(final long limit) {
            return setLimit(limit);
        }

        /**
         * Sets the location to find documents near.
         * <p>
         * This method delegates to {@link #setLocation(Point2D)}.
         * </p>
         * 
         * @param location
         *            The new location to find documents near.
         * @return This builder for chaining method calls.
         * @see GeoJson#p
         */
        public Builder location(final Point2D location) {
            return setLocation(location);
        }

        /**
         * Sets the name of the field to place the location information from the
         * document.
         * <p>
         * This method delegates to {@link #setLocationField(String)}.
         * </p>
         * 
         * @param locationField
         *            The new name of the field to place the location
         *            information from the document.
         * @return This builder for chaining method calls.
         */
        public Builder locationField(final String locationField) {
            return setLocationField(locationField);
        }

        /**
         * Sets the maximum distance to return documents from the specified
         * location.
         * <p>
         * This method delegates to {@link #setMaxDistance(double)}.
         * </p>
         * 
         * @param maxDistance
         *            The new maximum distance to return documents from the
         *            specified location.
         * @return This builder for chaining method calls.
         */
        public Builder maxDistance(final double maxDistance) {
            return setMaxDistance(maxDistance);
        }

        /**
         * Sets the optional query for further refining the documents to add to
         * the pipeline.
         * <p>
         * This method delegates to {@link #setQuery(DocumentAssignable)}.
         * </p>
         * 
         * @param query
         *            The new optional query for further refining the documents
         *            to add to the pipeline.
         * @return This builder for chaining method calls.
         */
        public Builder query(final DocumentAssignable query) {
            return setQuery(query);
        }

        /**
         * Resets the builder back to its initial state for reuse.
         * 
         * @return This builder for chaining method calls.
         */
        public Builder reset() {
            myDistanceField = null;
            myDistanceMultiplier = null;
            myLimit = null;
            myLocation = null;
            myLocationField = null;
            myMaxDistance = null;
            myQuery = null;
            mySpherical = false;
            myUniqueDocs = true;
            return this;
        }

        /**
         * Sets the name of the field to place the distance from the source
         * {@link #getLocation() location}.
         * 
         * @param distanceField
         *            The new name of the field to place the distance from the
         *            source {@link #setLocation(Point2D) location}.
         * @return This builder for chaining method calls.
         */
        public Builder setDistanceField(final String distanceField) {
            myDistanceField = distanceField;
            return this;
        }

        /**
         * Sets the distance multiplier to use in the {@code $geoNear}.
         * 
         * @param distanceMultiplier
         *            The new distance multiplier to use in the {@code $geoNear}
         *            .
         * @return This builder for chaining method calls.
         */
        public Builder setDistanceMultiplier(final double distanceMultiplier) {
            myDistanceMultiplier = Double.valueOf(distanceMultiplier);
            return this;
        }

        /**
         * Sets the maximum number of documents to return.
         * 
         * @param limit
         *            The new maximum number of documents to return.
         * @return This builder for chaining method calls.
         */
        public Builder setLimit(final long limit) {
            myLimit = Long.valueOf(limit);
            return this;
        }

        /**
         * Sets the location to find documents near.
         * 
         * @param location
         *            The new location to find documents near.
         * @return This builder for chaining method calls.
         * @see GeoJson#p
         */
        public Builder setLocation(final Point2D location) {
            myLocation = location;
            return this;
        }

        /**
         * Sets the name of the field to place the location information from the
         * document.
         * 
         * @param locationField
         *            The new name of the field to place the location
         *            information from the document.
         * @return This builder for chaining method calls.
         */
        public Builder setLocationField(final String locationField) {
            myLocationField = locationField;
            return this;
        }

        /**
         * Sets the maximum distance to return documents from the specified
         * location.
         * 
         * @param maxDistance
         *            The new maximum distance to return documents from the
         *            specified location.
         * @return This builder for chaining method calls.
         */
        public Builder setMaxDistance(final double maxDistance) {
            myMaxDistance = Double.valueOf(maxDistance);
            return this;
        }

        /**
         * Sets the optional query for further refining the documents to add to
         * the pipeline.
         * 
         * @param query
         *            The new optional query for further refining the documents
         *            to add to the pipeline.
         * @return This builder for chaining method calls.
         */
        public Builder setQuery(final DocumentAssignable query) {
            if (query != null) {
                myQuery = query.asDocument();
            }
            else {
                myQuery = null;
            }
            return this;
        }

        /**
         * Sets if (true) the {@code $geoNear} should compute distances using
         * spherical coordinates instead of planar coordinates. Defaults to
         * false.
         * 
         * @param spherical
         *            The new value for if the {@code $geoNear} should compute
         *            distances using spherical coordinates instead of planar
         *            coordinates
         * @return This builder for chaining method calls.
         */
        public Builder setSpherical(final boolean spherical) {
            mySpherical = spherical;
            return this;
        }

        /**
         * Sets if (true) the {@code $geoNear} should only return documents
         * once. Defaults to true.
         * 
         * @param uniqueDocs
         *            The new value for if the {@code $geoNear} should only
         *            return documents once.
         * @return This builder for chaining method calls.
         */
        public Builder setUniqueDocs(final boolean uniqueDocs) {
            myUniqueDocs = uniqueDocs;
            return this;
        }

        /**
         * Sets the {@code $geoNear} to compute distances using spherical
         * coordinates instead of planar coordinates.
         * <p>
         * This method delegates to {@link #setSpherical(boolean)
         * setSpherical(true)}.
         * </p>
         * 
         * 
         * @return This builder for chaining method calls.
         */
        public Builder spherical() {
            return setSpherical(true);
        }

        /**
         * Sets if (true) the {@code $geoNear} should compute distances using
         * spherical coordinates instead of planar coordinates. Defaults to
         * false.
         * <p>
         * This method delegates to {@link #setSpherical(boolean)}.
         * </p>
         * 
         * @param spherical
         *            The new value for if the {@code $geoNear} should compute
         *            distances using spherical coordinates instead of planar
         *            coordinates
         * @return This builder for chaining method calls.
         */
        public Builder spherical(final boolean spherical) {
            return setSpherical(spherical);
        }

        /**
         * Sets if (true) the {@code $geoNear} should only return documents
         * once. Defaults to true.
         * <p>
         * This method delegates to {@link #setUniqueDocs(boolean)}.
         * </p>
         * 
         * @param uniqueDocs
         *            The new value for if the {@code $geoNear} should only
         *            return documents once.
         * @return This builder for chaining method calls.
         */
        public Builder uniqueDocs(final boolean uniqueDocs) {
            return setUniqueDocs(uniqueDocs);
        }
    }
}
