/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.awt.Point;
import java.awt.geom.Point2D;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * AggregationGeoNearTest provides tests for the {@link AggregationGeoNear}
 * class.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@SuppressWarnings("boxing")
public class AggregationGeoNearTest {

    /**
     * Test method for {@link AggregationGeoNear}.
     */
    @Test
    public void testAggregationGeoNearFull() {
        final Point2D location = new Point(1, 2);

        final AggregationGeoNear geoNear = AggregationGeoNear.builder()
                .location(location).distanceField("dist")
                .distanceMultiplier(1.2).limit(123).locationField("loc")
                .maxDistance(12345.6).query(BuilderFactory.start().add("a", 2))
                .spherical().uniqueDocs(false).build();

        assertThat(geoNear.getLocation(), is(location));
        assertThat(geoNear.getDistanceField(), is("dist"));
        assertThat(geoNear.getDistanceMultiplier(), is(1.2));
        assertThat(geoNear.getLimit(), is(123L));
        assertThat(geoNear.getLocationField(), is("loc"));
        assertThat(geoNear.getMaxDistance(), is(12345.6));
        assertThat(geoNear.getQuery(), is(BuilderFactory.start().add("a", 2)
                .build()));
        assertThat(geoNear.isSpherical(), is(true));
        assertThat(geoNear.isUniqueDocs(), is(false));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.pushArray("near").add(1).add(2);
        expected.add("distanceField", "dist");
        expected.add("spherical", true);
        expected.add("uniqueDocs", false);

        expected.add("limit", 123L);
        expected.add("maxDistance", 12345.6);
        expected.add("query", BuilderFactory.start().add("a", 2));
        expected.add("distanceMultiplier", 1.2);
        expected.add("includeLocs", "loc");

        assertThat(geoNear.asDocument(), is(expected.build()));
    }

    /**
     * Test method for {@link AggregationGeoNear}.
     */
    @Test
    public void testAggregationGeoNearMinimal() {
        final Point2D location = new Point(1, 2);

        final AggregationGeoNear geoNear = AggregationGeoNear.builder()
                .location(location).distanceField("dist").build();

        assertThat(geoNear.getLocation(), is(location));
        assertThat(geoNear.getDistanceField(), is("dist"));
        assertThat(geoNear.getDistanceMultiplier(), nullValue());
        assertThat(geoNear.getLimit(), nullValue());
        assertThat(geoNear.getLocationField(), nullValue());
        assertThat(geoNear.getMaxDistance(), nullValue());
        assertThat(geoNear.getQuery(), nullValue());
        assertThat(geoNear.isSpherical(), is(false));
        assertThat(geoNear.isUniqueDocs(), is(true));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.pushArray("near").add(1).add(2);
        expected.add("distanceField", "dist");
        expected.add("spherical", false);
        expected.add("uniqueDocs", true);

        assertThat(geoNear.asDocument(), is(expected.build()));
    }

    /**
     * Test method for {@link AggregationGeoNear} .
     */
    @Test()
    public void testAggregationGeoNearMissingDistanceField() {
        final Point2D location = new Point(1, 2);

        final AggregationGeoNear.Builder builder = AggregationGeoNear.builder()
                .location(location).distanceField(null);

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a AggregationGeoNear command without a distance field.",
                built);
    }

    /**
     * Test method for {@link AggregationGeoNear} .
     */
    @Test()
    public void testAggregationGeoNearMissingLocation() {

        final AggregationGeoNear.Builder builder = AggregationGeoNear.builder()
                .location(null).distanceField("dist");

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a AggregationGeoNear command without a location.",
                built);
    }

    /**
     * Test method for {@link AggregationGeoNear}.
     */
    @Test
    public void testAggregationGeoNearReset() {
        final Point2D location = new Point(1, 2);

        final AggregationGeoNear.Builder builder = AggregationGeoNear.builder()
                .location(location).distanceField("dist")
                .distanceMultiplier(1.2).limit(123).locationField("loc")
                .maxDistance(12345.6).query(BuilderFactory.start().add("a", 2))
                .spherical(true).uniqueDocs(false);
        AggregationGeoNear geoNear = builder.build();

        assertThat(geoNear.getLocation(), is(location));
        assertThat(geoNear.getDistanceField(), is("dist"));
        assertThat(geoNear.getDistanceMultiplier(), is(1.2));
        assertThat(geoNear.getLimit(), is(123L));
        assertThat(geoNear.getLocationField(), is("loc"));
        assertThat(geoNear.getMaxDistance(), is(12345.6));
        assertThat(geoNear.getQuery(), is(BuilderFactory.start().add("a", 2)
                .build()));
        assertThat(geoNear.isSpherical(), is(true));
        assertThat(geoNear.isUniqueDocs(), is(false));

        builder.reset();

        boolean built = false;
        try {
            builder.build();
            built = true;
        }
        catch (final IllegalArgumentException expected) {
            // Good.
        }
        assertFalse(
                "Should have failed to create a geoNear command without a location.",
                built);

        geoNear = builder.location(location).distanceField("dist").build();

        assertThat(geoNear.getLocation(), is(location));
        assertThat(geoNear.getDistanceField(), is("dist"));
        assertThat(geoNear.getDistanceMultiplier(), nullValue());
        assertThat(geoNear.getLimit(), nullValue());
        assertThat(geoNear.getLocationField(), nullValue());
        assertThat(geoNear.getMaxDistance(), nullValue());
        assertThat(geoNear.getQuery(), nullValue());
        assertThat(geoNear.isSpherical(), is(false));
        assertThat(geoNear.isUniqueDocs(), is(true));
    }

    /**
     * Test method for {@link AggregationGeoNear}.
     */
    @Test
    public void testAggregationGeoNearWithExplicitSpherical() {
        final Point2D location = new Point(1, 2);

        final AggregationGeoNear geoNear = AggregationGeoNear.builder()
                .location(location).distanceField("dist")
                .distanceMultiplier(1.2).limit(123).locationField("loc")
                .maxDistance(12345.6).query(BuilderFactory.start().add("a", 2))
                .spherical(true).uniqueDocs(false).build();

        assertThat(geoNear.getLocation(), is(location));
        assertThat(geoNear.getDistanceField(), is("dist"));
        assertThat(geoNear.getDistanceMultiplier(), is(1.2));
        assertThat(geoNear.getLimit(), is(123L));
        assertThat(geoNear.getLocationField(), is("loc"));
        assertThat(geoNear.getMaxDistance(), is(12345.6));
        assertThat(geoNear.getQuery(), is(BuilderFactory.start().add("a", 2)
                .build()));
        assertThat(geoNear.isSpherical(), is(true));
        assertThat(geoNear.isUniqueDocs(), is(false));

        final DocumentBuilder expected = BuilderFactory.start();
        expected.pushArray("near").add(1).add(2);
        expected.add("distanceField", "dist");
        expected.add("spherical", true);
        expected.add("uniqueDocs", false);

        expected.add("limit", 123L);
        expected.add("maxDistance", 12345.6);
        expected.add("query", BuilderFactory.start().add("a", 2));
        expected.add("distanceMultiplier", 1.2);
        expected.add("includeLocs", "loc");

        assertThat(geoNear.asDocument(), is(expected.build()));
    }
}
