/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.GeoJson;
import com.allanbank.mongodb.builder.GeospatialOperator;
import com.allanbank.mongodb.builder.MiscellaneousOperator;
import com.allanbank.mongodb.client.VersionRange;

/**
 * QueryVersionVisitorTest provides tests for the {@link QueryVersionVisitor}.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class QueryVersionVisitorTest {

    /**
     * Test method for {@link QueryVersionVisitor#version}.
     */
    @Test
    public void testVersion() {

        assertThat(
                QueryVersionVisitor.version(where("a").equals(true).build()),
                is(VersionRange.range(null, null)));

        assertThat(QueryVersionVisitor.version(null),
                is(VersionRange.range(null, null)));

        assertThat(QueryVersionVisitor.version(where("a").geoWithin(
                GeoJson.multiPoint(GeoJson.p(1, 1), GeoJson.p(1, 1))).build()),
                is(VersionRange.range(
                        GeospatialOperator.GEO_WITHIN.getVersion(), null)));

        assertThat(QueryVersionVisitor.version(where("a").intersects(
                GeoJson.multiPoint(GeoJson.p(1, 1), GeoJson.p(1, 1))).build()),
                is(VersionRange.range(
                        GeospatialOperator.INTERSECT.getVersion(), null)));

        assertThat(QueryVersionVisitor.version(where("a").equals(1).text("abc")
                .build()), is(VersionRange.range(
                MiscellaneousOperator.TEXT.getVersion(), null)));

        assertThat(
                QueryVersionVisitor.version(BuilderFactory.start()
                        .add("$maxTimeMS", 1).build()),
                is(VersionRange.range(Find.MAX_TIMEOUT_VERSION, null)));

    }

    /**
     * Test method for {@link QueryVersionVisitor#version}.
     */
    @Deprecated
    @Test
    public void testVersionWithRemovedIntersects() {
        assertThat(QueryVersionVisitor.version(where("a").geoWithin(
                GeoJson.multiPoint(GeoJson.p(1, 1), GeoJson.p(1, 1)), true)
                .build()), is(VersionRange.range(
                GeospatialOperator.GEO_WITHIN.getVersion(),
                GeospatialOperator.UNIQUE_DOCS_REMOVED_VERSION)));
    }
}
