/*
 * Copyright 2013, Allanbank Consulting, Inc. 
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client.message;

import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.builder.GeoJson;

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
                nullValue());
        assertThat(QueryVersionVisitor.version(null), nullValue());

        assertThat(QueryVersionVisitor.version(where("a").geoWithin(
                GeoJson.multiPoint(GeoJson.p(1, 1), GeoJson.p(1, 1))).build()),
                is(Version.VERSION_2_4));
        assertThat(QueryVersionVisitor.version(where("a").intersects(
                GeoJson.multiPoint(GeoJson.p(1, 1), GeoJson.p(1, 1))).build()),
                is(Version.VERSION_2_4));
    }
}
