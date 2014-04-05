/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import static com.allanbank.mongodb.builder.GeoJson.p;
import static org.junit.Assert.assertEquals;

import java.awt.Point;
import java.awt.geom.Point2D;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.json.Json;

/**
 * GeoJsonTest provides tests for the {@link GeoJson} class.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class GeoJsonTest {

    /**
     * Validates the example in the {@link GeoJson} class's JavaDoc.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testClassJavaDoc() {
        final String json = "{ 'type': 'Polygon',  'coordinates': [ "
                + "[ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],"
                + "[ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ] ] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoJsonPolygon = GeoJson.polygon(
                Arrays.asList(p(100.0, 0.0), p(101.0, 0.0), p(101.0, 1.0),
                        p(100.0, 1.0), p(100.0, 0.0)),
                Arrays.asList(p(100.2, 0.2), p(100.8, 0.2), p(100.8, 0.8),
                        p(100.2, 0.8), p(100.2, 0.2)));

        assertEquals(jsonDoc, geoJsonPolygon);
    }

    /**
     * Test method for {@link GeoJson#lineString(List)}.
     */
    @Test
    public void testLineStringListOfPoint2D() {
        final String json = "{ 'type': 'LineString',  'coordinates': [ "
                + " [1,2], [3,4], [5,6] ] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoDoc = GeoJson.lineString(Arrays.asList(p(1, 2),
                p(3, 4), p(5, 6)));

        assertEquals(jsonDoc, geoDoc);
    }

    /**
     * Test method for {@link GeoJson#lineString(List)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testLineStringListOfPoint2DTooFewItems() {
        GeoJson.lineString(Arrays.asList(p(1, 2)));
    }

    /**
     * Test method for {@link GeoJson#lineString(Point2D, Point2D, Point2D[])} .
     */
    @Test
    public void testLineStringPoint2DPoint2DPoint2DArray() {
        final String json = "{ 'type': 'LineString',  'coordinates': [ "
                + " [1,2], [3,4], [5,6] ] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoDoc = GeoJson.lineString(p(1, 2), p(3, 4), p(5, 6));

        assertEquals(jsonDoc, geoDoc);
    }

    /**
     * Test method for {@link GeoJson#multiLineString(List, List...)}.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testMultiLineString() {
        final String json = "{ 'type': 'MultiLineString',  'coordinates': [ "
                + "[ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],"
                + "[ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ] ] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoJson = GeoJson.multiLineString(
                Arrays.asList(p(100.0, 0.0), p(101.0, 0.0), p(101.0, 1.0),
                        p(100.0, 1.0), p(100.0, 0.0)),
                Arrays.asList(p(100.2, 0.2), p(100.8, 0.2), p(100.8, 0.8),
                        p(100.2, 0.8), p(100.2, 0.2)));

        assertEquals(jsonDoc, geoJson);
    }

    /**
     * Test method for {@link GeoJson#multiPoint(List)}.
     */
    @Test
    public void testMultiPointListOfPoint2D() {
        final String json = "{ 'type': 'MultiPoint',  'coordinates': [ "
                + "[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoJson = GeoJson.multiPoint(Arrays.asList(
                p(100.0, 0.0), p(101.0, 0.0), p(101.0, 1.0), p(100.0, 1.0),
                p(100.0, 0.0)));

        assertEquals(jsonDoc, geoJson);
    }

    /**
     * Test method for {@link GeoJson#multiPoint(Point2D, Point2D[])} .
     */
    @Test
    public void testMultiPointPoint2DPoint2DArray() {
        final String json = "{ 'type': 'MultiPoint',  'coordinates': [ "
                + "[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoJson = GeoJson.multiPoint(p(100.0, 0.0),
                p(101.0, 0.0), p(101.0, 1.0), p(100.0, 1.0), p(100.0, 0.0));

        assertEquals(jsonDoc, geoJson);
    }

    /**
     * Test method for {@link GeoJson#p(double, double)}.
     */
    @Test
    public void testPDoubleDouble() {
        assertEquals(new Point2D.Double(1.1, 2.2), GeoJson.p(1.1, 2.2));
    }

    /**
     * Test method for {@link GeoJson#p(int, int)} .
     */
    @Test
    public void testPIntInt() {
        assertEquals(new Point(1, 2), GeoJson.p(1, 2));
    }

    /**
     * Test method for {@link GeoJson#point(Point2D)} .
     */
    @Test
    public void testPoint() {
        final String json = "{ 'type': 'Point',  'coordinates': [5.1,6.2] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoDoc = GeoJson.point(p(5.1, 6.2));

        assertEquals(jsonDoc, geoDoc);
    }

    /**
     * Test method for {@link GeoJson#point(Point2D)} .
     */
    @Test
    public void testPointWithInts() {
        final String json = "{ 'type': 'Point',  'coordinates': [5,6] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoDoc = GeoJson.point(p(5, 6));

        assertEquals(jsonDoc, geoDoc);
    }

    /**
     * Test method for {@link GeoJson#polygon(List)}.
     */
    @Test
    public void testPolygon() {
        final String json = "{ 'type': 'Polygon',  'coordinates': [ "
                + "[ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ] ] }";
        final Document jsonDoc = Json.parse(json);

        final Document geoJsonPolygon = GeoJson.polygon(Arrays.asList(
                p(100.0, 0.0), p(101.0, 0.0), p(101.0, 1.0), p(100.0, 1.0),
                p(100.0, 0.0)));

        assertEquals(jsonDoc, geoJsonPolygon);
    }

    /**
     * Test method for {@link GeoJson#polygon(List)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPolygonNotClosed() {
        GeoJson.polygon(Arrays.asList(p(100.0, 0.0), p(101.0, 0.0),
                p(101.0, 1.0), p(100.0, 1.0), p(100.0, 0.1)));
    }

    /**
     * Test method for {@link GeoJson#polygon(List)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPolygonTooSmall() {
        GeoJson.polygon(Arrays.asList(p(100.0, 0.0), p(101.0, 0.0),
                p(101.0, 1.0)));
    }
}
