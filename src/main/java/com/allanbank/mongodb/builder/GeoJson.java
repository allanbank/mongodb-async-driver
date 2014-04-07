/*
 * Copyright 2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.builder;

import java.awt.Point;
import java.awt.geom.Point2D;
import java.util.List;

import com.allanbank.mongodb.Version;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.builder.ArrayBuilder;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;

/**
 * GeoJson provides static methods to help create <a
 * href="http://www.geojson.org/geojson-spec.html">GeoJSON</a> BSON Documents.
 * <p>
 * This class uses the {@link Point} and {@link Point2D} classes to represent
 * GeoJSON positions. To assist in converting raw (x, y) coordinates the
 * {@link #p(double, double)} and {@link #p(int, int)} methods are provides to
 * quickly construct the Point instances.
 * </p>
 * <p>
 * As an example of using this class consider the following Polygon with a hole
 * from the GeoJSON specification: <blockquote>
 *
 * <pre>
 * <code>
 * { "type": "Polygon",
 *   "coordinates": [
 *     [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],
 *     [ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ]
 *     ]
 *  }
 * </code>
 * </pre>
 *
 * </blockquote>
 *
 * The equivalent BSON document can be constructed via:<blockquote>
 *
 * <pre>
 * <code>
 * import static com.allanbank.mongodb.builder.GeoJson.polygon;
 * import static com.allanbank.mongodb.builder.GeoJson.p;
 * 
 * Document geoJsonPolygon = polygon(
 *      Arrays.asList( p(100.0, 0.0), p(101.0, 0.0), p(101.0, 1.0), p(100.0, 1.0), p(100.0, 0.0) ),
 *      Arrays.asList( p(100.2, 0.2), p(100.8, 0.2), p(100.8, 0.8), p(100.2, 0.8), p(100.2, 0.2) ) );
 * </code>
 * </pre>
 *
 * </blockquote>
 *
 * @see <a href="http://www.geojson.org/geojson-spec.html">The GeoJSON Format
 *      Specification</a>
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class GeoJson {

    /** The version of MongoDB that provided support for Multi* GeoJSON objects. */
    public static final Version MULTI_SUPPORT_VERSION = Version.VERSION_2_6;

    /** The class for an integer {@link Point} */
    private static final Class<Point> POINT_CLASS = Point.class;

    /**
     * Constructs a GeoJSON 'LineString' document from the coordinates provided.
     *
     * @param points
     *            The positions in the line string. There should be at least 2
     *            positions for the document to be a valid LineString.
     *
     * @return A GeoJSON LineString document from the coordinates provided.
     * @throws IllegalArgumentException
     *             If the list does not contain at least 2 points.
     * @see <a
     *      href="http://www.geojson.org/geojson-spec.html#linestring">GeoJSON
     *      LineString</a>
     */
    public static Document lineString(final List<? extends Point2D> points)
            throws IllegalArgumentException {
        if (points.size() < 2) {
            throw new IllegalArgumentException(
                    "A GeoJSON LineString must have at least 2 postions.");
        }
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "LineString");

        final ArrayBuilder coordinates = builder.pushArray("coordinates");
        add(coordinates, points);

        return builder.build();
    }

    /**
     * Constructs a GeoJSON 'LineString' document from the coordinates provided.
     *
     * @param p1
     *            The first position in the line string.
     * @param p2
     *            The second position in the line string.
     * @param remaining
     *            The remaining positions in the line string.
     *
     * @return A GeoJSON LineString document from the coordinates provided.
     * @see <a
     *      href="http://www.geojson.org/geojson-spec.html#linestring">GeoJSON
     *      LineString</a>
     */
    public static Document lineString(final Point2D p1, final Point2D p2,
            final Point2D... remaining) {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "LineString");
        final ArrayBuilder coordinates = builder.pushArray("coordinates");

        add(coordinates, p1);
        add(coordinates, p2);
        for (final Point2D point : remaining) {
            add(coordinates, point);
        }

        return builder.build();
    }

    /**
     * Constructs a GeoJSON 'MultiLineString' document from the coordinates
     * provided.
     * <p>
     * Note: You will need to add a {@code @SuppressWarnings("unchecked")} to
     * uses of this method as Java does not like mixing generics and varargs.
     * The {@code @SafeVarargs} annotation is not available in Java 1.6, the
     * minimum version for the driver.
     * </p>
     *
     * @param firstLineString
     *            The first line string.
     * @param additionalLineStrings
     *            The remaining line strings.
     * @return A GeoJSON MultiLineString document from the coordinates provided.
     *
     * @see <a href="http://www.geojson.org/geojson-spec.html#polygon">GeoJSON
     *      Polygon</a>
     */
    public static Document multiLineString(
            final List<? extends Point2D> firstLineString,
            final List<? extends Point2D>... additionalLineStrings) {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "MultiLineString");

        final ArrayBuilder coordinates = builder.pushArray("coordinates");

        add(coordinates.pushArray(), firstLineString);

        for (final List<? extends Point2D> additionalLineString : additionalLineStrings) {
            add(coordinates.pushArray(), additionalLineString);
        }

        return builder.build();
    }

    /**
     * Constructs a GeoJSON 'MultiPoint' document from the positions provided.
     *
     * @param positions
     *            The positions
     * @return A GeoJSON MultiPoint document from the coordinates provided.
     * @see <a href="http://www.geojson.org/geojson-spec.html#point">GeoJSON
     *      Point</a>
     */
    public static Document multiPoint(final List<? extends Point2D> positions) {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "MultiPoint");

        final ArrayBuilder coordinates = builder.pushArray("coordinates");
        for (final Point2D position : positions) {
            add(coordinates, position);

        }
        return builder.build();
    }

    /**
     * Constructs a GeoJSON 'MultiPoint' document from the positions provided.
     *
     * @param firstPosition
     *            The first position
     * @param additionalPositions
     *            The other positions
     * @return A GeoJSON MultiPoint document from the coordinates provided.
     * @see <a href="http://www.geojson.org/geojson-spec.html#point">GeoJSON
     *      Point</a>
     */
    public static Document multiPoint(final Point2D firstPosition,
            final Point2D... additionalPositions) {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "MultiPoint");

        final ArrayBuilder coordinates = builder.pushArray("coordinates");
        add(coordinates, firstPosition);
        for (final Point2D additionalPosition : additionalPositions) {
            add(coordinates, additionalPosition);

        }
        return builder.build();
    }

    /**
     * Helper method to construct a {@link Point2D} from the (x, y) coordinates.
     *
     * @param x
     *            The point's x position or longitude.
     * @param y
     *            The point's y position or latitude.
     * @return A Point for the coordinates provided.
     */
    public static Point2D p(final double x, final double y) {
        return new Point2D.Double(x, y);
    }

    /**
     * Helper method to construct a {@link Point} from the (x, y) coordinates.
     *
     * @param x
     *            The point's x position or longitude.
     * @param y
     *            The point's y position or latitude.
     * @return A Point for the coordinates provided.
     */
    public static Point p(final int x, final int y) {
        return new Point(x, y);
    }

    /**
     * Constructs a GeoJSON 'Point' document from the coordinates provided.
     *
     * @param position
     *            The point's position
     * @return A GeoJSON Point document from the coordinates provided.
     * @see <a href="http://www.geojson.org/geojson-spec.html#point">GeoJSON
     *      Point</a>
     */
    public static Document point(final Point2D position) {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "Point");

        addRaw(builder.pushArray("coordinates"), position);

        return builder.build();
    }

    /**
     * Constructs a GeoJSON 'Polygon' document from the coordinates provided.
     *
     * @param boundary
     *            The boundary positions for the polygon.
     * @return A GeoJSON Polygon document from the coordinates provided.
     * @throws IllegalArgumentException
     *             If the line ring does not have at least 4 positions or the
     *             first and last positions are not equivalent.
     *
     * @see <a href="http://www.geojson.org/geojson-spec.html#polygon">GeoJSON
     *      Polygon</a>
     */
    public static Document polygon(final List<? extends Point2D> boundary)
            throws IllegalArgumentException {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "Polygon");

        final ArrayBuilder coordinates = builder.pushArray("coordinates");

        lineRing(coordinates.pushArray(), boundary);

        return builder.build();
    }

    /**
     * Constructs a GeoJSON 'Polygon' document from the coordinates provided.
     * <p>
     * Note: You will need to add a {@code @SuppressWarnings("unchecked")} to
     * uses of this method as Java does not like mixing generics and varargs.
     * The {@code @SafeVarargs} annotation is not available in Java 1.6, the
     * minimum version for the driver.
     * </p>
     *
     * @param boundary
     *            The boundary positions for the polygon.
     * @param holes
     *            The positions for the holes within the polygon.
     * @return A GeoJSON Polygon document from the coordinates provided.
     * @throws IllegalArgumentException
     *             If the line ring does not have at least 4 positions or the
     *             first and last positions are not equivalent.
     *
     * @see <a href="http://www.geojson.org/geojson-spec.html#polygon">GeoJSON
     *      Polygon</a>
     */
    public static Document polygon(final List<? extends Point2D> boundary,
            final List<? extends Point2D>... holes)
                    throws IllegalArgumentException {
        final DocumentBuilder builder = BuilderFactory.start();
        builder.add("type", "Polygon");

        final ArrayBuilder coordinates = builder.pushArray("coordinates");

        lineRing(coordinates.pushArray(), boundary);

        for (final List<? extends Point2D> hole : holes) {
            lineRing(coordinates.pushArray(), hole);
        }

        return builder.build();
    }

    /**
     * Adds a positions to the coordinates array.
     *
     * @param coordinates
     *            The array to add the position to.
     * @param positions
     *            The positions to add.
     */
    protected static void add(final ArrayBuilder coordinates,
            final List<? extends Point2D> positions) {
        for (final Point2D position : positions) {
            add(coordinates, position);
        }
    }

    /**
     * Adds a position to the coordinates array.
     *
     * @param coordinates
     *            The array to add the position to.
     * @param position
     *            The point to add.
     */
    protected static void add(final ArrayBuilder coordinates,
            final Point2D position) {
        addRaw(coordinates.pushArray(), position);
    }

    /**
     * Adds the (x,y) coordinates from the point directly to the array provided.
     *
     * @param arrayBuilder
     *            The builder to append the (x,y) coordinates to.
     * @param position
     *            The (x,y) coordinates.
     */
    protected static void addRaw(final ArrayBuilder arrayBuilder,
            final Point2D position) {
        if (position.getClass() == POINT_CLASS) {
            final Point p = (Point) position;
            arrayBuilder.add(p.x).add(p.y);
        }
        else {
            arrayBuilder.add(position.getX()).add(position.getY());
        }
    }

    /**
     * Fills in the LineRing coordinates.
     *
     * @param positionArray
     *            The array to fill with the positions.
     * @param positions
     *            The positions defining the LineRing.
     * @throws IllegalArgumentException
     *             If the line ring does not have at least 4 positions or the
     *             first and last positions are not equivalent.
     */
    protected static void lineRing(final ArrayBuilder positionArray,
            final List<? extends Point2D> positions)
                    throws IllegalArgumentException {

        if (positions.size() < 4) {
            throw new IllegalArgumentException(
                    "A GeoJSON LineRing must have at least 4 postions.");
        }

        final Point2D first = positions.get(0);
        Point2D last = first;
        for (final Point2D point : positions) {
            add(positionArray, point);
            last = point;
        }
        // The LineString must loop to form a LineRing.
        if (!last.equals(first)) {
            throw new IllegalArgumentException(
                    "A GeoJSON LineRing's first and last postion must be equal.");
        }
    }

    /**
     * Creates a new GeoJson.
     */
    private GeoJson() { /* Static Class */
    }
}
