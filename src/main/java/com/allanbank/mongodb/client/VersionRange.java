/*
 * Copyright 2014, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.client;

import java.io.Serializable;

import com.allanbank.mongodb.Version;

/**
 * VersionRange provides a simple class to hold a range of versions.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2014, Allanbank Consulting, Inc., All Rights Reserved
 */
public class VersionRange implements Serializable {
    /** Serialization version for the class. */
    private static final long serialVersionUID = 7841643157458023019L;

    /**
     * Returns a {@link VersionRange} that is only limited by a maximum version.
     * The minimum version is set to {@link Version#VERSION_0}.
     *
     * @param maxVersion
     *            The maximum version in the range. If <code>null</code> then
     *            <code>null</code> is returned.
     * @return The version range with only a maximum value.
     */
    public static VersionRange maximum(final Version maxVersion) {
        if (maxVersion != null) {
            return new VersionRange(Version.VERSION_0, maxVersion);
        }
        return null;
    }

    /**
     * Returns a {@link VersionRange} that is only limited by a minimum version.
     * The maximum version is set to {@link Version#UNKNOWN}.
     *
     * @param minVersion
     *            The minimum version in the range. If <code>null</code> then
     *            <code>null</code> is returned.
     * @return The version range with only a minimum value.
     */
    public static VersionRange minimum(final Version minVersion) {
        if (minVersion != null) {
            return new VersionRange(minVersion, Version.UNKNOWN);
        }
        return null;
    }

    /**
     * Returns a {@link VersionRange} based on the minimum and maximum versions
     * provided.
     * <p>
     * If {@code minVersion} and {@code maxVersion} are both <code>null</code>
     * then <code>null</code> is returned.
     * </p>
     * <p>
     * If only {@code minVersion} is <code>null</code> then the lower bound is
     * set to {@link Version#VERSION_0}.
     * </p>
     * <p>
     * If only {@code maxVersion} is <code>null</code> then the upper bound is
     * set to {@link Version#UNKNOWN}.
     * </p>
     *
     * @param minVersion
     *            The minimum version in the range. If <code>null</code> then
     *            <code>null</code> is returned.
     * @param maxVersion
     *            The maximum version in the range. If <code>null</code> then
     *            <code>null</code> is returned.
     * @return The version range with only a minimum value.
     */
    public static VersionRange range(final Version minVersion,
            final Version maxVersion) {
        if (minVersion != null) {
            if (maxVersion != null) {
                return new VersionRange(minVersion, maxVersion);
            }
            return new VersionRange(minVersion, Version.UNKNOWN);
        }
        else if (maxVersion != null) {
            return new VersionRange(Version.VERSION_0, maxVersion);
        }
        return null;
    }

    /** The lower, inclusive, bounds of the range. */
    private final Version myLowerBounds;

    /** The upper, exclusive, bounds of the range. */
    private final Version myUpperBounds;

    /**
     * Creates a new VersionRange.
     *
     * @param lowerBounds
     *            The lower, inclusive, bounds of the range.
     * @param upperBounds
     *            The upper, exclusive, bounds of the range.
     */
    private VersionRange(final Version lowerBounds, final Version upperBounds) {
        myLowerBounds = lowerBounds;
        myUpperBounds = upperBounds;
    }

    /**
     * Returns true if the version is within the bounds of this
     * {@link VersionRange}.
     *
     * @param version
     *            The version to check if it is within range.
     * @return True if the version is within the bounds of this
     *         {@link VersionRange}.
     */
    public boolean contains(final Version version) {
        return (myLowerBounds.compareTo(version) <= 0)
                && (myUpperBounds.compareTo(version) > 0);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare this {@link VersionRange} to the other.
     * </p>
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final VersionRange other = (VersionRange) object;

            result = myLowerBounds.equals(other.myLowerBounds)
                    && myUpperBounds.equals(other.myUpperBounds);
        }
        return result;
    }

    /**
     * Returns the lower, inclusive, bounds of the range.
     *
     * @return The lower, inclusive, bounds of the range.
     */
    public Version getLowerBounds() {
        return myLowerBounds;
    }

    /**
     * Returns the upper, exclusive, bounds of the range.
     *
     * @return The upper, exclusive, bounds of the range.
     */
    public Version getUpperBounds() {
        return myUpperBounds;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to hash the version range.
     * </p>
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + myLowerBounds.hashCode();
        result = (31 * result) + myUpperBounds.hashCode();
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        builder.append("[");
        builder.append(myLowerBounds);
        builder.append(", ");
        builder.append(myUpperBounds);
        builder.append(")");

        return builder.toString();
    }
}
