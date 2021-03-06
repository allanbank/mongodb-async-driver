/*
 * #%L
 * Version.java - mongodb-async-driver - Allanbank Consulting, Inc.
 * %%
 * Copyright (C) 2011 - 2014 Allanbank Consulting, Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.allanbank.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.util.IOUtils;
import com.allanbank.mongodb.util.log.Log;
import com.allanbank.mongodb.util.log.LogFactory;

/**
 * Version provides a class to handle version numbers and provide the version of
 * the driver in use.
 *
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
public class Version
        implements Serializable, Comparable<Version> {

    /**
     * A version to use when we don't know the version. This version will always
     * compare greater than (higher, more recent) than all other versions.
     */
    public static final Version UNKNOWN;

    /** The driver's version. */
    public static final Version VERSION;

    /** The "zero" version which should be before all other versions. */
    public static final Version VERSION_0 = Version.parse("0");

    /** Version 2.0 */
    public static final Version VERSION_2_0 = Version.parse("2.0");

    /** Version 2.2 */
    public static final Version VERSION_2_2 = Version.parse("2.2");

    /** Version 2.4 */
    public static final Version VERSION_2_4 = Version.parse("2.4");

    /** Version 2.6 */
    public static final Version VERSION_2_6 = Version.parse("2.6");

    /** Version 2.5.2 - Wire Protocol version = 1. */
    protected static final Version VERSION_2_5_2 = Version.parse("2.5.2");

    /** Version 2.5.4 - Wire Protocol version = 2. */
    protected static final Version VERSION_2_5_4 = Version.parse("2.5.4");

    /** The logger for the {@link Version}. */
    private static final Log LOG = LogFactory.getLog(Version.class);

    /** The serialization version for the class. */
    private static final long serialVersionUID = 4726973040107711788L;

    static {
        // Load the version from the maven pom.properties file.
        final Properties props = new Properties();
        InputStream in = null;
        try {
            in = Version.class
                    .getResourceAsStream("/META-INF/maven/com.allanbank/"
                            + "mongodb-async-driver/pom.properties");
            if (in != null) {
                props.load(in);
            }
        }
        catch (final IOException error) {
            LOG.info("Could not read the version information for the driver.");
        }
        finally {
            IOUtils.close(in);
        }

        VERSION = parse(props.getProperty("version", "0-DEVELOPMENT"));
        UNKNOWN = new Version(new int[0], "UNKNOWN");
    }

    /**
     * Returns the earlier of the two versions. If either version is
     * {@code null} then the other version is returned. Only if both version are
     * {@code null} will {@code null} be returned.
     *
     * @param lhs
     *            The first version to compare.
     * @param rhs
     *            The second version to compare.
     * @return The earlier (lesser) version of the two.
     */
    public static Version earlier(final Version lhs, final Version rhs) {
        if ((lhs == null) || ((rhs != null) && (lhs.compareTo(rhs) > 0))) {
            return rhs;
        }

        return lhs;
    }

    /**
     * Returns the best guess at the version of the server based on the wire
     * protocol version number. Returns the first version of the server to
     * support the wire protocol version.
     *
     * @param wireVersion
     *            Wire protocol version.
     * @return The best guess at the version of the server based on the wire
     *         protocol version number. Returns <code>null</code> if the version
     *         cannot be determined.
     */
    public static Version forWireVersion(final int wireVersion) {
        Version result = null;
        if (wireVersion >= 2) {
            result = VERSION_2_5_4;
        }
        else if (wireVersion == 1) {
            result = VERSION_2_5_2;
        }
        else if (wireVersion == 0) {
            result = VERSION_2_4;
        }

        return result;
    }

    /**
     * Returns the later of the two versions. If either version is {@code null}
     * then the other version is returned. Only if both version are {@code null}
     * will {@code null} be returned.
     *
     * @param lhs
     *            The first version to compare.
     * @param rhs
     *            The second version to compare.
     * @return The later (greater) version of the two.
     */
    public static Version later(final Version lhs, final Version rhs) {
        if ((lhs == null) || ((rhs != null) && (lhs.compareTo(rhs) < 0))) {
            return rhs;
        }
        return lhs;
    }

    /**
     * Parses a version from a version array. The values in the array are
     * assumed to be {@link NumericElement}s.
     *
     * @param versionArray
     *            The version array to parse.
     * @return The version.
     */
    public static Version parse(final List<NumericElement> versionArray) {

        final int[] version = new int[versionArray.size()];
        for (int i = 0; i < version.length; ++i) {
            version[i] = versionArray.get(i).getIntValue();
        }

        return new Version(version, null);
    }

    /**
     * Parses a version of the general format 'int.int.int-suffix'.
     *
     * @param version
     *            The version string to parse.
     * @return The version.
     */
    public static Version parse(final String version) {

        final String[] tokens = version.split("\\.");
        final int[] versions = new int[tokens.length];
        String suffix = "";
        for (int i = 0; i < tokens.length; ++i) {
            String token = tokens[i];

            // Check for a suffix on the last token.
            if (i == (tokens.length - 1)) {
                final int dashIndex = token.indexOf('-');
                if (dashIndex >= 0) {
                    suffix = token.substring(dashIndex + 1);
                    token = token.substring(0, dashIndex);
                }
            }

            try {
                versions[i] = Integer.parseInt(token);
            }
            catch (final NumberFormatException nfe) {
                LOG.debug(
                        "Could not parse version string token ('{}') from version '{}'.",
                        token, version);
            }
        }

        return new Version(versions, suffix);
    }

    /** The suffix for the version like 'SNAPSHOT'. May be the empty string. */
    private final String mySuffix;

    /** The "values" for the version number. */
    private final int[] myVersion;

    /**
     * Creates a new Version.
     *
     * @param version
     *            The "values" for the version number.
     * @param suffix
     *            The suffix for the version like 'SNAPSHOT'. May be the empty
     *            string.
     */
    private Version(final int[] version, final String suffix) {
        myVersion = version.clone();
        mySuffix = (suffix != null) ? suffix : "";
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare the two versions.
     * </p>
     */
    @Override
    public int compareTo(final Version other) {

        int compare = 0;

        // Check to make sure UNKNOWN is the highest version.
        if (UNKNOWN.equals(this)) {
            compare = UNKNOWN.equals(other) ? 0 : 1;
        }
        else if (UNKNOWN.equals(other)) {
            compare = -1;
        }

        final int fields = Math.min(myVersion.length, other.myVersion.length);
        for (int i = 0; (compare == 0) && (i < fields); ++i) {
            compare = compare(myVersion[i], other.myVersion[i]);
        }

        if (compare == 0) {
            compare = compare(myVersion.length, other.myVersion.length);
            if (compare == 0) {
                compare = mySuffix.compareTo(other.mySuffix);
            }
        }

        return compare;
    }

    /**
     * Determines if the passed object is of this same type as this object and
     * if so that its fields are equal.
     *
     * @param object
     *            The object to compare to.
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final Version other = (Version) object;

            result = mySuffix.equals(other.mySuffix)
                    && Arrays.equals(myVersion, other.myVersion);
        }
        return result;
    }

    /**
     * Computes a reasonable hash code.
     *
     * @return The hash code value.
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + mySuffix.hashCode();
        result = (31 * result) + Arrays.hashCode(myVersion);
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to produce a human readable string for the version.
     * </p>
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < myVersion.length; ++i) {
            if (i != 0) {
                builder.append('.');
            }
            builder.append(String.valueOf(myVersion[i]));
        }
        if (!mySuffix.isEmpty()) {
            if (myVersion.length > 0) {
                builder.append('-');
            }
            builder.append(mySuffix);
        }
        return builder.toString();
    }

    /**
     * Compares two {@code int} values numerically. The value returned is
     * identical to what would be returned by:
     *
     * <pre>
     * Integer.valueOf(x).compareTo(Integer.valueOf(y))
     * </pre>
     *
     * @param x
     *            the first {@code int} to compare
     * @param y
     *            the second {@code int} to compare
     * @return the value {@code 0} if {@code x == y}; a value less than
     *         {@code 0} if {@code x < y}; and a value greater than {@code 0} if
     *         {@code x > y}
     * @since 1.7
     */
    protected int compare(final int x, final int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

}
