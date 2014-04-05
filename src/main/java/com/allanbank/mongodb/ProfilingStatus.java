/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb;

import java.io.Serializable;

/**
 * ProfilingStatus provides a container for the {@link Level} and number of
 * milliseconds beyond which to consider an operation to be slow.
 *
 * @api.yes This class is part of the driver's API. Public and protected members
 *          will be deprecated for at least 1 non-bugfix release (version
 *          numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;) before being
 *          removed or modified.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class ProfilingStatus implements Comparable<ProfilingStatus>,
        Serializable {

    /**
     * Level provides the set of available profiling levels provided by the
     * MongoDB server.
     *
     * @api.yes This class is part of the driver's API. Public and protected
     *          members will be deprecated for at least 1 non-bugfix release
     *          (version numbers are &lt;major&gt;.&lt;minor&gt;.&lt;bugfix&gt;)
     *          before being removed or modified.
     * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
     */
    public static enum Level {

        /** Profile all operations. */
        ALL(2),

        /** Profile no operations. */
        NONE(0),

        /** Only profile slow operations. */
        SLOW_ONLY(1);

        /**
         * Returns the {@link Level} for the specified value.
         *
         * @param value
         *            The value of the profile level.
         * @return The profile level for the value.
         */
        public static Level fromValue(final int value) {
            for (final Level level : values()) {
                if (level.getValue() == value) {
                    return level;
                }
            }

            return null;
        }

        /** The profile level value to send to MongoDB. */
        private final int myValue;

        /**
         * Creates a new Level.
         *
         * @param value
         *            The profile level value to send to MongoDB.
         */
        private Level(final int value) {
            myValue = value;
        }

        /**
         * Returns the profile level value to send to MongoDB.
         *
         * @return The profile level value to send to MongoDB.
         */
        public int getValue() {
            return myValue;
        }
    }

    /**
     * The default threshold ({@value} )for the number of milliseconds beyond
     * considering an operation slow.
     */
    public static final long DEFAULT_SLOW_MS = 100;

    /** The off profiling state. */
    public static final ProfilingStatus OFF = new ProfilingStatus(Level.NONE);

    /** The all profiling state. */
    public static final ProfilingStatus ON = new ProfilingStatus(Level.ALL);

    /** The serialization version of the class. */
    private static final long serialVersionUID = 181636899391154872L;

    /**
     * Creates a profiling state to profile operations taking more than
     * {@code slowMillis} to complete.
     *
     * @param slowMillis
     *            The number of milliseconds beyond which to consider an
     *            operation to be slow.
     * @return The slow profiling state.
     */
    public static final ProfilingStatus slow(final int slowMillis) {
        return new ProfilingStatus(Level.SLOW_ONLY, slowMillis);
    }

    /** The profiling level. */
    private final Level myLevel;

    /**
     * The number of milliseconds beyond which to consider an operation to be
     * slow.
     */
    private final long mySlowMillisThreshold;

    /**
     * Creates a new ProfilingStatus.
     *
     * @param level
     *            The profiling level to use.
     */
    private ProfilingStatus(final Level level) {
        this(level, DEFAULT_SLOW_MS);
    }

    /**
     * Creates a new ProfilingStatus.
     *
     * @param level
     *            The profiling level to use.
     * @param slowMillis
     *            The number of milliseconds beyond which to consider an
     *            operation to be slow.
     */
    private ProfilingStatus(final Level level, final long slowMillis) {
        myLevel = level;
        mySlowMillisThreshold = slowMillis;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return an ordering based on the tuple ordering of (level,
     * slowMs).
     * </p>
     */
    @Override
    public int compareTo(final ProfilingStatus other) {
        int diff = myLevel.getValue() - other.myLevel.getValue();
        if (diff == 0) {
            if (mySlowMillisThreshold < other.mySlowMillisThreshold) {
                diff = -1;
            }
            else if (mySlowMillisThreshold == other.mySlowMillisThreshold) {
                diff = 0;
            }
            else {
                diff = 1;
            }

        }
        return diff;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to compare this object to the passed object.
     * </p>
     */
    @Override
    public boolean equals(final Object object) {
        boolean result = false;
        if (this == object) {
            result = true;
        }
        else if ((object != null) && (getClass() == object.getClass())) {
            final ProfilingStatus other = (ProfilingStatus) object;

            result = (myLevel == other.myLevel)
                    && (mySlowMillisThreshold == other.mySlowMillisThreshold);
        }
        return result;
    }

    /**
     * Returns the profiling level to use.
     *
     * @return The profiling level to use.
     */
    public Level getLevel() {
        return myLevel;
    }

    /**
     * Returns the number of milliseconds beyond which to consider an operation
     * to be slow.
     *
     * @return The number of milliseconds beyond which to consider an operation
     *         to be slow.
     */
    public long getSlowMillisThreshold() {
        return mySlowMillisThreshold;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to generate a suitable hash of the objects state.
     * </p>
     */
    @Override
    public int hashCode() {
        int result = 1;
        result = (31 * result) + ((myLevel == null) ? 0 : myLevel.hashCode());
        result = (31 * result)
                + (int) (mySlowMillisThreshold ^ (mySlowMillisThreshold >>> 32));
        return result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to return a readable form of the object.
     * </p>
     */
    @Override
    public String toString() {
        if (myLevel == Level.SLOW_ONLY) {
            return myLevel.name() + "(" + mySlowMillisThreshold + " ms)";
        }
        return myLevel.name();
    }

    /**
     * Hook into serialization to replace <tt>this</tt> object with the local
     * {@link #ON} or {@link #OFF} instance as appropriate.
     *
     * @return Either the {@link #ON} or {@link #OFF} instance if <tt>this</tt>
     *         instance equals one of those instances otherwise <tt>this</tt>
     *         instance.
     */
    private Object readResolve() {
        if (this.equals(ON)) {
            return ON;
        }
        else if (this.equals(OFF)) {
            return OFF;
        }
        else {
            return this;
        }
    }
}
