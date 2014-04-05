/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * PatternUtils provides utilities for handling patterns.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public final class PatternUtils {
    /** The regular expression matching all input. */
    public static final String ALL = ".*";

    /** The pattern for the regular expression matching all input. */
    public static final Pattern ALL_PATTERN = Pattern.compile(ALL);

    /**
     * Converts the Regular Expression string into a {@link Pattern}.
     *
     * @param regex
     *            The regular expression to convert.
     * @return The {@link Pattern} for the regular expression.
     * @throws PatternSyntaxException
     *             If the expression's syntax is invalid.
     */
    public static Pattern toPattern(final String regex)
            throws PatternSyntaxException {
        if (ALL.equals(regex)) {
            return ALL_PATTERN;
        }
        return Pattern.compile(regex);
    }

    /**
     * Creates a new PatternUtils.
     */
    private PatternUtils() {
        // Nothing.
    }
}
