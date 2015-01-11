/*
 * #%L
 * PatternUtils.java - mongodb-async-driver - Allanbank Consulting, Inc.
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
package com.allanbank.mongodb.util;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * PatternUtils provides utilities for handling patterns.
 *
 * @api.no This class is <b>NOT</b> part of the drivers API. This class may be
 *         mutated in incompatible ways between any two releases of the driver.
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
@ThreadSafe
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
