/*
 * Copyright 2012-2013, Allanbank Consulting, Inc.
 *           All Rights Reserved
 */

package com.allanbank.mongodb.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.regex.Pattern;

import org.junit.Test;

/**
 * PatternUtilsTest provides tests for the {@link PatternUtils} class.
 *
 * @copyright 2012-2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public class PatternUtilsTest {

    /**
     * Test method for {@link PatternUtils#toPattern(java.lang.String)}.
     */
    @Test
    public void testToPattern() {
        final Pattern p = PatternUtils.toPattern("abc.*def");

        assertNotNull(p);
        assertEquals("abc.*def", p.pattern());
    }

    /**
     * Test method for {@link PatternUtils#toPattern(java.lang.String)}.
     */
    @Test
    public void testToPatternCachesTheAllPattern() {
        final Pattern p = PatternUtils.toPattern(".*");

        assertNotNull(p);
        assertSame(PatternUtils.ALL_PATTERN, p);
    }

}
